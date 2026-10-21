package kubernetes_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/neilotoole/slogt"
	"github.com/sablierapp/sablier/pkg/config"
	"github.com/sablierapp/sablier/pkg/provider"
	"github.com/sablierapp/sablier/pkg/provider/kubernetes"
	"github.com/sablierapp/sablier/pkg/sablier"
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func TestBatchLifecycleIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("requires k3s")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	client := sharedKinD.client
	p, err := kubernetes.New(ctx, client, sharedKinD.dynamic, slogt.New(t), config.NewProviderConfig().Kubernetes)
	if err != nil {
		t.Fatal(err)
	}
	ns, err := client.CoreV1().Namespaces().Create(ctx, &corev1.Namespace{ObjectMeta: metav1.ObjectMeta{GenerateName: "batch-"}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.CoreV1().Namespaces().Delete(context.Background(), ns.Name, metav1.DeleteOptions{}) })
	yes := true
	grace := int64(20)
	template := corev1.PodTemplateSpec{Spec: corev1.PodSpec{RestartPolicy: corev1.RestartPolicyNever, TerminationGracePeriodSeconds: &grace, Containers: []corev1.Container{{Name: "worker", Image: "busybox:1.37", Command: []string{"sh", "-c", "trap 'echo SIGTERM; sleep 4; exit 0' TERM; echo RUNNING; while :; do sleep 1; done"}}}}}
	labels := map[string]string{"sablierapp.dev/enable": "true", "sablierapp.dev/group": ns.Name}
	c, err := client.BatchV1().CronJobs(ns.Name).Create(ctx, &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: "cron", Labels: labels}, Spec: batchv1.CronJobSpec{Schedule: "* * * * *", Suspend: &yes, JobTemplate: batchv1.JobTemplateSpec{Spec: batchv1.JobSpec{Template: template}}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	serverTemplate := template.DeepCopy()
	serverTemplate.Spec.RestartPolicy = corev1.RestartPolicyAlways
	replicas := int32(1)
	deployment, err := client.AppsV1().Deployments(ns.Name).Create(ctx, &appsv1.Deployment{ObjectMeta: metav1.ObjectMeta{Name: "server", Labels: labels}, Spec: appsv1.DeploymentSpec{Replicas: &replicas, Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "server"}}, Template: corev1.PodTemplateSpec{ObjectMeta: metav1.ObjectMeta{Labels: map[string]string{"app": "server"}}, Spec: serverTemplate.Spec}}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	job, err := client.BatchV1().Jobs(ns.Name).Create(ctx, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "standalone", Labels: labels}, Spec: batchv1.JobSpec{Template: template}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	child, err := client.BatchV1().Jobs(ns.Name).Create(ctx, &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "child", OwnerReferences: []metav1.OwnerReference{{APIVersion: "batch/v1", Kind: "CronJob", Name: c.Name, UID: c.UID, Controller: &yes}}}, Spec: batchv1.JobSpec{Template: template}}, metav1.CreateOptions{})
	if err != nil {
		t.Fatal(err)
	}
	wait := func(check func() bool) {
		t.Helper()
		for !check() {
			select {
			case <-ctx.Done():
				t.Fatal(ctx.Err())
			case <-time.After(500 * time.Millisecond):
			}
		}
	}
	var podName string
	wait(func() bool {
		pods, e := client.CoreV1().Pods(ns.Name).List(ctx, metav1.ListOptions{LabelSelector: "job-name=" + child.Name})
		if e != nil || len(pods.Items) == 0 {
			return false
		}
		podName = pods.Items[0].Name
		b, e := client.CoreV1().Pods(ns.Name).GetLogs(podName, &corev1.PodLogOptions{}).DoRaw(ctx)
		return e == nil && strings.Contains(string(b), "RUNNING")
	})
	cronName := kubernetes.CronJobName(c, kubernetes.ParseOptions{Delimiter: "_"}).Original
	jobName := kubernetes.JobName(job, kubernetes.ParseOptions{Delimiter: "_"}).Original
	if err = p.InstanceStart(ctx, cronName); err != nil {
		t.Fatal(err)
	}
	deploymentName := kubernetes.DeploymentName(deployment, kubernetes.ParseOptions{Delimiter: "_"}).Original
	wait(func() bool {
		i, e := p.InstanceInspect(ctx, deploymentName)
		return e == nil && i.Status == sablier.InstanceStatusReady
	})
	for _, name := range []string{deploymentName, cronName, jobName} {
		i, e := p.InstanceInspect(ctx, name)
		if e != nil || i.Status != sablier.InstanceStatusReady {
			t.Fatal(i, e)
		}
	}
	if err = p.InstanceStop(ctx, cronName); err != nil {
		t.Fatal(err)
	}
	wait(func() bool {
		b, e := client.CoreV1().Pods(ns.Name).GetLogs(podName, &corev1.PodLogOptions{}).DoRaw(ctx)
		return e == nil && strings.Contains(string(b), "SIGTERM")
	})
	if err = p.InstanceStop(ctx, jobName); err != nil {
		t.Fatal(err)
	}
	wait(func() bool {
		pods, e := client.CoreV1().Pods(ns.Name).List(ctx, metav1.ListOptions{})
		if e != nil {
			return false
		}
		for _, pod := range pods.Items {
			if pod.Labels["job-name"] != "" {
				return false
			}
		}
		return true
	})
	if err = p.InstanceStart(ctx, jobName); err != nil {
		t.Fatal(err)
	}
	jobs, err := client.BatchV1().Jobs(ns.Name).List(ctx, metav1.ListOptions{})
	if err != nil || len(jobs.Items) != 0 {
		t.Fatal("deleted jobs recreated", jobs, err)
	}
	groups, err := p.InstanceGroups(ctx)
	if err != nil || len(groups[ns.Name]) != 2 {
		t.Fatal(groups, err)
	}
	list, err := p.InstanceList(ctx, provider.InstanceListOptions{All: true})
	if err != nil {
		t.Fatal(err)
	}
	for _, i := range list {
		if i.Name == jobName {
			t.Fatal("cancelled member remains")
		}
	}
	if err = p.InstanceStart(ctx, cronName); err != nil {
		t.Fatal(err)
	}
	wait(func() bool {
		jobs, e := client.BatchV1().Jobs(ns.Name).List(ctx, metav1.ListOptions{})
		return e == nil && len(jobs.Items) > 0
	})
}
