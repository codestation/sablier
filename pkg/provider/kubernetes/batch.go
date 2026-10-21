package kubernetes

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/sablierapp/sablier/pkg/provider"
	"github.com/sablierapp/sablier/pkg/sablier"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
)

func batchName(kind string, obj metav1.Object, delimiter string) ParsedName {
	return ParsedName{Original: fmt.Sprintf("%s%s%s%s%s%s1", kind, delimiter, obj.GetNamespace(), delimiter, obj.GetName(), delimiter), Kind: kind, Namespace: obj.GetNamespace(), Name: obj.GetName(), Replicas: 1}
}
func CronJobName(j *batchv1.CronJob, opts ParseOptions) ParsedName {
	return batchName("cronjob", j, opts.Delimiter)
}
func JobName(j *batchv1.Job, opts ParseOptions) ParsedName {
	return batchName("job", j, opts.Delimiter)
}
func suspended(v *bool) bool { return v != nil && *v }
func jobTerminal(j *batchv1.Job) bool {
	for _, c := range j.Status.Conditions {
		if (c.Type == batchv1.JobComplete || c.Type == batchv1.JobFailed) && c.Status == "True" {
			return true
		}
	}
	return false
}
func batchInfo(parsed ParsedName, obj metav1.Object, status sablier.InstanceStatus, image string) sablier.InstanceInfo {
	info := sablier.InstanceInfo{Name: parsed.Original, Status: status, DesiredReplicas: 1, Provider: sablier.ProviderKubernetes, Kubernetes: &sablier.KubernetesWorkloadInfo{Namespace: parsed.Namespace, Kind: parsed.Kind, Labels: obj.GetLabels(), Image: image}}
	if status == sablier.InstanceStatusReady {
		info.CurrentReplicas = 1
	}
	sablier.PopulateEnabledAndGroup(&info, sablierConfig(obj.GetLabels(), obj.GetAnnotations()))
	return info
}
func (p *Provider) cronJobInfo(j *batchv1.CronJob) sablier.InstanceInfo {
	status := sablier.InstanceStatusReady
	if suspended(j.Spec.Suspend) || j.DeletionTimestamp != nil {
		status = sablier.InstanceStatusStopped
	}
	image := ""
	if len(j.Spec.JobTemplate.Spec.Template.Spec.Containers) > 0 {
		image = j.Spec.JobTemplate.Spec.Template.Spec.Containers[0].Image
	}
	return batchInfo(CronJobName(j, ParseOptions{Delimiter: p.delimiter}), j, status, image)
}
func (p *Provider) jobInfo(j *batchv1.Job) sablier.InstanceInfo {
	status := sablier.InstanceStatusReady
	if suspended(j.Spec.Suspend) || j.DeletionTimestamp != nil {
		status = sablier.InstanceStatusStopped
	}
	for _, c := range j.Status.Conditions {
		if c.Status == "True" {
			if c.Type == batchv1.JobComplete {
				status = sablier.InstanceStatusCompleted
			}
			if c.Type == batchv1.JobFailed {
				status = sablier.InstanceStatusError
			}
		}
	}
	image := ""
	if len(j.Spec.Template.Spec.Containers) > 0 {
		image = j.Spec.Template.Spec.Containers[0].Image
	}
	return batchInfo(JobName(j, ParseOptions{Delimiter: p.delimiter}), j, status, image)
}
func (p *Provider) batchInspect(ctx context.Context, name ParsedName) (sablier.InstanceInfo, error) {
	if name.Kind == "cronjob" {
		j, e := p.Client.BatchV1().CronJobs(name.Namespace).Get(ctx, name.Name, metav1.GetOptions{})
		if e != nil {
			return sablier.InstanceInfo{}, e
		}
		return p.cronJobInfo(j), nil
	}
	j, e := p.Client.BatchV1().Jobs(name.Namespace).Get(ctx, name.Name, metav1.GetOptions{})
	if e != nil {
		return sablier.InstanceInfo{}, e
	}
	return p.jobInfo(j), nil
}
func (p *Provider) batchList(ctx context.Context, opts provider.InstanceListOptions) ([]sablier.InstanceConfiguration, error) {
	cs, err := listEnabled(func(o metav1.ListOptions) ([]batchv1.CronJob, error) {
		l, e := p.Client.BatchV1().CronJobs("").List(ctx, o)
		if e != nil {
			return nil, e
		}
		return l.Items, nil
	})
	if err != nil {
		return nil, err
	}
	js, err := listEnabled(func(o metav1.ListOptions) ([]batchv1.Job, error) {
		l, e := p.Client.BatchV1().Jobs("").List(ctx, o)
		if e != nil {
			return nil, e
		}
		return l.Items, nil
	})
	if err != nil {
		return nil, err
	}
	var result []sablier.InstanceConfiguration
	add := func(i sablier.InstanceInfo) {
		result = append(result, sablier.InstanceConfiguration{Name: i.Name, Groups: i.Groups, Enabled: i.Enabled})
	}
	for i := range cs {
		if cs[i].DeletionTimestamp == nil && (opts.All || !suspended(cs[i].Spec.Suspend)) {
			add(p.cronJobInfo(&cs[i]))
		}
	}
	for i := range js {
		if !jobTerminal(&js[i]) && js[i].DeletionTimestamp == nil && (opts.All || !suspended(js[i].Spec.Suspend)) {
			add(p.jobInfo(&js[i]))
		}
	}
	return result, nil
}
func (p *Provider) cancelJob(ctx context.Context, j *batchv1.Job) error {
	if jobTerminal(j) {
		return nil
	}
	foreground := metav1.DeletePropagationForeground
	err := p.Client.BatchV1().Jobs(j.Namespace).Delete(ctx, j.Name, metav1.DeleteOptions{PropagationPolicy: &foreground, Preconditions: &metav1.Preconditions{UID: &j.UID}})
	if apierrors.IsNotFound(err) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("cancel job %s/%s: %w", j.Namespace, j.Name, err)
	}
	return nil
}
func ownedByCronJob(j *batchv1.Job, c *batchv1.CronJob) bool {
	for _, o := range j.OwnerReferences {
		if o.Kind == "CronJob" && o.UID == c.UID && o.Name == c.Name {
			return true
		}
	}
	return false
}
func suspendPatch(uid types.UID, active bool) []byte {
	b, _ := json.Marshal(map[string]any{"metadata": map[string]any{"uid": uid}, "spec": map[string]any{"suspend": !active}})
	return b
}
func (p *Provider) batchSetActive(ctx context.Context, name ParsedName, active bool) error {
	if name.Kind == "job" {
		j, err := p.Client.BatchV1().Jobs(name.Namespace).Get(ctx, name.Name, metav1.GetOptions{})
		if apierrors.IsNotFound(err) {
			return nil
		}
		if err != nil {
			return err
		}
		if jobTerminal(j) || j.DeletionTimestamp != nil {
			return nil
		}
		if !active {
			return p.cancelJob(ctx, j)
		}
		_, err = p.Client.BatchV1().Jobs(name.Namespace).Patch(ctx, name.Name, types.MergePatchType, suspendPatch(j.UID, true), metav1.PatchOptions{})
		return err
	}
	c, err := p.Client.BatchV1().CronJobs(name.Namespace).Get(ctx, name.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	_, err = p.Client.BatchV1().CronJobs(name.Namespace).Patch(ctx, name.Name, types.MergePatchType, suspendPatch(c.UID, active), metav1.PatchOptions{})
	if err != nil {
		return err
	}
	if active {
		return nil
	}
	jobs, err := p.Client.BatchV1().Jobs(name.Namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return fmt.Errorf("list children of cronjob %s/%s: %w", name.Namespace, name.Name, err)
	}
	var errs []error
	for i := range jobs.Items {
		j := &jobs.Items[i]
		if ownedByCronJob(j, c) {
			if e := p.cancelJob(ctx, j); e != nil {
				errs = append(errs, e)
			}
		}
	}
	return errors.Join(errs...)
}
