package kubernetes

import (
	"context"
	"errors"
	"testing"

	"github.com/sablierapp/sablier/pkg/provider"
	"github.com/sablierapp/sablier/pkg/sablier"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	batchv1 "k8s.io/api/batch/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes/fake"
	kt "k8s.io/client-go/testing"
	"k8s.io/client-go/tools/cache"
)

func TestBatchCancellation(t *testing.T) {
	ctx := context.Background()
	c := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: "cron", Namespace: "ns", UID: "parent", Labels: map[string]string{"sablierapp.dev/enable": "true"}}}
	child := func(name string, uid types.UID) *batchv1.Job {
		return &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: "ns", UID: types.UID(name), OwnerReferences: []metav1.OwnerReference{{Kind: "CronJob", Name: c.Name, UID: uid}}}}
	}
	a, b, foreign, done := child("a", c.UID), child("b", c.UID), child("foreign", "other"), child("done", c.UID)
	done.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: "True"}}
	client := fake.NewSimpleClientset(c, a, b, foreign, done)
	p := &Provider{Client: client, delimiter: "_"}
	fail := true
	client.PrependReactor("delete", "jobs", func(action kt.Action) (bool, runtime.Object, error) {
		d := action.(kt.DeleteAction)
		o := d.GetDeleteOptions()
		if o.GracePeriodSeconds != nil || o.PropagationPolicy == nil || *o.PropagationPolicy != metav1.DeletePropagationForeground || o.Preconditions == nil || *o.Preconditions.UID != types.UID(d.GetName()) {
			t.Fatal("unsafe deletion options", o)
		}
		obj, e := client.Tracker().Get(batchv1.SchemeGroupVersion.WithResource("cronjobs"), "ns", "cron")
		parent, _ := obj.(*batchv1.CronJob)
		if e != nil || !suspended(parent.Spec.Suspend) {
			t.Fatal("deleted before suspension")
		}
		if d.GetName() == "b" && fail {
			return true, nil, errors.New("denied")
		}
		return false, nil, nil
	})
	name := CronJobName(c, ParseOptions{Delimiter: "_"})
	if err := p.batchSetActive(ctx, name, false); err == nil {
		t.Fatal("expected partial failure")
	}
	fail = false
	if err := p.batchSetActive(ctx, name, false); err != nil {
		t.Fatal(err)
	}
	if err := p.batchSetActive(ctx, name, false); err != nil {
		t.Fatal(err)
	}
	jobs, _ := client.BatchV1().Jobs("ns").List(ctx, metav1.ListOptions{})
	if len(jobs.Items) != 2 {
		t.Fatal(jobs.Items)
	}
	if err := p.batchSetActive(ctx, name, true); err != nil {
		t.Fatal(err)
	}
	jobs, _ = client.BatchV1().Jobs("ns").List(ctx, metav1.ListOptions{})
	if len(jobs.Items) != 2 {
		t.Fatal("recreated jobs")
	}
}
func TestBatchDiscoveryAndEvents(t *testing.T) {
	ctx := context.Background()
	yes := true
	j := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "job", Namespace: "ns", UID: "job", Labels: map[string]string{"sablier.enable": "true"}, Annotations: map[string]string{"sablierapp.dev/group": "a,b"}}, Spec: batchv1.JobSpec{Suspend: &yes}}
	c := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: "cron", Namespace: "ns", Labels: map[string]string{"sablierapp.dev/enable": "true", "sablierapp.dev/group": "a"}}}
	p := &Provider{Client: fake.NewSimpleClientset(j, c), delimiter: "~"}
	list, e := p.batchList(ctx, provider.InstanceListOptions{})
	if e != nil || len(list) != 1 {
		t.Fatal(list, e)
	}
	list, e = p.batchList(ctx, provider.InstanceListOptions{All: true})
	if e != nil || len(list) != 2 {
		t.Fatal(list, e)
	}
	if len(p.jobInfo(j).Groups) != 2 {
		t.Fatal("annotation groups missing")
	}
	if p.cronJobInfo(c).Status != sablier.InstanceStatusReady {
		t.Fatal("cronjob should be ready")
	}
	if e = p.batchSetActive(ctx, JobName(j, ParseOptions{Delimiter: "~"}), true); e != nil {
		t.Fatal(e)
	}
	inspected, e := p.batchInspect(ctx, JobName(j, ParseOptions{Delimiter: "~"}))
	if e != nil || inspected.Status != sablier.InstanceStatusReady {
		t.Fatal(inspected, e)
	}
	j.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobFailed, Status: "True"}}
	if p.jobInfo(j).Status != sablier.InstanceStatusError {
		t.Fatal("lost terminal status")
	}
	out := make(chan sablier.InstanceEvent, 8)
	h := p.batchEventHandler(ctx, out, true, true, true, true, true)
	h.DeleteFunc(cache.DeletedFinalStateUnknown{Obj: j})
	if ev := <-out; ev.Type != provider.InstanceEventRemoved || ev.Info.Name != "job~ns~job~1" {
		t.Fatal(ev)
	}
	h.AddFunc(&batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "unmanaged"}})
	if len(out) != 1 {
		t.Fatal("unmanaged job emitted")
	}
}
func TestScaleConflictRetries(t *testing.T) {
	for _, kind := range []string{"deployment", "statefulset"} {
		for _, mode := range []string{"recover", "exhaust", "other", "already", "cancel"} {
			t.Run(kind+"/"+mode, func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				client := fake.NewSimpleClientset()
				p := &Provider{Client: client}
				reads, writes := 0, 0
				cause := errors.New("forbidden")
				client.PrependReactor("get", "*", func(a kt.Action) (bool, runtime.Object, error) {
					reads++
					replicas := int32(2)
					if mode == "already" {
						replicas = 0
					}
					return true, &autoscalingv1.Scale{Spec: autoscalingv1.ScaleSpec{Replicas: replicas}}, nil
				})
				client.PrependReactor("update", "*", func(a kt.Action) (bool, runtime.Object, error) {
					writes++
					if mode == "other" {
						return true, nil, cause
					}
					if mode == "cancel" {
						cancel()
					}
					if mode == "recover" && writes > 1 {
						return true, &autoscalingv1.Scale{}, nil
					}
					return true, nil, apierrors.NewConflict(schema.GroupResource{Resource: kind}, "x", cause)
				})
				err := p.scale(ctx, ParsedName{Kind: kind, Namespace: "ns", Name: "x"}, 0)
				switch mode {
				case "recover":
					if err != nil || reads != 2 || writes != 2 {
						t.Fatal(reads, writes, err)
					}
				case "already":
					if err != nil || writes != 0 {
						t.Fatal(err, writes)
					}
				case "other":
					if !errors.Is(err, cause) || reads != 1 {
						t.Fatal(err, reads)
					}
				case "exhaust":
					if !apierrors.IsConflict(err) || reads != 5 {
						t.Fatal(err, reads)
					}
				case "cancel":
					if !errors.Is(err, context.Canceled) || writes != 1 {
						t.Fatal(err, writes)
					}
				}
			})
		}
	}
}

func TestBatchChildRaceAndMembershipUpdates(t *testing.T) {
	ctx := context.Background()
	yes := true
	c := &batchv1.CronJob{ObjectMeta: metav1.ObjectMeta{Name: "cron", Namespace: "ns", UID: "uid", Labels: map[string]string{"sablierapp.dev/enable": "true"}}, Spec: batchv1.CronJobSpec{Suspend: &yes}}
	j := &batchv1.Job{ObjectMeta: metav1.ObjectMeta{Name: "late", Namespace: "ns", UID: "job", OwnerReferences: []metav1.OwnerReference{{Kind: "CronJob", Name: c.Name, UID: c.UID}}}}
	client := fake.NewSimpleClientset(c, j)
	p := &Provider{Client: client, delimiter: "_"}
	out := make(chan sablier.InstanceEvent, 20)
	h := p.batchEventHandler(ctx, out, true, true, true, true, true)
	h.AddFunc(j)
	if len(out) != 0 {
		t.Fatal("child registered independently")
	}
	if _, err := client.BatchV1().Jobs("ns").Get(ctx, j.Name, metav1.GetOptions{}); !apierrors.IsNotFound(err) {
		t.Fatal("late child survived", err)
	}
	if err := p.cancelJob(ctx, j); err != nil {
		t.Fatal("NotFound was not successful", err)
	}
	own := j.DeepCopy()
	own.OwnerReferences = nil
	own.Labels = map[string]string{"sablierapp.dev/enable": "true", "sablierapp.dev/group": "a"}
	h.AddFunc(own)
	<-out
	changed := own.DeepCopy()
	changed.Annotations = map[string]string{"sablierapp.dev/group": "b,c"}
	h.UpdateFunc(own, changed)
	ev := <-out
	if ev.Type != provider.InstanceEventUpdated || len(ev.Info.Groups) != 2 {
		t.Fatal(ev)
	}
	done := changed.DeepCopy()
	done.Status.Conditions = []batchv1.JobCondition{{Type: batchv1.JobComplete, Status: "True"}}
	h.UpdateFunc(changed, done)
	ev = <-out
	if ev.Type != provider.InstanceEventRemoved || ev.Info.Status != sablier.InstanceStatusCompleted {
		t.Fatal(ev)
	}
	<-out
	h.AddFunc(done)
	if len(out) != 0 {
		t.Fatal("terminal member readded")
	}
	deleting := own.DeepCopy()
	now := metav1.Now()
	deleting.DeletionTimestamp = &now
	h.UpdateFunc(own, deleting)
	ev = <-out
	if ev.Type != provider.InstanceEventRemoved {
		t.Fatal(ev)
	}
}
