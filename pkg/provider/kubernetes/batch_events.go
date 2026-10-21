package kubernetes

import (
	"context"
	"reflect"
	"time"

	"github.com/sablierapp/sablier/pkg/provider"
	"github.com/sablierapp/sablier/pkg/sablier"
	batchv1 "k8s.io/api/batch/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/tools/cache"
)

func batchManaged(obj metav1.Object) bool {
	labels := obj.GetLabels()
	return labels[sablierPublicPrefix+"enable"] == "true" || labels[sablier.LabelEnable] == "true"
}

// Check the current parent, rather than an informer snapshot, to cover Jobs
// created concurrently with the suspension patch and child listing.
func (p *Provider) cancelSuspendedChild(ctx context.Context, j *batchv1.Job) {
	if jobTerminal(j) {
		return
	}
	for _, owner := range j.OwnerReferences {
		if owner.Kind != "CronJob" {
			continue
		}
		c, err := p.Client.BatchV1().CronJobs(j.Namespace).Get(ctx, owner.Name, metav1.GetOptions{})
		if err != nil {
			if p.l != nil {
				p.l.WarnContext(ctx, "cannot inspect job owner", "job", j.Name, "error", err)
			}
			continue
		}
		if ownedByCronJob(j, c) && batchManaged(c) && suspended(c.Spec.Suspend) {
			if err := p.cancelJob(ctx, j); err != nil && p.l != nil {
				p.l.ErrorContext(ctx, "cannot cancel suspended cronjob child", "job", j.Name, "error", err)
			}
		}
	}
}
func (p *Provider) batchEventHandler(ctx context.Context, out chan<- sablier.InstanceEvent, stopped, started, created, removed, updated bool) cache.ResourceEventHandlerFuncs {
	info := func(obj any) (sablier.InstanceInfo, bool) {
		if j, ok := eventObject[*batchv1.Job](obj); ok {
			return p.jobInfo(j), batchManaged(j) && !jobTerminal(j) && j.DeletionTimestamp == nil
		}
		if c, ok := eventObject[*batchv1.CronJob](obj); ok {
			return p.cronJobInfo(c), batchManaged(c)
		}
		return sablier.InstanceInfo{}, false
	}
	emit := func(t provider.InstanceEventType, i sablier.InstanceInfo) {
		select {
		case out <- sablier.InstanceEvent{Type: t, Info: i}:
		case <-ctx.Done():
		}
	}
	return cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj any) {
			if j, ok := eventObject[*batchv1.Job](obj); ok {
				p.cancelSuspendedChild(ctx, j)
			}
			i, ok := info(obj)
			if ok && created {
				emit(provider.InstanceEventCreated, i)
			}
		},
		UpdateFunc: func(old, new any) {
			if j, ok := eventObject[*batchv1.Job](new); ok {
				p.cancelSuspendedChild(ctx, j)
			}
			i, ok := info(new)
			previous, was := info(old)
			if was && !ok {
				if removed {
					emit(provider.InstanceEventRemoved, i)
				}
				if stopped && previous.Status != i.Status {
					emit(provider.InstanceEventStopped, i)
				}
				return
			}
			if !ok {
				return
			}
			if !was {
				if created {
					emit(provider.InstanceEventCreated, i)
				}
				return
			}
			if reflect.DeepEqual(i, previous) {
				return
			}
			if i.Status == sablier.InstanceStatusStopped || i.Status == sablier.InstanceStatusCompleted || i.Status == sablier.InstanceStatusError {
				if stopped {
					emit(provider.InstanceEventStopped, i)
				}

			} else if started && previous.Status != i.Status {
				emit(provider.InstanceEventStarted, i)
			}
			if updated {
				emit(provider.InstanceEventUpdated, i)
			}
		},
		DeleteFunc: func(obj any) {
			i, ok := info(obj)
			if j, valid := eventObject[*batchv1.Job](obj); valid {
				i, ok = p.jobInfo(j), batchManaged(j)
			}
			if !ok {
				return
			}
			if removed {
				emit(provider.InstanceEventRemoved, i)
			}
			if stopped {
				i.Status = sablier.InstanceStatusStopped
				emit(provider.InstanceEventStopped, i)
			}
		},
	}
}
func (p *Provider) watchBatch(ctx context.Context, out chan<- sablier.InstanceEvent, stopped, started, created, removed, updated bool) {
	factory := informers.NewSharedInformerFactory(p.Client, 2*time.Second)
	handler := p.batchEventHandler(ctx, out, stopped, started, created, removed, updated)
	cs := factory.Batch().V1().CronJobs().Informer()
	js := factory.Batch().V1().Jobs().Informer()
	_, _ = cs.AddEventHandler(handler)
	_, _ = js.AddEventHandler(handler)
	go cs.Run(ctx.Done())
	go js.Run(ctx.Done())
}
