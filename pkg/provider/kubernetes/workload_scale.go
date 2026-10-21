package kubernetes

import (
	"context"
	"fmt"
	autoscalingv1 "k8s.io/api/autoscaling/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/util/retry"
)

type Workload interface {
	GetScale(ctx context.Context, workloadName string, options metav1.GetOptions) (*autoscalingv1.Scale, error)
	UpdateScale(ctx context.Context, workloadName string, scale *autoscalingv1.Scale, opts metav1.UpdateOptions) (*autoscalingv1.Scale, error)
}

func (p *Provider) scale(ctx context.Context, config ParsedName, replicas int32) error {
	var workload Workload

	switch config.Kind {
	case "deployment":
		workload = p.Client.AppsV1().Deployments(config.Namespace)
	case "statefulset":
		workload = p.Client.AppsV1().StatefulSets(config.Namespace)
	default:
		return fmt.Errorf("unsupported kind \"%s\" must be one of \"deployment\", \"statefulset\"", config.Kind)
	}

	err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		if err := ctx.Err(); err != nil {
			return err
		}
		s, err := workload.GetScale(ctx, config.Name, metav1.GetOptions{})
		if err != nil {
			return err
		}
		if s.Spec.Replicas == replicas {
			return nil
		}
		s.Spec.Replicas = replicas
		_, err = workload.UpdateScale(ctx, config.Name, s, metav1.UpdateOptions{})
		return err
	})
	if ctx.Err() != nil {
		err = ctx.Err()
	}
	if err != nil {
		return fmt.Errorf("scale %s %s/%s to %d replicas: %w", config.Kind, config.Namespace, config.Name, replicas, err)
	}
	return nil
}
