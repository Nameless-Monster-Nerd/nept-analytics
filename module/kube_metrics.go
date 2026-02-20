package module

import (
	"context"
	"fmt"
	"log"
	"time"

	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	metricsV1beta1 "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"
)

type ServiceResourceUsage struct {
	ProjectID      string
	PodCount       int
	CPUUsage       resource.Quantity
	MemoryUsage    resource.Quantity
	CPURequests    resource.Quantity
	MemoryRequests resource.Quantity
	CPULimits      resource.Quantity
	MemoryLimits   resource.Quantity
}

// GetAllServicesResourceUsage calculates resource usage for all services in the given namespace.
// It uses batch fetching to minimize API calls (O(1) instead of O(N)).
func GetAllServicesResourceUsage(ctx context.Context, clientset *kubernetes.Clientset, metricsClientset *metrics.Clientset, namespace string) (map[string]ServiceResourceUsage, error) {
	// 1. Fetch ALL Services in the namespace (1 API call)
	services, err := clientset.CoreV1().Services(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list services: %w", err)
	}

	// 2. Fetch ALL Pods in the namespace (1 API call)
	allPodsList, err := clientset.CoreV1().Pods(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list pods: %w", err)
	}

	// 3. Fetch ALL Pod Metrics in the namespace (1 API call)
	allPodMetricsList, err := metricsClientset.MetricsV1beta1().PodMetricses(namespace).List(ctx, metav1.ListOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to list pod metrics (ensure metrics-server is running): %w", err)
	}

	// Create a lookup map for pod metrics by pod name for O(1) access
	podMetricsMap := make(map[string]metricsV1beta1.PodMetrics)
	for _, pm := range allPodMetricsList.Items {
		podMetricsMap[pm.Name] = pm
	}

	results := make(map[string]ServiceResourceUsage)

	for _, svc := range services.Items {
		// Skip services without selectors
		if len(svc.Spec.Selector) == 0 {
			continue
		}

		projectID := svc.Labels["projectID"]
		if projectID == "" {
			continue
		}

		// Create a selector to match pods to this service
		selector := labels.SelectorFromSet(svc.Spec.Selector)

		usage := ServiceResourceUsage{
			ProjectID: projectID,
		}

		// Iterate through all cached pods and check if they belong to this service
		for _, pod := range allPodsList.Items {
			if selector.Matches(labels.Set(pod.Labels)) {
				usage.PodCount++

				// Aggregate Requests and Limits from Pod Spec
				for _, container := range pod.Spec.Containers {
					usage.CPURequests.Add(*container.Resources.Requests.Cpu())
					usage.MemoryRequests.Add(*container.Resources.Requests.Memory())
					usage.CPULimits.Add(*container.Resources.Limits.Cpu())
					usage.MemoryLimits.Add(*container.Resources.Limits.Memory())
				}

				// Aggregate Usage from Metrics (if available in our map)
				if podMetric, ok := podMetricsMap[pod.Name]; ok {
					for _, container := range podMetric.Containers {
						usage.CPUUsage.Add(*container.Usage.Cpu())
						usage.MemoryUsage.Add(*container.Usage.Memory())
					}
				}
			}
		}

		results[svc.Name] = usage
	}

	return results, nil
}

// StartResourceMetricsIngestion runs a background loop to fetch missing usage metrics and upload them to ClickHouse.
func StartResourceMetricsIngestion(clientset *kubernetes.Clientset, metricsClientset *metrics.Clientset, interval time.Duration) {
	conn, err := ConnectClickHouse()
	if err != nil {
		log.Printf("Failed to connect to ClickHouse for Resource Metrics: %v", err)
		return
	}

	if err := InitResourceMetricsSchema(conn); err != nil {
		log.Printf("Failed to init ClickHouse schema: %v", err)
		return
	}

	log.Printf("Starting continuous Resource Metrics ingestion every %v...", interval)

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		<-ticker.C

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)

		// Query all namespaces ("")
		usages, err := GetAllServicesResourceUsage(ctx, clientset, metricsClientset, "")
		if err != nil {
			log.Printf("Error fetching resource usages during interval: %v", err)
			cancel()
			continue
		}

		var batch []ResourceMetric
		now := time.Now()

		for svcName, usage := range usages {
			batch = append(batch, ResourceMetric{
				Timestamp:        now,
				ProjectID:        usage.ProjectID,
				ServiceName:      svcName,
				MemoryUsageBytes: usage.MemoryUsage.Value(),
				// Convert milli-cores to nano-cores for more precision without float, since k8s stores internally as string anyway
				CPUUsageNanocores: usage.CPUUsage.MilliValue() * 1000000,
			})
		}

		if len(batch) > 0 {
			if err := InsertResourceMetricsBatch(conn, batch); err != nil {
				log.Printf("Error inserting batch of %d resource metrics: %v", len(batch), err)
			} else {
				log.Printf("Inserted %d resource metrics into ClickHouse.", len(batch))
			}
		}

		cancel()
	}
}
