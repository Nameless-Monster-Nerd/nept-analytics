package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/client-go/util/homedir"
	metrics "k8s.io/metrics/pkg/client/clientset/versioned"

	"github.com/Nameless-Monster-Nerd/nept-analytics/module"
)

func main() {
	var kubeconfig *string
	if home := homedir.HomeDir(); home != "" {
		kubeconfig = flag.String("kubeconfig", filepath.Join(home, ".kube", "config"), "(optional) absolute path to the kubeconfig file")
	} else {
		kubeconfig = flag.String("kubeconfig", "", "absolute path to the kubeconfig file")
	}
	namespace := flag.String("namespace", "", "Namespace to inspect (leave empty for all)")
	flag.Parse()

	if err := run(*kubeconfig, *namespace); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func run(kubeconfig, namespace string) error {
	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", kubeconfig)
	if err != nil {
		return fmt.Errorf("failed to build kubeconfig: %w", err)
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create kubernetes clientset: %w", err)
	}

	// create the metrics clientset
	metricsClientset, err := metrics.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("failed to create metrics clientset: %w", err)
	}

	// Start ClickHouse memory/cpu ingester as a goroutine (runs every 60 seconds)
	go module.StartResourceMetricsIngestion(clientset, metricsClientset, 5*time.Second)

	// Start Traefik Log Ingestion as a background goroutine
	go module.StartTraefikLogIngestion()

	// Block forever since workers are running
	fmt.Println("NEPT Analytics started. Ingestors are running in the background...")

	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	})

	fmt.Println("Starting health server on :80...")
	if err := http.ListenAndServe(":80", nil); err != nil {
		fmt.Fprintf(os.Stderr, "HTTP server error: %v\n", err)
	}

	return nil
}
