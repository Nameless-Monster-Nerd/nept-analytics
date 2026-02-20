package module

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/joho/godotenv"
)

// ConnectClickHouse initializes and returns a ClickHouse driver.Conn
func ConnectClickHouse() (driver.Conn, error) {
	// Load .env file if it exists (mostly for local development)
	_ = godotenv.Load()

	endpoint := os.Getenv("CH_ENDPOINT")
	user := os.Getenv("CH_USER")
	password := os.Getenv("CH_PASSWORD")

	if endpoint == "" {
		return nil, fmt.Errorf("CH_ENDPOINT is required in environment")
	}

	endpoint = strings.TrimPrefix(endpoint, "http://")
	endpoint = strings.TrimPrefix(endpoint, "https://")

	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{endpoint}, // Expects e.g., "207.244.199.30:9000" or "207.244.199.30:8123" without scheme
		Auth: clickhouse.Auth{
			Database: "default",
			Username: user,
			Password: password,
		},
		Protocol:        clickhouse.HTTP, // Use HTTP protocol since endpoint is likely http://...:8123
		DialTimeout:     time.Second * 30,
		MaxOpenConns:    10,
		MaxIdleConns:    5,
		ConnMaxLifetime: time.Hour,
	})

	if err != nil {
		return nil, fmt.Errorf("failed to open ClickHouse connection: %w", err)
	}

	if err := conn.Ping(context.Background()); err != nil {
		return nil, fmt.Errorf("failed to ping ClickHouse: %w", err)
	}

	return conn, nil
}

// InitClickHouseSchema creates the traefik_logs table if it doesn't exist
func InitClickHouseSchema(conn driver.Conn) error {
	ctx := context.Background()

	// Drop table for schema update (since we are replacing columns)
	// _ = conn.Exec(ctx, "DROP TABLE IF EXISTS traefik_logs")

	// Using MergeTree for local/single-node deployments
	query := `
		CREATE TABLE IF NOT EXISTS traefik_logs (
			timestamp DateTime64(3),
			request_method String,
			request_path String,
			downstream_status Int32,
			duration Float64,
			client_ip String,
			request_host String,
			user_agent String
		) ENGINE = MergeTree()
		ORDER BY timestamp
		TTL toDateTime(timestamp) + INTERVAL 30 DAY
	`

	if err := conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create traefik_logs table: %w", err)
	}

	log.Println("ClickHouse schema initialized successfully.")
	return nil
}

// InsertTraefikLogBatch inserts a slice of TraefikLog into ClickHouse efficiently using batching
func InsertTraefikLogBatch(conn driver.Conn, logs []*TraefikLog) error {
	if len(logs) == 0 {
		return nil
	}

	ctx := context.Background()
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO traefik_logs")
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, l := range logs {
		durationMs := parseDuration(l.Duration)
		status := parseStatus(l.DownstreamStatus)

		err := batch.Append(
			l.Time,
			l.RequestMethod,
			l.RequestPath,
			int32(status),
			durationMs,
			l.RequestCfConnectingIP, // using CF connecting IP as primary Client IP per instructions
			l.RequestHost,
			l.RequestUserAgent,
		)
		if err != nil {
			return fmt.Errorf("failed to append log to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}

// InitResourceMetricsSchema creates the service_resource_metrics table if it doesn't exist
func InitResourceMetricsSchema(conn driver.Conn) error {
	ctx := context.Background()

	// Using MergeTree for local/single-node deployments
	query := `
		CREATE TABLE IF NOT EXISTS service_resource_metrics (
			timestamp DateTime64(3),
			project_id String,
			service_name String,
			memory_usage_bytes Int64,
			cpu_usage_nanocores Int64
		) ENGINE = MergeTree()
		ORDER BY (timestamp, project_id, service_name)
		TTL toDateTime(timestamp) + INTERVAL 90 DAY
	`

	if err := conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("failed to create service_resource_metrics table: %w", err)
	}

	log.Println("Resource Metrics ClickHouse schema initialized successfully.")
	return nil
}

// ResourceMetric represents a single reading to be inserted
type ResourceMetric struct {
	Timestamp         time.Time
	ProjectID         string
	ServiceName       string
	MemoryUsageBytes  int64
	CPUUsageNanocores int64
}

// InsertResourceMetricsBatch inserts a slice of ResourceMetric into ClickHouse efficiently using batching
func InsertResourceMetricsBatch(conn driver.Conn, metrics []ResourceMetric) error {
	if len(metrics) == 0 {
		return nil
	}

	ctx := context.Background()
	batch, err := conn.PrepareBatch(ctx, "INSERT INTO service_resource_metrics")
	if err != nil {
		return fmt.Errorf("failed to prepare batch: %w", err)
	}

	for _, m := range metrics {
		err := batch.Append(
			m.Timestamp,
			m.ProjectID,
			m.ServiceName,
			m.MemoryUsageBytes,
			m.CPUUsageNanocores,
		)
		if err != nil {
			return fmt.Errorf("failed to append metric to batch: %w", err)
		}
	}

	if err := batch.Send(); err != nil {
		return fmt.Errorf("failed to send batch: %w", err)
	}

	return nil
}
