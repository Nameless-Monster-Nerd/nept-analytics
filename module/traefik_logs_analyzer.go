package module

import (
	"encoding/json"
	"fmt"
	"iter"
	"log"
	"net/http"
	"os"
	"strconv"
	"time"

	"github.com/joho/godotenv"
)

func NewTraefikLog() iter.Seq[*TraefikLog] {
	return func(yield func(*TraefikLog) bool) {
		// 1. Define the VictoriaLogs tailing URL
		// Added start_offset=5m to catch recent logs upon connection.
		err := godotenv.Load()
		if err != nil {
			fmt.Println("Error loading .env file: %v", err)
		}
		urlPart := os.Getenv("VICTORIA_LOGSENDPOINTS")

		url := urlPart + "/select/logsql/tail?query=kubernetes.container_name:traefik&start_offset=5m"
		fmt.Println(url)

		fmt.Printf("Connecting to VictoriaLogs: %s\n", url)

		// 2. Create the request
		req, err := http.NewRequest("GET", url, nil)
		if err != nil {
			fmt.Println("Error creating request: %v", err)
		}

		// 3. Execute the request
		// Use a standard client; for production, consider setting a
		// Transport with a long IdleConnTimeout, but NO global Timeout.
		client := &http.Client{}
		resp, err := client.Do(req)
		if err != nil {
			fmt.Println("Error connecting to VictoriaLogs:", err)
			return
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			fmt.Println("Server returned non-200 status:", resp.StatusCode)
			return
		}

		fmt.Println("Connection established. Streaming logs...")
		fmt.Println("--------------------------------------------------")

		// 4. Read the stream using json.Decoder for NDJSON
		decoder := json.NewDecoder(resp.Body)

		for {
			var logEntry TraefikLog
			if err := decoder.Decode(&logEntry); err != nil {
				// EOF means stream ended
				if err.Error() == "EOF" {
					break
				}
				fmt.Fprintf(os.Stderr, "Error decoding stream: %v\n", err)
				break
			}

			// Yield the parsed log to the iterator
			if !yield(&logEntry) {
				return
			}
		}
	}
}

// StartTraefikLogIngestion connects to ClickHouse, initializes the schema,
// and continuously reads live Traefik logs, inserting them in batches.
func StartTraefikLogIngestion() {
	conn, err := ConnectClickHouse()
	if err != nil {
		fmt.Println("Failed to connect to ClickHouse:", err)
		return
	}

	if err := InitClickHouseSchema(conn); err != nil {
		fmt.Println("Failed to init ClickHouse schema:", err)
		return
	}

	log.Println("Starting continuous Traefik log ingestion...")

	var batch []*TraefikLog
	const batchSize = 1000
	lastFlush := time.Now()

	for logEntry := range NewTraefikLog() {
		// Only collect requests that probably reached a backend
		// But for now we just collect all. You can filter if you'd like.
		batch = append(batch, logEntry)

		// Flush condition: batch is huge, or 2 seconds have passed
		if len(batch) >= batchSize || time.Since(lastFlush) > 2*time.Second {
			if len(batch) > 0 {
				if err := InsertTraefikLogBatch(conn, batch); err != nil {
					log.Printf("Error inserting batch of %d logs: %v", len(batch), err)
				} else {
					fmt.Printf("Inserted batch of %d Traefik logs to ClickHouse.\n", len(batch))
				}
				// Reset batch
				batch = make([]*TraefikLog, 0, batchSize)
				lastFlush = time.Now()
			}
		}
	}
}

// parseDuration helper to safely parse a duration string like "1.2ms" to a float64 in ms
func parseDuration(d string) float64 {
	if d == "" {
		return 0.0
	}
	parsed, err := time.ParseDuration(d)
	if err == nil {
		return float64(parsed.Microseconds()) / 1000.0 // Return strictly as MS Float64
	}
	// fallback parsing just in case Traefik throws something weird
	val, err := strconv.ParseFloat(d, 64)
	if err == nil {
		return val
	}
	return 0.0
}

func parseStatus(s string) int {
	if s == "" {
		return 0
	}
	val, err := strconv.Atoi(s)
	if err == nil {
		return val
	}
	return 0
}

type TraefikLog struct {
	Time                                        time.Time `json:"_time"`
	StreamID                                    string    `json:"_stream_id"`
	Stream                                      string    `json:"_stream"`
	Msg                                         string    `json:"_msg"`
	ClientAddr                                  string    `json:"ClientAddr"`
	ClientHost                                  string    `json:"ClientHost"`
	ClientPort                                  string    `json:"ClientPort"`
	ClientUsername                              string    `json:"ClientUsername"`
	DownstreamContentSize                       string    `json:"DownstreamContentSize"`
	DownstreamStatus                            string    `json:"DownstreamStatus"`
	Duration                                    string    `json:"Duration"`
	OriginContentSize                           string    `json:"OriginContentSize"`
	OriginDuration                              string    `json:"OriginDuration"`
	OriginStatus                                string    `json:"OriginStatus"`
	Overhead                                    string    `json:"Overhead"`
	RequestAddr                                 string    `json:"RequestAddr"`
	RequestContentSize                          string    `json:"RequestContentSize"`
	RequestCount                                string    `json:"RequestCount"`
	RequestHost                                 string    `json:"RequestHost"`
	RequestMethod                               string    `json:"RequestMethod"`
	RequestPath                                 string    `json:"RequestPath"`
	RequestPort                                 string    `json:"RequestPort"`
	RequestProtocol                             string    `json:"RequestProtocol"`
	RequestScheme                               string    `json:"RequestScheme"`
	RetryAttempts                               string    `json:"RetryAttempts"`
	RouterName                                  string    `json:"RouterName"`
	ServiceAddr                                 string    `json:"ServiceAddr"`
	ServiceName                                 string    `json:"ServiceName"`
	ServiceURL                                  string    `json:"ServiceURL"`
	StartLocal                                  time.Time `json:"StartLocal"`
	StartUTC                                    time.Time `json:"StartUTC"`
	DownstreamContentLength                     string    `json:"downstream_Content-Length"`
	DownstreamContentType                       string    `json:"downstream_Content-Type"`
	DownstreamDate                              string    `json:"downstream_Date"`
	EntryPointName                              string    `json:"entryPointName"`
	KubernetesContainerID                       string    `json:"kubernetes.container_id"`
	KubernetesContainerName                     string    `json:"kubernetes.container_name"`
	KubernetesPodIP                             string    `json:"kubernetes.pod_ip"`
	KubernetesPodLabelsAppKubernetesIoInstance  string    `json:"kubernetes.pod_labels.app.kubernetes.io/instance"`
	KubernetesPodLabelsAppKubernetesIoManagedBy string    `json:"kubernetes.pod_labels.app.kubernetes.io/managed-by"`
	KubernetesPodLabelsAppKubernetesIoName      string    `json:"kubernetes.pod_labels.app.kubernetes.io/name"`
	KubernetesPodLabelsHelmShChart              string    `json:"kubernetes.pod_labels.helm.sh/chart"`
	KubernetesPodLabelsPodTemplateHash          string    `json:"kubernetes.pod_labels.pod-template-hash"`
	KubernetesPodName                           string    `json:"kubernetes.pod_name"`
	KubernetesPodNamespace                      string    `json:"kubernetes.pod_namespace"`
	KubernetesPodNodeName                       string    `json:"kubernetes.pod_node_name"`
	Level                                       string    `json:"level"`
	OriginContentLength                         string    `json:"origin_Content-Length"`
	OriginContentType                           string    `json:"origin_Content-Type"`
	OriginDate                                  string    `json:"origin_Date"`
	RequestAcceptEncoding                       string    `json:"request_Accept-Encoding"`
	RequestCdnLoop                              string    `json:"request_Cdn-Loop"`
	RequestCfConnectingIP                       string    `json:"request_Cf-Connecting-Ip"`
	RequestCfConnectingO2O                      string    `json:"request_Cf-Connecting-O2o"`
	RequestCfIpcountry                          string    `json:"request_Cf-Ipcountry"`
	RequestCfRay                                string    `json:"request_Cf-Ray"`
	RequestCfVisitor                            string    `json:"request_Cf-Visitor"`
	RequestHost2                                string    `json:"request_Host2"`
	RequestUserAgent                            string    `json:"request_User-Agent"`
	RequestXForwardedHost                       string    `json:"request_X-Forwarded-Host"`
	RequestXForwardedPort                       string    `json:"request_X-Forwarded-Port"`
	RequestXForwardedProto                      string    `json:"request_X-Forwarded-Proto"`
	RequestXForwardedServer                     string    `json:"request_X-Forwarded-Server"`
	RequestXRealIP                              string    `json:"request_X-Real-Ip"`
}
