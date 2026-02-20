package main

import (
	"log"

	"github.com/Nameless-Monster-Nerd/nept-analytics/module"
)

func main() {
	log.Println("Starting Traefik log to ClickHouse migration test...")
	module.StartTraefikLogIngestion()
}
