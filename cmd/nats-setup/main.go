package main

import (
	"log"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/stream"
)

func main() {
	cfg := config.Load("processor", "")
	if err := stream.SetupJetStream(
		cfg.NATSURL,
		cfg.NATSStream,
		cfg.NATSSubject,
		cfg.NATSDLQSubject,
		cfg.NATSDurable,
		cfg.NATSMaxDeliver,
	); err != nil {
		log.Fatal(err)
	}
}
