package main

import (
	"log"

	"github.com/PratypartyY2K/real-time-log-aggregator/internal/config"
	"github.com/PratypartyY2K/real-time-log-aggregator/internal/stream"
)

func main() {
	cfg := config.Load("processor", "")
	if err := stream.SetupJetStream(cfg.NATSURL, cfg.NATSStream, cfg.NATSSubject, cfg.NATSDurable); err != nil {
		log.Fatal(err)
	}
}
