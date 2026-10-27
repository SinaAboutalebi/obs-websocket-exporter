package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func main() {
	if err := run(); err != nil {
		slog.Error("Exporter stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	path := flag.String("config", "config.yml", "Path to YAML configuration (.yml or .yaml)")
	flag.Parse()
	cfg, err := loadConfig(*path)
	if err != nil {
		return fmt.Errorf("configuration: %w", err)
	}
	listener, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return err
	}
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	collector := newCollector(cfg.Instances)
	registry := prometheus.NewRegistry()
	registry.MustRegister(collector, prometheus.NewGoCollector(), prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("ok\n")) })
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		fmt.Fprint(w, `<h1>OBS WebSocket Exporter</h1><a href="/metrics">Metrics</a>`)
	})
	server := &http.Server{Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 60 * time.Second}
	var wg sync.WaitGroup
	for i, target := range cfg.Instances {
		wg.Add(1)
		go func(t *targetState, target Instance) { defer wg.Done(); t.run(ctx, cfg, target) }(collector.targets[i], target)
	}
	slog.Info("Exporter listening", "address", listener.Addr().String(), "instances", len(cfg.Instances))
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case <-ctx.Done():
	case err = <-done:
		if err == http.ErrServerClosed {
			err = nil
		}
	}
	cancel()
	shutdown, release := context.WithTimeout(context.Background(), 5*time.Second)
	defer release()
	if shutdownErr := server.Shutdown(shutdown); shutdownErr != nil {
		server.Close()
		if err == nil {
			err = shutdownErr
		}
	}
	wg.Wait()
	return err
}
