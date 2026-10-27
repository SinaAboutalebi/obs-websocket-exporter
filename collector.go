package main

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

type Stats struct {
	CPU           float64 `json:"cpuUsage"`
	Memory        float64 `json:"memoryUsage"`
	Disk          float64 `json:"availableDiskSpace"`
	FPS           float64 `json:"activeFps"`
	RenderTime    float64 `json:"averageFrameRenderTime"`
	RenderSkipped float64 `json:"renderSkippedFrames"`
	RenderTotal   float64 `json:"renderTotalFrames"`
	OutputSkipped float64 `json:"outputSkippedFrames"`
	OutputTotal   float64 `json:"outputTotalFrames"`
}
type Output struct {
	Active       bool    `json:"outputActive"`
	Reconnecting bool    `json:"outputReconnecting"`
	Paused       bool    `json:"outputPaused"`
	Duration     float64 `json:"outputDuration"`
	Congestion   float64 `json:"outputCongestion"`
	Bytes        float64 `json:"outputBytes"`
	Skipped      float64 `json:"outputSkippedFrames"`
	Frames       float64 `json:"outputTotalFrames"`
}
type Snapshot struct {
	Stats          Stats
	Stream, Record Output
}
type targetState struct {
	mu          sync.RWMutex
	name        string
	up          bool
	lastSuccess time.Time
	snapshot    Snapshot
	failures    float64
}
type metricDef struct {
	desc  *prometheus.Desc
	kind  prometheus.ValueType
	value func(Snapshot) float64
}
type Collector struct {
	targets          []*targetState
	up, last, errors *prometheus.Desc
	metrics          []metricDef
}

func boolValue(v bool) float64 {
	if v {
		return 1
	}
	return 0
}
func newCollector(instances []Instance) *Collector {
	c := &Collector{}
	desc := func(name, help string) *prometheus.Desc {
		return prometheus.NewDesc("obs_"+name, help, []string{"obs_instance"}, nil)
	}
	c.up = desc("up", "Whether the latest OBS connection or complete poll attempt succeeded.")
	c.last = desc("last_successful_poll_timestamp_seconds", "Unix timestamp of the last successful complete poll; zero before first success.")
	c.errors = desc("collection_errors_total", "Failed connection or poll attempts.")
	for _, instance := range instances {
		c.targets = append(c.targets, &targetState{name: instance.Name})
	}
	add := func(name, help string, kind prometheus.ValueType, value func(Snapshot) float64) {
		c.metrics = append(c.metrics, metricDef{desc(name, help), kind, value})
	}
	g, counter := prometheus.GaugeValue, prometheus.CounterValue
	add("global_active_fps", "Current rendered frames per second.", g, func(s Snapshot) float64 { return s.Stats.FPS })
	add("global_average_frame_time_seconds", "Average frame rendering time in seconds.", g, func(s Snapshot) float64 { return s.Stats.RenderTime / 1000 })
	add("global_cpu_usage_percent", "OBS CPU usage in percent.", g, func(s Snapshot) float64 { return s.Stats.CPU })
	add("global_memory_usage_megabytes", "OBS memory usage in MB as reported by OBS.", g, func(s Snapshot) float64 { return s.Stats.Memory })
	add("global_recording_disk_available_megabytes", "Recording storage available space in MB as reported by OBS.", g, func(s Snapshot) float64 { return s.Stats.Disk })
	add("global_render_frames_total", "Total render thread frames reported by OBS.", counter, func(s Snapshot) float64 { return s.Stats.RenderTotal })
	add("global_render_skipped_frames_total", "Frames skipped by the render thread.", counter, func(s Snapshot) float64 { return s.Stats.RenderSkipped })
	add("global_output_frames_total", "Total output thread frames reported by OBS.", counter, func(s Snapshot) float64 { return s.Stats.OutputTotal })
	add("global_output_skipped_frames_total", "Frames skipped by the output thread (encoding lag).", counter, func(s Snapshot) float64 { return s.Stats.OutputSkipped })
	add("stream_active", "Whether streaming is active.", g, func(s Snapshot) float64 { return boolValue(s.Stream.Active) })
	add("stream_reconnecting", "Whether streaming is reconnecting.", g, func(s Snapshot) float64 { return boolValue(s.Stream.Reconnecting) })
	add("stream_congestion", "Stream congestion reported by OBS.", g, func(s Snapshot) float64 { return s.Stream.Congestion })
	add("stream_duration_seconds", "Duration of the current stream in seconds.", g, func(s Snapshot) float64 { return s.Stream.Duration / 1000 })
	add("stream_bytes_total", "Bytes sent during the current stream session.", counter, func(s Snapshot) float64 { return s.Stream.Bytes })
	add("stream_frames_total", "Frames delivered by the stream output process.", counter, func(s Snapshot) float64 { return s.Stream.Frames })
	add("stream_skipped_frames_total", "Skipped frames reported by the stream output process.", counter, func(s Snapshot) float64 { return s.Stream.Skipped })
	add("record_active", "Whether recording is active.", g, func(s Snapshot) float64 { return boolValue(s.Record.Active) })
	add("record_paused", "Whether recording is paused.", g, func(s Snapshot) float64 { return boolValue(s.Record.Paused) })
	add("record_duration_seconds", "Duration of the current recording in seconds.", g, func(s Snapshot) float64 { return s.Record.Duration / 1000 })
	add("record_bytes_total", "Bytes written during the current recording session.", counter, func(s Snapshot) float64 { return s.Record.Bytes })
	return c
}
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.up
	ch <- c.last
	ch <- c.errors
	for _, m := range c.metrics {
		ch <- m.desc
	}
}
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	for _, t := range c.targets {
		t.mu.RLock()
		up, last, failures, snap := t.up, t.lastSuccess, t.failures, t.snapshot
		t.mu.RUnlock()
		ch <- prometheus.MustNewConstMetric(c.up, prometheus.GaugeValue, boolValue(up), t.name)
		timestamp := float64(0)
		if !last.IsZero() {
			timestamp = float64(last.Unix())
		}
		ch <- prometheus.MustNewConstMetric(c.last, prometheus.GaugeValue, timestamp, t.name)
		ch <- prometheus.MustNewConstMetric(c.errors, prometheus.CounterValue, failures, t.name)
		if up {
			for _, m := range c.metrics {
				ch <- prometheus.MustNewConstMetric(m.desc, m.kind, m.value(snap), t.name)
			}
		}
	}
}
func (t *targetState) fail(err error) {
	t.mu.Lock()
	t.up = false
	t.failures++
	t.mu.Unlock()
	slog.Warn("OBS collection unavailable", "instance", t.name, "error", err.Error())
}
func (t *targetState) run(ctx context.Context, cfg Config, target Instance) {
	defer func() { t.mu.Lock(); t.up = false; t.mu.Unlock() }()
	addresses := target.endpoints()
	if len(addresses) == 0 {
		t.fail(fmt.Errorf("no configured endpoints"))
		return
	}
	next, failures := 0, 0
	for ctx.Err() == nil {
		candidate := target
		candidate.URL = addresses[next]
		next = (next + 1) % len(addresses)
		client, err := connect(ctx, candidate, cfg.timeout)
		if err == nil {
			slog.Info("Connected to OBS", "instance", target.Name, "endpoint", candidate.URL)
			succeeded := false
			succeeded, err = t.pollConnection(ctx, cfg, client)
			if succeeded {
				failures = 0
			}
		}
		if ctx.Err() != nil {
			return
		}
		t.fail(fmt.Errorf("endpoint %s: %w", candidate.URL, err))
		failures++
		// Try every alternate immediately before backing off for a new round.
		if failures >= len(addresses) {
			if !pause(ctx, cfg.reconnect) {
				return
			}
			failures = 0
		}
	}
}

func (t *targetState) pollConnection(ctx context.Context, cfg Config, client *rpcClient) (bool, error) {
	stop := context.AfterFunc(ctx, func() { client.conn.Close() })
	defer stop()
	defer client.conn.Close()
	succeeded := false
	for ctx.Err() == nil {
		var snap Snapshot
		err := client.call("GetStats", &snap.Stats)
		if err == nil {
			err = client.call("GetStreamStatus", &snap.Stream)
		}
		if err == nil {
			err = client.call("GetRecordStatus", &snap.Record)
		}
		if err != nil {
			return succeeded, err
		}
		t.mu.Lock()
		t.snapshot = snap
		t.lastSuccess = time.Now()
		t.up = true
		t.mu.Unlock()
		succeeded = true
		if !pause(ctx, cfg.poll) {
			break
		}
	}
	return succeeded, ctx.Err()
}

func pause(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
