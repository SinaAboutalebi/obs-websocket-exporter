package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

func mockOBS(t *testing.T, password string, requests func(string, int64) (any, bool)) (string, *atomic.Int64) {
	t.Helper()
	var connections atomic.Int64
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		n := connections.Add(1)
		hello := map[string]any{"rpcVersion": 1}
		if password != "" {
			hello["authentication"] = map[string]string{"salt": "test-salt", "challenge": "test-challenge"}
		}
		send := func(op int, d any) error { return conn.WriteJSON(map[string]any{"op": op, "d": d}) }
		if send(0, hello) != nil {
			return
		}
		var identify struct {
			Op int `json:"op"`
			D  struct {
				RPCVersion     int    `json:"rpcVersion"`
				Authentication string `json:"authentication"`
				Events         int    `json:"eventSubscriptions"`
			} `json:"d"`
		}
		if conn.ReadJSON(&identify) != nil {
			return
		}
		if identify.Op != 1 || identify.D.RPCVersion != 1 || identify.D.Events != 0 {
			t.Error("invalid identification")
			return
		}
		if password != "" && identify.D.Authentication != authResponse(password, "test-salt", "test-challenge") {
			return
		}
		if send(2, map[string]int{"negotiatedRpcVersion": 1}) != nil {
			return
		}
		for {
			var request struct {
				Op int `json:"op"`
				D  struct {
					Method string `json:"requestType"`
					ID     string `json:"requestId"`
				} `json:"d"`
			}
			if conn.ReadJSON(&request) != nil {
				return
			}
			data, ok := requests(request.D.Method, n)
			if !ok {
				return
			}
			if send(7, map[string]any{"requestId": request.D.ID, "requestStatus": map[string]any{"result": true, "code": 100}, "responseData": data}) != nil {
				return
			}
		}
	}))
	t.Cleanup(server.Close)
	return "ws" + strings.TrimPrefix(server.URL, "http"), &connections
}
func sampleResponse(method string) any {
	switch method {
	case "GetStats":
		return Stats{FPS: 60, RenderTime: 2.5, CPU: 12, RenderTotal: 500, RenderSkipped: 3, OutputTotal: 490, OutputSkipped: 2}
	case "GetStreamStatus":
		return Output{Active: true, Duration: 12500, Bytes: 64000, Frames: 600, Congestion: 0.2}
	case "GetRecordStatus":
		return Output{Active: true, Paused: true, Duration: 1000, Bytes: 100}
	default:
		return nil
	}
}
func scrape(t *testing.T, c *Collector) string {
	t.Helper()
	registry := prometheus.NewRegistry()
	registry.MustRegister(c)
	w := httptest.NewRecorder()
	promhttp.HandlerFor(registry, promhttp.HandlerOpts{}).ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	if w.Code != 200 {
		t.Fatalf("scrape status %d: %s", w.Code, w.Body.String())
	}
	return w.Body.String()
}
func awaitState(t *testing.T, predicate func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if predicate() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatal("timed out waiting for state")
}
func TestWorkerAuthMetricsFailureAndRecovery(t *testing.T) {
	var fail atomic.Bool
	endpoint, connections := mockOBS(t, "secret", func(method string, n int64) (any, bool) { return sampleResponse(method), !fail.Load() })
	target := Instance{Name: "studio-1", URL: endpoint, password: "secret"}
	cfg := Config{poll: 20 * time.Millisecond, timeout: 100 * time.Millisecond, reconnect: 50 * time.Millisecond}
	c := newCollector([]Instance{target})
	state := c.targets[0]
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); state.run(ctx, cfg, target) }()
	defer func() { cancel(); <-done }()
	awaitState(t, func() bool { state.mu.RLock(); defer state.mu.RUnlock(); return state.up })
	body := scrape(t, c)
	for _, want := range []string{
		`obs_up{obs_instance="studio-1"} 1`,
		`obs_global_active_fps{obs_instance="studio-1"} 60`,
		`obs_global_average_frame_time_seconds{obs_instance="studio-1"} 0.0025`,
		`obs_stream_duration_seconds{obs_instance="studio-1"} 12.5`,
		`obs_stream_bytes_total{obs_instance="studio-1"} 64000`,
		`obs_record_paused{obs_instance="studio-1"} 1`,
	} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s", want)
		}
	}
	fail.Store(true)
	awaitState(t, func() bool { state.mu.RLock(); defer state.mu.RUnlock(); return !state.up && state.failures > 0 })
	body = scrape(t, c)
	if !strings.Contains(body, `obs_up{obs_instance="studio-1"} 0`) || strings.Contains(body, "obs_global_active_fps{") {
		t.Fatal("failed targets must not expose stale OBS metrics")
	}
	state.mu.RLock()
	last := state.lastSuccess
	state.mu.RUnlock()
	if last.IsZero() {
		t.Fatal("last success lost")
	}
	fail.Store(false)
	awaitState(t, func() bool {
		state.mu.RLock()
		defer state.mu.RUnlock()
		return state.up && state.lastSuccess.After(last)
	})
	if connections.Load() < 2 {
		t.Fatal("expected reconnect")
	}
}
func TestAuthenticationRejected(t *testing.T) {
	endpoint, _ := mockOBS(t, "correct", func(method string, n int64) (any, bool) { return sampleResponse(method), true })
	_, err := connect(context.Background(), Instance{URL: endpoint, password: "wrong"}, 100*time.Millisecond)
	if err == nil {
		t.Fatal("wrong password accepted")
	}
}
func TestCancellationInterruptsBlockedRequest(t *testing.T) {
	entered := make(chan struct{})
	release := make(chan struct{})
	endpoint, _ := mockOBS(t, "", func(method string, n int64) (any, bool) { close(entered); <-release; return nil, false })
	defer close(release)
	target := Instance{Name: "blocked", URL: endpoint}
	cfg := Config{timeout: time.Hour, poll: time.Second, reconnect: time.Second}
	c := newCollector([]Instance{target})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { defer close(done); c.targets[0].run(ctx, cfg, target) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("request not received")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked on network read")
	}
}
func TestIndependentTargetsAndInitialHealth(t *testing.T) {
	c := newCollector([]Instance{{Name: "a"}, {Name: "b"}})
	c.targets[0].up = true
	c.targets[0].snapshot.Stats.FPS = 60
	body := scrape(t, c)
	for _, want := range []string{`obs_up{obs_instance="a"} 1`, `obs_up{obs_instance="b"} 0`, `obs_last_successful_poll_timestamp_seconds{obs_instance="b"} 0`} {
		if !strings.Contains(body, want) {
			t.Errorf("missing %s", want)
		}
	}
	if strings.Contains(body, `obs_global_active_fps{obs_instance="b"}`) {
		t.Fatal("unavailable target has metrics")
	}
}
func TestConfigValidation(t *testing.T) {
	t.Setenv("OBS_TEST_PASSWORD", "secret")
	tests := []struct {
		name, body string
		valid      bool
	}{
		{"valid", "instances:\n  - name: a\n    urls: [ws://localhost:4455]\n    password_env: OBS_TEST_PASSWORD\n", true},
		{"no targets", "listen: ':9407'", false},
		{"unknown key", "typo: true", false},
		{"duplicate studios", "instances: [{name: a, urls: ['ws://a']}, {name: a, urls: ['ws://b']}]", false},
		{"bad duration", "poll_interval: 0s\ninstances: [{name: a, urls: ['ws://a']}]", false},
		{"bad URL", "instances: [{name: a, urls: ['http://a']}]", false},
		{"URL credentials", "instances: [{name: a, urls: ['ws://user:password@a']}]", false},
		{"missing password", "instances: [{name: a, urls: ['ws://a'], password_env: OBS_TEST_UNSET}]", false},
		{"trailing document", "instances: [{name: a, urls: ['ws://a']}]\n---\nlisten: ':9408'", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yml")
			if err := os.WriteFile(path, []byte(test.body), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := loadConfig(path)
			if (err == nil) != test.valid {
				t.Fatalf("valid=%v, err=%v", test.valid, err)
			}
			if test.valid && (cfg.Instances[0].password != "secret" || cfg.poll != 5*time.Second) {
				t.Fatal("defaults or password missing")
			}
		})
	}
}
func TestRPCError(t *testing.T) {
	upgrader := websocket.Upgrader{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		var req map[string]any
		if conn.ReadJSON(&req) != nil {
			return
		}
		d := req["d"].(map[string]any)
		conn.WriteJSON(map[string]any{"op": 7, "d": map[string]any{"requestId": d["requestId"], "requestStatus": map[string]any{"result": false, "code": 600, "comment": "sensitive server details"}}})
	}))
	defer server.Close()
	conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	c := rpcClient{conn: conn, timeout: time.Second}
	var stats Stats
	err = c.call("GetStats", &stats)
	if err == nil || !strings.Contains(err.Error(), "600") || strings.Contains(err.Error(), "sensitive") {
		t.Fatalf("unexpected error %v", err)
	}
}
func TestAuthenticationVector(t *testing.T) {
	// Fixture uses the protocol example salt/challenge; expected hash verified with Python hashlib.
	got := authResponse("supersecretpassword", "lM1GncleQOaCu9lT1yeUZhFYnqhsLLP1G5lAGo3ixaI=", "+IxH4CnCiqpX1rM9scsNynZzbOe4KhDeYcTNS3PDaeY=")
	if got != "1Ct943GAT+6YQUUX47Ia/ncufilbe6+oD6lY+5kaCu4=" {
		t.Fatal(fmt.Sprintf("unexpected response %s", got))
	}
}

func TestYAMLConfigAndEndpointValidation(t *testing.T) {
	t.Setenv("OBS_TEST_PASSWORD", "secret")
	tests := []struct {
		name, body string
		valid      bool
	}{
		{"YAML alternatives", `# Same OBS studio on two networks
poll_interval: 2s
instances:
  - name: studio-1
    urls:
      - ws://192.168.1.10:4455
      - ws://10.1.1.10:4455
    password_env: OBS_TEST_PASSWORD
`, true},
		{"YAML single-address shorthand", "instances:\n  - name: studio-1\n    url: ws://localhost:4455\n", true},
		{"unknown nested field", "instances:\n  - name: studio-1\n    urlz: [ws://localhost:4455]\n", false},
		{"both URL forms", `instances: [{name: studio-1, url: 'ws://a', urls: ['ws://b']}]`, false},
		{"duplicate addresses", `instances: [{name: studio-1, urls: ['ws://a', 'ws://a']}]`, false},
		{"invalid alternate", `instances: [{name: studio-1, urls: ['ws://a', 'http://b']}]`, false},
		{"empty addresses", `instances: [{name: studio-1, urls: []}]`, false},
		{"multiple documents", "instances: [{name: a, url: 'ws://a'}]\n---\ninstances: [{name: b, url: 'ws://b'}]\n", false},
		{"duplicate YAML key", "instances: [{name: a, name: b, url: 'ws://a'}]", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yml")
			if err := os.WriteFile(path, []byte(test.body), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := loadConfig(path)
			if (err == nil) != test.valid {
				t.Fatalf("valid=%v err=%v", test.valid, err)
			}
			if test.valid && len(cfg.Instances[0].endpoints()) == 0 {
				t.Fatal("missing endpoints")
			}
			if test.name == "YAML alternatives" && (len(cfg.Instances[0].URLs) != 2 || cfg.Instances[0].password != "secret" || cfg.poll != 2*time.Second) {
				t.Fatal("incorrect YAML values")
			}
		})
	}
}

func TestUnreachablePrimaryUsesAlternateWithoutBackoff(t *testing.T) {
	endpoint, _ := mockOBS(t, "", func(method string, n int64) (any, bool) { return sampleResponse(method), true })
	target := Instance{Name: "studio", URLs: []string{"ws://127.0.0.1:1", endpoint}}
	c := newCollector([]Instance{target})
	cfg := Config{poll: 20 * time.Millisecond, timeout: 100 * time.Millisecond, reconnect: 10 * time.Second}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); c.targets[0].run(ctx, cfg, target) }()
	defer func() { cancel(); <-done }()
	awaitState(t, func() bool { c.targets[0].mu.RLock(); defer c.targets[0].mu.RUnlock(); return c.targets[0].up })
	if !strings.Contains(scrape(t, c), `obs_global_active_fps{obs_instance="studio"} 60`) {
		t.Fatal("alternate not collected with stable studio label")
	}
}

func TestActiveEndpointFailoverAndStickyConnection(t *testing.T) {
	var primaryDown, alternateDown atomic.Bool
	first, firstConnections := mockOBS(t, "shared", func(method string, n int64) (any, bool) {
		if method == "GetStats" {
			stats := sampleResponse("GetStats").(Stats)
			stats.FPS = 30
			return stats, !primaryDown.Load()
		}
		return sampleResponse(method), !primaryDown.Load()
	})
	second, _ := mockOBS(t, "shared", func(method string, n int64) (any, bool) { return sampleResponse(method), !alternateDown.Load() })
	target := Instance{Name: "studio", URLs: []string{first, second}, password: "shared"}
	c := newCollector([]Instance{target})
	state := c.targets[0]
	cfg := Config{poll: 10 * time.Millisecond, timeout: 100 * time.Millisecond, reconnect: 20 * time.Millisecond}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); state.run(ctx, cfg, target) }()
	defer func() { cancel(); <-done }()
	fpsIs := func(fps float64) bool {
		state.mu.RLock()
		defer state.mu.RUnlock()
		return state.up && state.snapshot.Stats.FPS == fps
	}
	awaitState(t, func() bool { return fpsIs(30) })
	primaryDown.Store(true)
	awaitState(t, func() bool { return fpsIs(60) })
	primaryDown.Store(false)
	connectionsBefore := firstConnections.Load()
	// Confirm several more polls complete on the alternate, without probing primary.
	state.mu.RLock()
	last := state.lastSuccess
	state.mu.RUnlock()
	awaitState(t, func() bool {
		state.mu.RLock()
		defer state.mu.RUnlock()
		return state.lastSuccess.Sub(last) >= 50*time.Millisecond
	})
	if firstConnections.Load() != connectionsBefore || !fpsIs(60) {
		t.Fatal("healthy alternate should remain selected")
	}
	alternateDown.Store(true)
	awaitState(t, func() bool { return fpsIs(30) })
	if !strings.Contains(scrape(t, c), `obs_up{obs_instance="studio"} 1`) {
		t.Fatal("studio identity changed")
	}
}

func TestSharedPasswordConfiguration(t *testing.T) {
	t.Setenv("OBS_SHARED_TEST_PASSWORD", "shared-secret")
	t.Setenv("OBS_OVERRIDE_TEST_PASSWORD", "override-secret")
	t.Setenv("OBS_EMPTY_TEST_PASSWORD", "")
	tests := []struct {
		name, shared, override string
		valid                  bool
		passwords              []string
	}{
		{"shared for all", "OBS_SHARED_TEST_PASSWORD", "", true, []string{"shared-secret", "shared-secret"}},
		{"studio override", "OBS_SHARED_TEST_PASSWORD", "OBS_OVERRIDE_TEST_PASSWORD", true, []string{"shared-secret", "override-secret"}},
		{"missing shared password", "OBS_MISSING_TEST_PASSWORD", "", false, nil},
		{"empty shared password", "OBS_EMPTY_TEST_PASSWORD", "", false, nil},
		{"missing override must fail", "OBS_SHARED_TEST_PASSWORD", "OBS_MISSING_TEST_PASSWORD", false, nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config.yml")
			body := fmt.Sprintf("password_env: %s\ninstances:\n  - name: a\n    urls: [ws://192.168.1.1:4455, ws://10.0.0.1:4455]\n  - name: b\n    url: ws://192.168.1.2:4455\n", test.shared)
			if test.override != "" {
				body += fmt.Sprintf("    password_env: %s\n", test.override)
			}
			if err := os.WriteFile(path, []byte(body), 0600); err != nil {
				t.Fatal(err)
			}
			cfg, err := loadConfig(path)
			if (err == nil) != test.valid {
				t.Fatalf("valid=%v err=%v", test.valid, err)
			}
			if !test.valid {
				return
			}
			for i, want := range test.passwords {
				if cfg.Instances[i].password != want {
					t.Fatalf("studio %d password inheritance failed", i)
				}
			}
		})
	}
}

func TestYAMLFileExtensions(t *testing.T) {
	body := []byte("instances:\n  - name: studio\n    urls: [ws://localhost:4455]\n")
	for _, ext := range []string{".yml", ".yaml", ".YAML", ".json", ".txt", ""} {
		t.Run(ext, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "config"+ext)
			if err := os.WriteFile(path, body, 0600); err != nil {
				t.Fatal(err)
			}
			_, err := loadConfig(path)
			valid := strings.EqualFold(ext, ".yml") || strings.EqualFold(ext, ".yaml")
			if (err == nil) != valid {
				t.Fatalf("extension %q: %v", ext, err)
			}
		})
	}
}

func TestMalformedResponseDoesNotPublishMetrics(t *testing.T) {
	for _, method := range []string{"GetStats", "GetStreamStatus", "GetRecordStatus"} {
		for _, payload := range []string{"null", "{}", `{"outputActive":null}`} {
			t.Run(method+"/"+payload, func(t *testing.T) {
				endpoint, _ := mockOBS(t, "", func(request string, n int64) (any, bool) {
					if request == method {
						return json.RawMessage(payload), true
					}
					return sampleResponse(request), true
				})
				target := Instance{Name: "studio", URLs: []string{endpoint}}
				cfg := Config{poll: time.Second, timeout: time.Second, reconnect: time.Second}
				c := newCollector([]Instance{target})
				state := c.targets[0]
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan struct{})
				go func() { defer close(done); state.run(ctx, cfg, target) }()
				defer func() { cancel(); <-done }()
				awaitState(t, func() bool { state.mu.RLock(); defer state.mu.RUnlock(); return state.failures > 0 })
				body := scrape(t, c)
				if !strings.Contains(body, `obs_up{obs_instance="studio"} 0`) || strings.Contains(body, "obs_global_active_fps{") {
					t.Fatal("invalid response published metrics")
				}
			})
		}
	}
}

func TestExampleConfiguration(t *testing.T) {
	t.Setenv("OBS_PASSWORD", "example-test-password")
	cfg, err := loadConfig("config.example.yml")
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Instances) != 2 {
		t.Fatal("example should configure two studios")
	}
	for _, instance := range cfg.Instances {
		if len(instance.endpoints()) != 2 || instance.password != "example-test-password" {
			t.Fatal("example must configure address failover and the shared password")
		}
	}
}
