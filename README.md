# OBS WebSocket Exporter

A standalone Go Prometheus exporter for one or more OBS Studio instances over a LAN. Runs on a VM, Linux host, macOS, or Windows. No exporter plugin or OBS libraries are required on the exporter machine.

```text
OBS studio-1 :4455 ─┐
OBS studio-2 :4455 ─┼── Go exporter :9407 ← Prometheus ← Grafana
OBS studio-3 :4455 ─┘
```

## Quick start

Requires Go 1.23 or later (Docker build uses Go 1.27) and OBS Studio 28+ with OBS WebSocket v5 enabled.

1. On each OBS machine, open **Tools → WebSocket Server Settings**, enable the server, and set a password. Default port: `4455`. Allow the VM's address through the OBS machine's firewall.
2. Build and configure:

```sh
go build -o obs-websocket-exporter .
cp config.example.yml config.yml
```

Edit `config.yml` to contain your actual OBS addresses and stable instance names. Delete example instances you don't use. Set the shared OBS password once:

```sh
export OBS_PASSWORD='your-shared-obs-password'
./obs-websocket-exporter -config config.yml
```

Open <http://localhost:9407/metrics>. `/healthz` reports whether the HTTP service is alive; individual OBS availability is reported by `obs_up`.

Configuration uses YAML files with a `.yml` or `.yaml` extension. The default is `config.yml`. Use `urls` for a list of alternative addresses, or `url` as a shorthand for a single address; do not combine them.

The exporter reads configuration once at startup. Restart it after configuration or password changes. `.env` files are **not** loaded automatically. `config.yml`, `config.yaml`, and `.env` are ignored by Git and Docker builds.

## Configuration

See [config.example.yml](config.example.yml). Defaults:

| Field | Default | Meaning |
|---|---|---|
| `listen` | `:9407` | HTTP listen address |
| `poll_interval` | `5s` | Wait between complete polls, separately for each instance |
| `request_timeout` | `3s` | Deadline per request and each connection handshake stage |
| `reconnect_interval` | `5s` | Wait after connection or polling failure |
| `instances[].name` | required | Unique, stable `obs_instance` label |
| `instances[].urls` | required | Ordered list of alternative `ws://` or `wss://` addresses for the same studio |
| `password_env` | optional | Environment variable holding the shared password for all studios |
| `instances[].password_env` | optional | Override the shared password environment variable for this studio |

Set top-level `password_env: OBS_PASSWORD` to use the same password for every studio. A studio can override it with its own `password_env`; otherwise it inherits the shared setting. The selected environment variable must exist and be nonempty. If neither setting is present, the studio is treated as having authentication disabled. Passwords in URLs, URL queries, unknown config fields, and duplicate instance names are rejected. TLS certificate verification remains enabled for `wss://` endpoints.

Use a trusted LAN or VPN. Password authentication does not encrypt plain `ws://` traffic. OBS credentials allow remote control even though this exporter only issues read requests. Restrict WebSocket access to the VM and restrict exporter HTTP access to your monitoring network. The HTTP endpoint does not provide authentication or TLS; use a reverse proxy when needed.

## Collection behavior

- One persistent, authenticated connection and independent worker per OBS instance. Addresses are tried in configured order on startup. After a failure, the next alternative is tried immediately; after a full failed round, the exporter waits `reconnect_interval`. A healthy connection stays on its current address; it does not switch back merely because the first address returns. All addresses for a studio share the same password and must refer to the same logical OBS instance. Metrics retain the same `obs_instance` label across address changes. The connected address and failures are logged. A brief failed-poll window sets `obs_up = 0` until the alternative completes a successful poll.
- Polls `GetStats`, `GetStreamStatus`, and `GetRecordStatus` sequentially. Publishes the snapshot only when all three succeed and contain the required measurements. Incomplete or malformed responses fail the poll instead of producing misleading zero values.
- Keeps the latest successful snapshot between polls. On any connection or request failure, reports `obs_up = 0`, omits OBS data metrics, and reconnects. Retains the last successful poll timestamp for diagnostics.
- A broken idle connection is detected at the next poll, plus the request timeout. This is polling health, not an instantaneous network probe.
- A failed or slow instance does not block HTTP scrapes or other OBS workers.
- Counters are raw OBS counters: they can reset when OBS, an output session, or a relevant OBS statistics session restarts. Use Prometheus `rate()`/`increase()`.
- Requests in a poll occur at slightly different times; this is not an atomic OBS-side snapshot.
- Graceful shutdown on SIGINT/SIGTERM interrupts pending WebSocket operations.

## Metrics

Every `obs_*` metric carries an `obs_instance` label.

| Metric | Type | Meaning |
|---|---|---|
| `obs_up` | gauge | Latest connection/poll attempt succeeded (1/0) |
| `obs_last_successful_poll_timestamp_seconds` | gauge | Last complete successful poll; zero before first success |
| `obs_collection_errors_total` | counter | Failed connection or poll attempts |
| `obs_global_active_fps` | gauge | Rendered FPS |
| `obs_global_average_frame_time_seconds` | gauge | Average render time |
| `obs_global_cpu_usage_percent` | gauge | OBS CPU usage |
| `obs_global_memory_usage_megabytes` | gauge | OBS memory usage, MB reported by OBS |
| `obs_global_recording_disk_available_megabytes` | gauge | Free recording storage, MB reported by OBS |
| `obs_global_render_frames_total` | counter | Render thread frames |
| `obs_global_render_skipped_frames_total` | counter | Frames skipped by rendering |
| `obs_global_output_frames_total` | counter | Output thread frames |
| `obs_global_output_skipped_frames_total` | counter | Frames skipped by output/encoding |
| `obs_stream_active` | gauge | Streaming active (1/0) |
| `obs_stream_reconnecting` | gauge | Streaming reconnecting (1/0) |
| `obs_stream_congestion` | gauge | Congestion value reported by OBS |
| `obs_stream_duration_seconds` | gauge | Current stream duration |
| `obs_stream_bytes_total` | counter | Bytes sent by stream output |
| `obs_stream_frames_total` | counter | Stream output frames |
| `obs_stream_skipped_frames_total` | counter | Skipped frames reported by stream output |
| `obs_record_active` | gauge | Recording active (1/0) |
| `obs_record_paused` | gauge | Recording paused (1/0) |
| `obs_record_duration_seconds` | gauge | Current recording duration |
| `obs_record_bytes_total` | counter | Bytes written by recording output |

The stream skipped-frame field is exported under its WebSocket name; do not assume it is interchangeable with a metric from another exporter without checking its source semantics.

`go_*` and `process_*` describe this exporter process on the VM. OBS CPU and memory come from the `obs_global_*` metrics.

### Differences from the native OBS plugin

This is a separate exporter with its own metric names; existing dashboards need updates. It does not collect live encoder enumeration/codec/sample rates, exact dimensions of every output/encoder, output connection setup time, or audio levels. Audio metering is a possible follow-up via `InputVolumeMeters`; it is not implemented in this first version. Only the main stream and recording output are polled, not arbitrary plugin-created outputs. Global video settings and extra outputs could be added through additional requests.

## Prometheus

```yaml
scrape_configs:
  - job_name: obs
    scrape_interval: 15s
    static_configs:
      - targets: ['EXPORTER_VM_IP:9407']
```

Examples:

```promql
# Unreachable OBS instance (allow transient failures in alert rule with for: 30s)
obs_up == 0

# Stream bitrate in bits per second
8 * rate(obs_stream_bytes_total[1m])

# Percentage of render frames skipped
100 * rate(obs_global_render_skipped_frames_total[5m])
  / clamp_min(rate(obs_global_render_frames_total[5m]), 1)

# Encoding/output skipped frames per second
rate(obs_global_output_skipped_frames_total[1m])
```

Also alert on Prometheus `up{job="obs"} == 0` to detect an unavailable exporter.

## Linux VM / systemd

Build a binary for the VM architecture, for example:

```sh
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -trimpath -o obs-websocket-exporter .
```

Install the binary as `/usr/local/bin/obs-websocket-exporter`, configuration as `/etc/obs-websocket-exporter/config.yml`, and a password environment file as `/etc/obs-websocket-exporter/passwords` (root-owned, mode `0600`):

```text
OBS_PASSWORD=your-shared-obs-password
```

Example service unit:

```ini
[Unit]
Description=OBS WebSocket Prometheus Exporter
Wants=network-online.target
After=network-online.target

[Service]
DynamicUser=yes
EnvironmentFile=/etc/obs-websocket-exporter/passwords
ExecStart=/usr/local/bin/obs-websocket-exporter -config /etc/obs-websocket-exporter/config.yml
Restart=on-failure
RestartSec=5s
NoNewPrivileges=yes
ProtectSystem=strict
ProtectHome=yes
PrivateTmp=yes

[Install]
WantedBy=multi-user.target
```

The service's dynamic user must be able to read the YAML configuration. Keep passwords in the separate environment file, which systemd reads before dropping privileges.

## Docker

```sh
docker build -t obs-websocket-exporter .
docker run --rm --name obs-websocket-exporter \
  -p 9407:9407 \
  --mount type=bind,src="$(pwd)/config.yml",dst=/config/config.yml,readonly \
  -e OBS_PASSWORD \
  obs-websocket-exporter
```

The runtime image uses Alpine 3.24 with CA certificates and runs as UID/GID `10001:10001`. Make the mounted YAML configuration readable by that user; keep passwords in environment variables. CI publishes a single Linux amd64 image.

The container must have a route to the OBS machines. Inside a container, `localhost` refers to that container. Export the password variable in your shell before using `-e NAME`.

## CI/CD

GitHub Actions builds and publishes the Docker image to GHCR on pushes to `main` and `v*` tags:

- Push to `main`: `ghcr.io/sinaaboutalebi/obs-websocket-exporter:latest`.
- Push a version tag, such as `v1.0.0`: `ghcr.io/sinaaboutalebi/obs-websocket-exporter:v1.0.0`.

The workflow uses the built-in `GITHUB_TOKEN` and builds one Linux amd64 image. Repository/organization policy must allow Actions to write packages. Package visibility is managed in GHCR settings.

To run the published image, replace the local image name in the Docker command above with `ghcr.io/sinaaboutalebi/obs-websocket-exporter:latest` or a version tag. Deployment to your VM is separate.

## Development

```sh
go test -race ./...
go vet ./...
go build ./...
```

Tests use a simulated OBS WebSocket server and cover authentication, polling and unit conversions, reconnection, stale metric removal, instance isolation, shutdown, configuration validation, YAML parsing, and address failover. Real OBS interoperability requires testing against your OBS hosts.

Protocol reference: <https://github.com/obsproject/obs-websocket/blob/master/docs/generated/protocol.md>
