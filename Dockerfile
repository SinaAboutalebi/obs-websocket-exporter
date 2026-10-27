FROM golang:1.27-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /exporter .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /exporter /obs-websocket-exporter
EXPOSE 9407
ENTRYPOINT ["/obs-websocket-exporter"]
CMD ["-config", "/config/config.yml"]
