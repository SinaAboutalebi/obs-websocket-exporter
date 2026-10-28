FROM --platform=$BUILDPLATFORM golang:1.27-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /exporter .

FROM alpine:3.24
RUN apk add --no-cache ca-certificates \
    && addgroup -S -g 10001 exporter \
    && adduser -S -D -H -u 10001 -G exporter exporter
COPY --from=build /exporter /obs-websocket-exporter
USER 10001:10001
EXPOSE 9407
ENTRYPOINT ["/obs-websocket-exporter"]
CMD ["-config", "/config/config.yml"]
