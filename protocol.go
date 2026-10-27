package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
)

type packet struct {
	Op int             `json:"op"`
	D  json.RawMessage `json:"d"`
}
type rpcClient struct {
	conn     *websocket.Conn
	timeout  time.Duration
	sequence int
}

func authResponse(password, salt, challenge string) string {
	secret := sha256.Sum256([]byte(password + salt))
	response := sha256.Sum256([]byte(base64.StdEncoding.EncodeToString(secret[:]) + challenge))
	return base64.StdEncoding.EncodeToString(response[:])
}
func connect(ctx context.Context, target Instance, timeout time.Duration) (*rpcClient, error) {
	dialer := websocket.Dialer{HandshakeTimeout: timeout, Proxy: http.ProxyFromEnvironment}
	conn, response, err := dialer.DialContext(ctx, target.URL, nil)
	if err != nil {
		if response != nil && response.Body != nil {
			response.Body.Close()
		}
		return nil, fmt.Errorf("WebSocket connection failed")
	}
	conn.SetReadLimit(1 << 20)
	c := &rpcClient{conn: conn, timeout: timeout}
	// Closing the socket also interrupts reads when the service shuts down.
	stop := context.AfterFunc(ctx, func() { conn.Close() })
	ok := false
	defer func() {
		stop()
		if !ok {
			conn.Close()
		}
	}()
	p, err := c.read()
	if err != nil || p.Op != 0 {
		return nil, fmt.Errorf("expected OBS Hello")
	}
	var hello struct {
		RPCVersion     int `json:"rpcVersion"`
		Authentication *struct {
			Salt      string `json:"salt"`
			Challenge string `json:"challenge"`
		} `json:"authentication"`
	}
	if json.Unmarshal(p.D, &hello) != nil || hello.RPCVersion < 1 {
		return nil, fmt.Errorf("unsupported OBS protocol")
	}
	identify := map[string]any{"rpcVersion": 1, "eventSubscriptions": 0}
	if hello.Authentication != nil {
		if target.password == "" {
			return nil, fmt.Errorf("OBS requires a password; configure password_env")
		}
		identify["authentication"] = authResponse(target.password, hello.Authentication.Salt, hello.Authentication.Challenge)
	}
	if err := c.write(1, identify); err != nil {
		return nil, fmt.Errorf("OBS identification failed")
	}
	p, err = c.read()
	if err != nil || p.Op != 2 {
		return nil, fmt.Errorf("OBS authentication or identification failed")
	}
	var identified struct {
		RPCVersion int `json:"negotiatedRpcVersion"`
	}
	if json.Unmarshal(p.D, &identified) != nil || identified.RPCVersion != 1 {
		return nil, fmt.Errorf("unsupported negotiated OBS protocol")
	}
	ok = true
	return c, nil
}
func (c *rpcClient) read() (packet, error) {
	var p packet
	if err := c.conn.SetReadDeadline(time.Now().Add(c.timeout)); err != nil {
		return p, err
	}
	err := c.conn.ReadJSON(&p)
	return p, err
}
func (c *rpcClient) write(op int, data any) error {
	if err := c.conn.SetWriteDeadline(time.Now().Add(c.timeout)); err != nil {
		return err
	}
	return c.conn.WriteJSON(struct {
		Op int `json:"op"`
		D  any `json:"d"`
	}{op, data})
}
func (c *rpcClient) call(method string, result any) error {
	deadline := time.Now().Add(c.timeout)
	c.sequence++
	id := fmt.Sprint(c.sequence)
	if err := c.write(6, map[string]any{"requestType": method, "requestId": id}); err != nil {
		return fmt.Errorf("%s write failed", method)
	}
	// A single absolute deadline bounds the entire response, including unsolicited messages.
	if err := c.conn.SetReadDeadline(deadline); err != nil {
		return err
	}
	for {
		var p packet
		if err := c.conn.ReadJSON(&p); err != nil {
			return fmt.Errorf("%s response unavailable", method)
		}
		if p.Op != 7 {
			continue
		}
		var r struct {
			RequestID string `json:"requestId"`
			Status    struct {
				Result bool `json:"result"`
				Code   int  `json:"code"`
			} `json:"requestStatus"`
			Data json.RawMessage `json:"responseData"`
		}
		if json.Unmarshal(p.D, &r) != nil {
			return fmt.Errorf("invalid OBS response")
		}
		if r.RequestID != id {
			continue
		}
		if !r.Status.Result {
			return fmt.Errorf("%s rejected (code %d)", method, r.Status.Code)
		}
		if err := validateResponseData(method, r.Data); err != nil {
			return err
		}
		if err := json.Unmarshal(r.Data, result); err != nil {
			return fmt.Errorf("invalid %s data", method)
		}
		return nil
	}
}

// A successful status must contain the measurements we export. Missing or null
// fields otherwise decode as zero, hiding failed or incompatible OBS responses.
func validateResponseData(method string, data json.RawMessage) error {
	var fields map[string]json.RawMessage
	if json.Unmarshal(data, &fields) != nil || fields == nil {
		return fmt.Errorf("invalid %s data", method)
	}
	var required []string
	switch method {
	case "GetStats":
		required = []string{"cpuUsage", "memoryUsage", "availableDiskSpace", "activeFps", "averageFrameRenderTime", "renderSkippedFrames", "renderTotalFrames", "outputSkippedFrames", "outputTotalFrames"}
	case "GetStreamStatus":
		required = []string{"outputActive", "outputReconnecting", "outputDuration", "outputCongestion", "outputBytes", "outputSkippedFrames", "outputTotalFrames"}
	case "GetRecordStatus":
		required = []string{"outputActive", "outputPaused", "outputDuration", "outputBytes"}
	}
	for _, name := range required {
		value, ok := fields[name]
		if !ok || bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return fmt.Errorf("incomplete %s data: missing %s", method, name)
		}
	}
	return nil
}
