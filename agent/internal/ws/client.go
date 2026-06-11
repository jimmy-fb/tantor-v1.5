// Package ws is the agent's WebSocket client. One Client instance owns the
// connection lifecycle: dial, handshake, heartbeat ticker, command receive
// loop, reconnect with exponential backoff. The Dispatcher (passed in)
// handles the actual op execution.
package ws

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/rand/v2"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/coder/websocket"
	"github.com/google/uuid"

	"github.com/jimmy-fb/tantor/agent/internal/config"
	"github.com/jimmy-fb/tantor/agent/internal/ops"
	"github.com/jimmy-fb/tantor/agent/internal/proto"
	"github.com/jimmy-fb/tantor/agent/internal/sysinfo"
)

// Version is the agent binary version, baked in via ldflags.
var Version = "1.5.0-dev"

// Client owns one WebSocket connection (and reconnects when it drops).
type Client struct {
	cfg        *config.Config
	dispatcher *ops.Dispatcher
	logger     *log.Logger
	startTime  time.Time

	// jwtPath is the persisted-credential location: cfg.StateDir/agent.jwt.
	jwtPath string

	// ackMu guards heartbeatInterval (the SCM may override the local
	// default during register_ack).
	ackMu             sync.Mutex
	heartbeatInterval time.Duration
}

// NewClient assembles a Client from the config + dispatcher. The
// dispatcher is consulted when cmd frames arrive.
func NewClient(cfg *config.Config, dispatcher *ops.Dispatcher, logger *log.Logger) *Client {
	if logger == nil {
		logger = log.Default()
	}
	return &Client{
		cfg:               cfg,
		dispatcher:        dispatcher,
		logger:            logger,
		startTime:         time.Now(),
		jwtPath:           filepath.Join(cfg.StateDirOrDefault(), "agent.jwt"),
		heartbeatInterval: time.Duration(cfg.HeartbeatInterval()) * time.Second,
	}
}

// Run dials the SCM, handles frames, and reconnects forever until ctx
// cancels. It never returns an error — connection failures are logged and
// retried; ctx cancellation is the only exit.
func (c *Client) Run(ctx context.Context) {
	delay := time.Second
	const delayCap = 30 * time.Second

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		err := c.connectOnce(ctx)
		if err != nil {
			c.logger.Printf("session ended: %v", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(jitter(delay)):
		}
		if delay < delayCap {
			delay *= 2
			if delay > delayCap {
				delay = delayCap
			}
		}
		// On a successful connection that later dropped, reset the
		// backoff so the next reconnect tries quickly.
		if errors.Is(err, errCleanShutdown) {
			delay = time.Second
		}
	}
}

// jitter applies ±20% jitter to a base delay so a fleet of agents doesn't
// re-dial in lockstep after an SCM outage.
func jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return d
	}
	f := 0.8 + rand.Float64()*0.4
	return time.Duration(float64(d) * f)
}

var errCleanShutdown = errors.New("clean shutdown")

// tlsConfig builds the TLS config the WebSocket dialer uses. Returns nil
// when InsecureSkipVerify is enough (default behavior of the net/http
// transport gives us system trust store; we only override when ca_bundle_path
// is set or tls_verify is false).
func (c *Client) tlsConfig() (*tls.Config, error) {
	tc := &tls.Config{InsecureSkipVerify: !c.cfg.TLSVerifyEnabled()}

	if c.cfg.CABundlePath != "" {
		pem, err := os.ReadFile(c.cfg.CABundlePath)
		if err != nil {
			return nil, fmt.Errorf("read CA bundle %s: %w", c.cfg.CABundlePath, err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("CA bundle %s contains no valid certs", c.cfg.CABundlePath)
		}
		tc.RootCAs = pool
	}
	return tc, nil
}

// connectOnce dials, registers, and runs the receive loop until the
// connection closes or ctx cancels. Heartbeats are sent on a goroutine.
func (c *Client) connectOnce(parent context.Context) error {
	tc, err := c.tlsConfig()
	if err != nil {
		return err
	}

	httpClient := &http.Client{Transport: &http.Transport{TLSClientConfig: tc}}

	auth := c.bearerToken()
	if auth == "" {
		return errors.New("no registration_token in config and no persisted agent.jwt — cannot authenticate")
	}

	header := http.Header{}
	header.Set("Authorization", "Bearer "+auth)
	header.Set("X-Tantor-Agent-Version", Version)

	c.logger.Printf("dialing %s", c.cfg.SCMUrl)
	conn, resp, err := websocket.Dial(parent, c.cfg.SCMUrl, &websocket.DialOptions{
		HTTPClient: httpClient,
		HTTPHeader: header,
	})
	if err != nil {
		if resp != nil {
			return fmt.Errorf("dial: %w (http %d)", err, resp.StatusCode)
		}
		return fmt.Errorf("dial: %w", err)
	}
	// Generous read limit so kafka-*.sh stdout doesn't get truncated mid-list.
	conn.SetReadLimit(1 << 22) // 4 MiB
	defer func() { _ = conn.Close(websocket.StatusNormalClosure, "shutdown") }()

	ctx, cancel := context.WithCancel(parent)
	defer cancel()

	if err := c.register(ctx, conn); err != nil {
		return fmt.Errorf("register: %w", err)
	}

	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go c.heartbeatLoop(hbCtx, conn)

	return c.receiveLoop(ctx, conn)
}

// bearerToken returns the persisted agent JWT if present, else the
// registration token from the config.
func (c *Client) bearerToken() string {
	if data, err := os.ReadFile(c.jwtPath); err == nil && len(data) > 0 {
		return string(data)
	}
	return c.cfg.RegistrationToken
}

// register sends the first frame, waits for register_ack, persists the
// long-lived JWT, and applies the SCM-supplied heartbeat interval.
func (c *Client) register(ctx context.Context, conn *websocket.Conn) error {
	osi := sysinfo.Detect()
	features := c.cfg.AgentFeatures
	if len(features) == 0 {
		// Derived from allowlist when operator didn't override.
		features = c.dispatcher.Features()
	}

	regPayload := proto.RegisterPayload{
		Hostname:      sysinfo.Hostname(),
		IPAddresses:   sysinfo.LocalIPs(),
		OS:            proto.OSInfo{Family: osi.Family, Version: osi.Version, Kernel: osi.Kernel},
		AgentVersion:  Version,
		AgentFeatures: features,
	}
	if err := c.sendFrame(ctx, conn, proto.Envelope{
		V:       proto.ProtocolVersion,
		Kind:    proto.KindRegister,
		ID:      uuid.NewString(),
		Ts:      time.Now().UTC().Format(time.RFC3339),
		Payload: mustMarshal(regPayload),
	}); err != nil {
		return err
	}

	// Wait for register_ack (or error).
	regCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	_, data, err := conn.Read(regCtx)
	if err != nil {
		return fmt.Errorf("await ack: %w", err)
	}
	var env proto.Envelope
	if err := json.Unmarshal(data, &env); err != nil {
		return fmt.Errorf("parse ack: %w", err)
	}
	switch env.Kind {
	case proto.KindRegisterAck:
		var ack proto.RegisterAckPayload
		if err := json.Unmarshal(env.Payload, &ack); err != nil {
			return fmt.Errorf("parse ack payload: %w", err)
		}
		if ack.AgentJWT != "" {
			c.persistJWT(ack.AgentJWT)
		}
		if ack.HeartbeatIntervalSec > 0 {
			c.ackMu.Lock()
			c.heartbeatInterval = time.Duration(ack.HeartbeatIntervalSec) * time.Second
			c.ackMu.Unlock()
		}
		c.logger.Printf("registered: agent_id=%s host_id=%s features=%v",
			ack.AgentID, ack.HostID, features)
		return nil
	case proto.KindError:
		var ep proto.ErrorPayload
		_ = json.Unmarshal(env.Payload, &ep)
		return fmt.Errorf("register rejected: %s — %s", ep.Code, ep.Message)
	default:
		return fmt.Errorf("unexpected frame kind during registration: %s", env.Kind)
	}
}

// persistJWT writes the long-lived agent JWT to disk (mode 600).
func (c *Client) persistJWT(token string) {
	_ = os.MkdirAll(c.cfg.StateDirOrDefault(), 0o700)
	tmp := c.jwtPath + ".tmp"
	if err := os.WriteFile(tmp, []byte(token), 0o600); err != nil {
		c.logger.Printf("could not persist agent.jwt: %v", err)
		return
	}
	if err := os.Rename(tmp, c.jwtPath); err != nil {
		c.logger.Printf("could not atomically install agent.jwt: %v", err)
	}
}

// heartbeatLoop ticks every heartbeatInterval and sends a heartbeat frame.
func (c *Client) heartbeatLoop(ctx context.Context, conn *websocket.Conn) {
	for {
		c.ackMu.Lock()
		interval := c.heartbeatInterval
		c.ackMu.Unlock()
		if interval <= 0 {
			interval = 15 * time.Second
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}

		hb := proto.HeartbeatPayload{
			UptimeSec: int64(time.Since(c.startTime).Seconds()),
		}
		if u := sysinfo.UptimeSec(); u > 0 {
			hb.UptimeSec = u
		}

		_ = c.sendFrame(ctx, conn, proto.Envelope{
			V:       proto.ProtocolVersion,
			Kind:    proto.KindHeartbeat,
			ID:      uuid.NewString(),
			Ts:      time.Now().UTC().Format(time.RFC3339),
			Payload: mustMarshal(hb),
		})
	}
}

// receiveLoop reads frames until the connection closes. Each cmd frame
// gets handed to the dispatcher in a goroutine so a slow op can't block
// other commands or the heartbeat ticker.
func (c *Client) receiveLoop(ctx context.Context, conn *websocket.Conn) error {
	for {
		_, data, err := conn.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return errCleanShutdown
			}
			return fmt.Errorf("read: %w", err)
		}
		var env proto.Envelope
		if err := json.Unmarshal(data, &env); err != nil {
			c.logger.Printf("dropped malformed frame: %v", err)
			continue
		}
		switch env.Kind {
		case proto.KindCmd:
			go c.handleCmd(ctx, conn, env)
		case proto.KindError:
			var ep proto.ErrorPayload
			_ = json.Unmarshal(env.Payload, &ep)
			c.logger.Printf("SCM error: %s — %s", ep.Code, ep.Message)
		default:
			c.logger.Printf("dropped frame: unexpected kind %q", env.Kind)
		}
	}
}

func (c *Client) handleCmd(ctx context.Context, conn *websocket.Conn, env proto.Envelope) {
	var cmd proto.CmdPayload
	if err := json.Unmarshal(env.Payload, &cmd); err != nil {
		_ = c.sendFrame(ctx, conn, proto.Envelope{
			V: proto.ProtocolVersion, Kind: proto.KindError, Ref: env.ID,
			Ts:      time.Now().UTC().Format(time.RFC3339),
			Payload: mustMarshal(proto.ErrorPayload{Code: proto.ErrBadArgs, Message: err.Error()}),
		})
		return
	}

	res, errPayload := c.dispatcher.Run(ctx, &cmd)
	if errPayload != nil {
		c.logger.Printf("cmd %s op=%s -> %s: %s", env.ID, cmd.Op, errPayload.Code, errPayload.Message)
		_ = c.sendFrame(ctx, conn, proto.Envelope{
			V: proto.ProtocolVersion, Kind: proto.KindError, Ref: env.ID,
			Ts:      time.Now().UTC().Format(time.RFC3339),
			Payload: mustMarshal(errPayload),
		})
		return
	}
	c.logger.Printf("cmd %s op=%s -> exit=%d (%dms)", env.ID, cmd.Op, res.ExitCode, res.DurationMs)
	_ = c.sendFrame(ctx, conn, proto.Envelope{
		V: proto.ProtocolVersion, Kind: proto.KindResult, Ref: env.ID,
		Ts:      time.Now().UTC().Format(time.RFC3339),
		Payload: mustMarshal(res),
	})
}

// sendFrame marshals env and writes it as a text WS message with a small
// per-write deadline so a stalled connection can't block forever.
func (c *Client) sendFrame(ctx context.Context, conn *websocket.Conn, env proto.Envelope) error {
	data, err := json.Marshal(env)
	if err != nil {
		return err
	}
	wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	return conn.Write(wctx, websocket.MessageText, data)
}

func mustMarshal(v any) []byte {
	data, err := json.Marshal(v)
	if err != nil {
		// Should never happen — the structs are simple. Log + send empty.
		log.Printf("mustMarshal: %v", err)
		return []byte("null")
	}
	return data
}
