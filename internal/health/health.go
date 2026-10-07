package health

import (
	"bufio"
	"context"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Check is one dependency result in /readyz.
type Check struct {
	Reachable bool   `json:"reachable"`
	Host      string `json:"host,omitempty"`
	Detail    string `json:"detail,omitempty"`
}

type Report struct {
	Status string           `json:"status"`
	Checks map[string]Check `json:"checks"`
}

// TCP dial helper used for Postgres/Redis reachability (TASK-0 level).
func dialHost(hostport string, timeout time.Duration, useTLS bool) error {
	d := &net.Dialer{Timeout: timeout}
	if useTLS {
		_, err := tls.DialWithDialer(d, "tcp", hostport, &tls.Config{MinVersion: tls.VersionTLS12})
		return err
	}
	conn, err := d.Dial("tcp", hostport)
	if err != nil {
		return err
	}
	conn.Close()
	return nil
}

func hostportFromURL(raw string) (hostport string, password string, useTLS bool, err error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", "", false, err
	}
	hostport = u.Host
	if !strings.Contains(hostport, ":") {
		if u.Scheme == "postgres" || u.Scheme == "postgresql" {
			hostport += ":5432"
		} else {
			hostport += ":6379"
		}
	}
	if u.User != nil {
		password, _ = u.User.Password()
		if password == "" {
			// redis://:pass@host style puts empty username
			password = u.User.Username()
		}
	}
	useTLS = u.Scheme == "rediss" || u.Scheme == "postgres" && strings.Contains(raw, "sslmode=require")
	return hostport, password, useTLS, nil
}

// PostgresCheck: TCP reachability of the pg host. Full SQL ping comes in TASK-1 (pgx).
func PostgresCheck(databaseURL string) Check {
	if databaseURL == "" {
		return Check{Reachable: false, Detail: "DATABASE_URL not set"}
	}
	hostport, _, _, err := hostportFromURL(databaseURL)
	if err != nil {
		return Check{Reachable: false, Detail: "bad DATABASE_URL"}
	}
	// Aiven requires TLS; plain TCP dial still proves routing/firewall.
	// Try TLS first, fall back to plain TCP so we report accurately.
	if err := dialHost(hostport, 4*time.Second, true); err == nil {
		return Check{Reachable: true, Host: hostport, Detail: "tcp+tls open"}
	}
	if err := dialHost(hostport, 4*time.Second, false); err != nil {
		return Check{Reachable: false, Host: hostport, Detail: "dial failed"}
	}
	return Check{Reachable: true, Host: hostport, Detail: "tcp open (tls handshake failed, host reachable)"}
}

// RedisCheck: TCP + AUTH + PING using raw RESP (no client dep in TASK-0).
func RedisCheck(redisURL string) Check {
	if redisURL == "" {
		return Check{Reachable: false, Detail: "REDIS_URL not set"}
	}
	hostport, password, useTLS, err := hostportFromURL(redisURL)
	if err != nil {
		return Check{Reachable: false, Detail: "bad REDIS_URL"}
	}
	d := &net.Dialer{Timeout: 4 * time.Second}
	var conn net.Conn
	if useTLS {
		conn, err = tls.DialWithDialer(d, "tcp", hostport, &tls.Config{MinVersion: tls.VersionTLS12})
	} else {
		conn, err = d.Dial("tcp", hostport)
	}
	if err != nil {
		return Check{Reachable: false, Host: hostport, Detail: "dial failed"}
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(4 * time.Second))
	rw := bufio.NewReader(conn)
	write := func(s string) error {
		_, err := fmt.Fprint(conn, s)
		return err
	}
	readLine := func() (string, error) {
		l, err := rw.ReadString('\n')
		return strings.TrimSpace(l), err
	}
	if password != "" {
		// AUTH <password>
		msg := fmt.Sprintf("AUTH %s\r\n", password)
		// Send as inline command (Upstash accepts inline AUTH).
		if err := write(msg); err != nil {
			return Check{Reachable: false, Host: hostport, Detail: "auth write failed"}
		}
		line, err := readLine()
		if err != nil || !strings.HasPrefix(line, "+OK") {
			return Check{Reachable: false, Host: hostport, Detail: "auth rejected: " + line}
		}
	}
	if err := write("PING\r\n"); err != nil {
		return Check{Reachable: false, Host: hostport, Detail: "ping write failed"}
	}
	line, err := readLine()
	if err != nil {
		return Check{Reachable: false, Host: hostport, Detail: "ping read failed"}
	}
	if strings.Contains(line, "PONG") {
		return Check{Reachable: true, Host: hostport, Detail: "PONG"}
	}
	return Check{Reachable: false, Host: hostport, Detail: "unexpected: " + line}
}

// R2Check: HTTP reachability of the public URL / endpoint. Any HTTP response = reachable.
func R2Check(publicURL, endpointURL, bucket string) Check {
	target := publicURL
	if target == "" {
		target = endpointURL
	}
	if target == "" {
		return Check{Reachable: false, Detail: "S3_PUBLIC_URL / S3_ENDPOINT_URL not set"}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 6*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, "HEAD", target, nil)
	if err != nil {
		return Check{Reachable: false, Detail: "bad url"}
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		// HEAD sometimes blocked; try GET with range-less short body
		req2, _ := http.NewRequestWithContext(ctx, "GET", target, nil)
		resp, err = http.DefaultClient.Do(req2)
		if err != nil {
			return Check{Reachable: false, Detail: "unreachable: " + err.Error()}
		}
	}
	defer resp.Body.Close()
	return Check{Reachable: true, Host: bucket, Detail: fmt.Sprintf("http %d", resp.StatusCode)}
}
