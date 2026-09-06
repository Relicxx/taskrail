package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	DatabaseURL, HTTPAddr, WorkerID string
	Keys                            map[string]string
	Concurrency                     int
	Lease, Heartbeat, Poll, Grace   time.Duration
	Demo                            bool
}

func Load() (c Config, e error) {
	c.DatabaseURL = os.Getenv("DATABASE_URL")
	u, err := url.Parse(c.DatabaseURL)
	if err != nil || u.Host == "" || (u.Scheme != "postgres" && u.Scheme != "postgresql") {
		return c, fmt.Errorf("DATABASE_URL must be a PostgreSQL URL")
	}
	c.HTTPAddr = value("HTTP_ADDR", ":8080")
	c.WorkerID = value("WORKER_ID", "")
	if _, _, err = net.SplitHostPort(c.HTTPAddr); err != nil {
		return c, fmt.Errorf("invalid server address")
	}
	c.Keys = map[string]string{}
	for _, pair := range strings.Split(os.Getenv("API_KEYS"), ",") {
		parts := strings.SplitN(pair, ":", 2)
		if len(parts) != 2 || parts[0] == "" || parts[1] == "" {
			return c, fmt.Errorf("API_KEYS requires client:key entries")
		}
		if _, ok := c.Keys[parts[0]]; ok {
			return c, fmt.Errorf("duplicate API client")
		}
		for _, k := range c.Keys {
			if k == parts[1] {
				return c, fmt.Errorf("duplicate API key")
			}
		}
		c.Keys[parts[0]] = parts[1]
	}
	c.Concurrency, err = strconv.Atoi(value("CONCURRENCY", "4"))
	if err != nil || c.Concurrency < 1 || c.Concurrency > 256 {
		return c, fmt.Errorf("CONCURRENCY must be 1..256")
	}
	for _, p := range []struct {
		name, def string
		dst       *time.Duration
	}{{"LEASE", "30s", &c.Lease}, {"HEARTBEAT", "10s", &c.Heartbeat}, {"POLL", "500ms", &c.Poll}, {"SHUTDOWN_GRACE", "20s", &c.Grace}} {
		*p.dst, err = time.ParseDuration(value(p.name, p.def))
		if err != nil || *p.dst <= 0 {
			return c, fmt.Errorf("invalid %s", p.name)
		}
	}
	if c.Heartbeat >= c.Lease {
		return c, fmt.Errorf("HEARTBEAT must be less than LEASE")
	}
	c.Demo, err = strconv.ParseBool(value("DEMO_MODE", "false"))
	if err != nil {
		return c, fmt.Errorf("invalid DEMO_MODE")
	}
	return c, nil
}
func value(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}
