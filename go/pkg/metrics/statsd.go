package metrics

import (
	"fmt"
	"net"
	"strings"
	"sync"
)

// statsDClient is a minimal DogStatsD-compatible UDP client (increment + timing).
type statsDClient struct {
	mu   sync.Mutex
	conn net.Conn
}

func newStatsDClient(addr string) (*statsDClient, error) {
	if addr == "" {
		return nil, fmt.Errorf("statsd addr required")
	}
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	return &statsDClient{conn: conn}, nil
}

func (c *statsDClient) increment(name string, tags []string) error {
	return c.send(fmt.Sprintf("%s:1|c%s", name, tagSuffix(tags)))
}

func (c *statsDClient) timing(name string, ms float64, tags []string) error {
	return c.send(fmt.Sprintf("%s:%d|ms%s", name, int(ms), tagSuffix(tags)))
}

func (c *statsDClient) send(line string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.conn == nil {
		return fmt.Errorf("statsd not connected")
	}
	_, err := c.conn.Write([]byte(line))
	return err
}

func tagSuffix(tags []string) string {
	if len(tags) == 0 {
		return ""
	}
	return "|#" + strings.Join(tags, ",")
}
