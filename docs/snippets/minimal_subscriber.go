// Minimal waitbus broadcast subscriber (Go, stdlib only).
//
// Mirror of minimal_subscriber.py; wire contract documented there.
//
// Run:
//     go run minimal_subscriber.go
package main

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
)

const maxFrameBytes = 65536

func defaultSocketPath() string {
	if override := os.Getenv("WAITBUS_BROADCAST_SOCKET"); override != "" {
		return override
	}
	if runtime.GOOS == "darwin" {
		home, err := os.UserHomeDir()
		if err != nil {
			home = "/"
		}
		return filepath.Join(home, "Library", "Application Support", "waitbus", "broadcast.sock")
	}
	runtimeDir := os.Getenv("XDG_RUNTIME_DIR")
	if runtimeDir == "" {
		runtimeDir = "/run/user/" + strconv.Itoa(os.Getuid())
	}
	return filepath.Join(runtimeDir, "waitbus", "broadcast.sock")
}

func recvExactly(conn net.Conn, n int) ([]byte, error) {
	buf := make([]byte, n)
	read := 0
	for read < n {
		got, err := conn.Read(buf[read:])
		if got > 0 {
			read += got
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				if read == 0 {
					return nil, io.EOF
				}
				return nil, fmt.Errorf("short read: expected %d bytes, got %d", n, read)
			}
			return nil, err
		}
	}
	return buf, nil
}

func readFrame(conn net.Conn) ([]byte, error) {
	prefix, err := recvExactly(conn, 4)
	if err != nil {
		return nil, err
	}
	length := binary.BigEndian.Uint32(prefix)
	if length == 0 || length > maxFrameBytes {
		return nil, fmt.Errorf("frame length %d out of bounds", length)
	}
	payload, err := recvExactly(conn, int(length))
	if err != nil {
		if errors.Is(err, io.EOF) {
			return nil, errors.New("EOF inside frame payload")
		}
		return nil, err
	}
	return payload, nil
}

func writeFrame(conn net.Conn, payload []byte) error {
	if len(payload) > maxFrameBytes {
		return fmt.Errorf("payload %d bytes exceeds %d", len(payload), maxFrameBytes)
	}
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(payload)))
	if _, err := conn.Write(prefix[:]); err != nil {
		return err
	}
	if _, err := conn.Write(payload); err != nil {
		return err
	}
	return nil
}

func run() int {
	socketPath := defaultSocketPath()
	conn, err := net.Dial("unix", socketPath)
	if err != nil {
		fmt.Fprintf(os.Stderr,
			"error: broadcast socket %s unavailable (%v). "+
				"Start the daemon via `systemctl --user start waitbus-broadcast.service`.\n",
			socketPath, err)
		return 2
	}
	defer conn.Close()

	// Subscribe envelope: proto=1 is mandatory. Empty filters means "all
	// repos, all event types, from now". Add "filters" or "event_types"
	// keys to narrow.
	if err := writeFrame(conn, []byte(`{"proto":1}`)); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		return 1
	}

	writer := os.Stdout
	for {
		frame, err := readFrame(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				return 0
			}
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			return 1
		}

		var event map[string]any
		if err := json.Unmarshal(frame, &event); err != nil {
			fmt.Fprintf(os.Stderr, "error: invalid JSON frame: %v\n", err)
			return 1
		}

		if kind, _ := event["kind"].(string); kind == "subscribe_rejected" {
			reason, _ := event["reason"].(string)
			if reason == "" {
				reason = "unknown"
			}
			fmt.Fprintf(os.Stderr, "error: subscribe_rejected: %s\n", reason)
			if remediation, _ := event["remediation"].(string); remediation != "" {
				fmt.Fprintf(os.Stderr, "remediation: %s\n", remediation)
			}
			return 2
		}

		// A "truncated" frame is a DATA frame (it carries an event_id and
		// advances the resume cursor), not a control frame: the event's
		// payload exceeded the wire cap, so only its identity rides the
		// socket. Surface it -- silently dropping it makes a large event
		// invisible -- and re-fetch the full row out of band.
		if kind, _ := event["kind"].(string); kind == "truncated" {
			fmt.Printf("%s\t[truncated; re-fetch full payload via `waitbus read-events`]\n", stringOr(event["event_id"], "?"))
			continue
		}
		// Control frames (daemon_heartbeat, subscribe_ack) carry no event
		// identity; skip them.
		if kind, _ := event["kind"].(string); kind != "event" {
			continue
		}

		deliveryID := stringOr(event["delivery_id"], "?")
		eventType := stringOr(event["event_type"], "?")
		source := "?"
		if fields, ok := event["fields"].(map[string]any); ok {
			source = stringOr(fields["source"], "?")
		}

		fmt.Fprintf(writer, "%s\tsource=%s\ttype=%s\n", deliveryID, source, eventType)
	}
}

func stringOr(v any, fallback string) string {
	if s, ok := v.(string); ok {
		return s
	}
	return fallback
}

func main() {
	os.Exit(run())
}
