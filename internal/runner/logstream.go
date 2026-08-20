package runner

import (
	"bytes"
	"context"
	"sync"
	"time"
)

// logStreamer buffers container output and ships it to the control plane at a
// fixed interval. Batching keeps a chatty build from turning into thousands of
// HTTP requests, while the ticker keeps logs close to live.
type logStreamer struct {
	send func(seq int, chunk []byte)

	mu  sync.Mutex // guards buf, so a build writing output is never blocked by HTTP
	buf bytes.Buffer

	// sendMu serialises delivery: chunk N must leave before chunk N+1, or the
	// sequence numbers the server dedupes on would not match the real order.
	sendMu sync.Mutex
	seq    int

	done chan struct{}
	once sync.Once
}

func newLogStreamer(ctx context.Context, interval time.Duration, send func(seq int, chunk []byte)) *logStreamer {
	s := &logStreamer{send: send, done: make(chan struct{})}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.flush()
			case <-ctx.Done():
				return
			case <-s.done:
				return
			}
		}
	}()
	return s
}

// Write satisfies io.Writer so the executor can stream straight into it.
func (s *logStreamer) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.buf.Write(p)
}

func (s *logStreamer) flush() {
	s.sendMu.Lock()
	defer s.sendMu.Unlock()

	s.mu.Lock()
	if s.buf.Len() == 0 {
		s.mu.Unlock()
		return
	}
	chunk := make([]byte, s.buf.Len())
	copy(chunk, s.buf.Bytes())
	s.buf.Reset()
	s.mu.Unlock()

	s.seq++
	s.send(s.seq, chunk)
}

// Close flushes whatever is left, so the tail of a build is never lost.
func (s *logStreamer) Close() {
	s.once.Do(func() {
		close(s.done)
		s.flush()
	})
}
