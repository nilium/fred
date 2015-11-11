package resv

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/nilium/fred"
)

type Handler interface {
	ServeRESP(ResponseWriter, fred.Resp) error
}

type HandlerFunc func(ResponseWriter, fred.Resp) error

func (f HandlerFunc) ServeRESP(w ResponseWriter, r fred.Resp) error {
	return f(w, r)
}

// Server

type Server struct {
	Handler  Handler
	ErrorLog Logger

	ReadTimeout  time.Duration
	WriteTimeout time.Duration

	stopped     chan struct{}
	stoppedOnce sync.Once
	openConns   sync.WaitGroup
}

func NewServer(handler Handler) *Server {
	return &Server{
		Handler: handler,

		ReadTimeout:  time.Second * 15,
		WriteTimeout: 0,

		stopped: make(chan struct{}),
	}
}

func (s *Server) log(format string, args ...interface{}) {
	if s.ErrorLog != nil {
		s.ErrorLog.Printf(format, args...)
	}
}

func (s *Server) Close() {
	s.stoppedOnce.Do(func() { close(s.stopped) })
	s.openConns.Wait()
}

func (s *Server) Serve(l net.Listener) error {
	addr := l.Addr()
	l = newInterruptListener(l)
	defer s.log("Stopping server listening on %v", addr)

	go func(l net.Listener) {
		<-s.stopped
		if err := l.Close(); err != nil {
			s.log("Error closing listener: %v", err)
		}
	}(l)

loop:
	for {
		select {
		case <-s.stopped:
			break loop
		default:
		}

		conn, err := l.Accept()
		if err != nil {
			if _, ok := err.(ListenerClosedErr); ok {
				// OK, exit
				return nil
			}

			s.log("Error accepting connection: %v", err)
			if ne, ok := err.(net.Error); ok && !ne.Temporary() {
				return err
			}

			continue
		}

		s.openConns.Add(1)
		go s.handleConn(conn)
	}

	return nil
}

func (s *Server) handleConn(conn net.Conn) {
	addr := conn.RemoteAddr()
	s.log("%v: Connection received", addr)
	defer func() {
		if err := conn.Close(); err != nil {
			s.log("error closing conn: %v", err)
		}
		s.openConns.Done()
	}()

	r := &scanner{r: conn}
	w := bufferResponder{}

	for {
		w.written = false
		w.w.Reset()

		select {
		case <-s.stopped:
			return
		default:
		}

		var rdead, wdead time.Time
		now := time.Now()
		if s.ReadTimeout > 0 {
			rdead = now.Add(s.ReadTimeout)
		}
		if s.WriteTimeout > 0 {
			wdead = now.Add(s.WriteTimeout)
		}

		conn.SetReadDeadline(rdead)
		conn.SetWriteDeadline(wdead)

		resp := fred.Read(r)
		if resp.Err != nil {
			if ne, ok := resp.Err.(net.Error); ok {
				temp := ne.Temporary()
				if !ne.Timeout() {
					w.Write(fmt.Errorf("CONNERR %v", resp.Err))
					if temp {
						goto writeResp
					}
				}

				if temp {
					continue
				}

				return
			}
		}

		if resp.IsType(fred.Invalid) {
			return
		}

		if err := s.Handler.ServeRESP(&w, resp); err != nil {
			w.w.Reset()
			w.written = false

			if werr := w.Write(fmt.Errorf("SERVERERR %v", err)); werr != nil {
				s.log("Error marshaling SERVERERR: %v", werr)
			}

			s.log("Error from %T.ServeRESP - hanging up connection: %v", s.Handler, err)
			w.Close()
		}

	writeResp:
		if _, err := w.w.WriteTo(conn); err != nil {
			if ne, ok := resp.Err.(net.Error); ok {
				if !ne.Timeout() {
					s.log("Write error: %v", resp.Err)
				}
				if ne.Temporary() {
					continue
				}
				return
			}
		}

		if w.Closed() {
			return
		}
	}
}

// Response writer

type ResponseWriter interface {
	Write(interface{}) error
	Close()

	Closed() bool
}

type bufferResponder struct {
	w       bytes.Buffer
	written bool
	closed  bool
}

func (n *bufferResponder) Write(v interface{}) (err error) {
	if n.closed || n.written {
		return io.EOF
	}

	es := encoderState{w: &n.w}
	if err := es.write(v); err != nil {
		n.w.Reset()
		return err
	}
	n.written = n.w.Len() > 0

	return nil
}

func (n *bufferResponder) Closed() bool {
	return n.closed
}

func (n *bufferResponder) Close() {
	n.closed = true
}

// Interruptible listener

type ListenerClosedErr struct{}

var _ = net.Error(ListenerClosedErr{})

func (ListenerClosedErr) Error() string {
	return "listener closed"
}

func (ListenerClosedErr) Temporary() bool {
	return false
}

func (ListenerClosedErr) Timeout() bool {
	return false
}

type interruptListener struct {
	l net.Listener

	closed    chan struct{}
	closeErr  error
	closeOnce sync.Once
}

func newInterruptListener(l net.Listener) *interruptListener {
	if il, ok := l.(*interruptListener); ok {
		return il
	}

	return &interruptListener{l: l, closed: make(chan struct{})}
}

func (l *interruptListener) Accept() (net.Conn, error) {
	select {
	case <-l.closed:
		return nil, ListenerClosedErr{}
	default:
		return l.l.Accept()
	}
}

func (l *interruptListener) Close() error {
	l.closeOnce.Do(func() {
		l.closeErr = l.l.Close()
		close(l.closed)
	})
	return l.closeErr
}

func (l *interruptListener) Addr() net.Addr {
	select {
	case <-l.closed:
		return nil
	default:
		return l.l.Addr()
	}
}
