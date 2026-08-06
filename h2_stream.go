package goproxy

import (
	"net/http"
	"strings"
)

// H2Stream represents an HTTP/2 stream in a proxied session.
// It is passed to H2StreamHandler and GrpcMessageHandler callbacks and persists
// for the lifetime of the stream, so UserData can hold per-stream state across
// header and data events.
type H2Stream struct {
	// ID is the HTTP/2 stream identifier.
	ID uint32

	// Headers holds the decoded header fields accumulated for this stream.
	Headers http.Header

	// IsGrpc is true when the stream uses a gRPC content-type.
	IsGrpc bool

	// UserData persists across all callbacks for this stream.
	UserData any

	direction H2Direction
	data      []byte
	endStream bool
}

// Direction returns the direction of the current frame (client→server or server→client).
func (s *H2Stream) Direction() H2Direction {
	return s.direction
}

// Data returns the payload of the current DATA frame event, or nil for header events.
func (s *H2Stream) Data() []byte {
	return s.data
}

// EndStream reports whether the current event ends the stream.
func (s *H2Stream) EndStream() bool {
	return s.endStream
}

// Method returns the :method pseudo-header.
func (s *H2Stream) Method() string {
	return s.header(":method")
}

// Path returns the :path pseudo-header.
func (s *H2Stream) Path() string {
	return s.header(":path")
}

// Authority returns the :authority pseudo-header, falling back to host.
func (s *H2Stream) Authority() string {
	if v := s.header(":authority"); v != "" {
		return v
	}
	return s.header("host")
}

// Header returns the first value for the given header name.
func (s *H2Stream) Header(name string) string {
	return s.header(name)
}

// ContentType returns the content-type header value.
func (s *H2Stream) ContentType() string {
	return s.header("content-type")
}

// GrpcMethod returns the gRPC method path (:path pseudo-header) for gRPC streams.
func (s *H2Stream) GrpcMethod() string {
	return s.Path()
}

// Event builds an H2StreamEvent describing the current callback state.
func (s *H2Stream) Event() *H2StreamEvent {
	event := &H2StreamEvent{
		StreamID:  s.ID,
		Direction: s.direction,
		EndStream: s.endStream,
	}
	if len(s.data) > 0 {
		event.Data = append([]byte(nil), s.data...)
	} else if s.Headers != nil {
		event.Headers = s.Headers.Clone()
	}
	return event
}

func (s *H2Stream) header(name string) string {
	if s.Headers == nil {
		return ""
	}
	if v := s.Headers.Get(name); v != "" {
		return v
	}
	// HPACK-decoded headers may use lowercase names.
	for k, vs := range s.Headers {
		if strings.EqualFold(k, name) && len(vs) > 0 {
			return vs[0]
		}
	}
	return ""
}

func (s *H2Stream) bind(dir H2Direction, data []byte, endStream bool) {
	s.direction = dir
	s.data = data
	s.endStream = endStream
}

func (s *h2Session) h2Stream(streamID uint32, st *h2StreamState, dir H2Direction, data []byte, endStream bool) *H2Stream {
	s.streamsMu.Lock()
	defer s.streamsMu.Unlock()

	stream, ok := s.streamObjects[streamID]
	if !ok {
		stream = &H2Stream{ID: streamID}
		s.streamObjects[streamID] = stream
	}
	if st.headers != nil {
		stream.Headers = st.headers
	}
	stream.IsGrpc = st.isGrpc
	stream.bind(dir, data, endStream)
	return stream
}

func (s *h2Session) deleteH2Stream(streamID uint32) {
	s.streamsMu.Lock()
	delete(s.streamObjects, streamID)
	delete(s.streams, streamID)
	s.streamsMu.Unlock()
}
