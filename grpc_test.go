package goproxy

import (
	"encoding/binary"
	"testing"
)

func TestIsGrpcContentType(t *testing.T) {
	tests := []struct {
		ct   string
		want bool
	}{
		{"application/grpc", true},
		{"application/grpc+proto", true},
		{"application/grpc+json", true},
		{"Application/Grpc", true},
		{"text/plain", false},
		{"application/json", false},
	}
	for _, tc := range tests {
		if got := isGrpcContentType(tc.ct); got != tc.want {
			t.Errorf("isGrpcContentType(%q) = %v, want %v", tc.ct, got, tc.want)
		}
	}
}

func TestProcessGrpcData(t *testing.T) {
	proxy := NewProxyHttpServer()
	proxy.OnGrpcStream().DoFunc(func(stream *H2Stream, msg []byte, endStream bool, ctx *ProxyCtx) ([]byte, error) {
		out := make([]byte, len(msg))
		copy(out, msg)
		for i := range out {
			out[i] ^= 0xff
		}
		return out, nil
	})

	frame := func(msg []byte) []byte {
		hdr := make([]byte, grpcHeaderSize)
		binary.BigEndian.PutUint32(hdr[1:grpcHeaderSize], uint32(len(msg)))
		return append(hdr, msg...)
	}

	stream := &H2Stream{ID: 1, IsGrpc: true, Headers: make(map[string][]string)}
	stream.bind(H2FromClient, nil, false)
	state := &h2StreamState{isGrpc: true}
	ctx := &ProxyCtx{Proxy: proxy, H2Stream: stream}

	out, modified, err := processGrpcData(frame([]byte("hello")), state, proxy, ctx, stream, true)
	if err != nil {
		t.Fatal(err)
	}
	if !modified {
		t.Fatal("expected modified output")
	}
	if len(out) != grpcHeaderSize+5 {
		t.Fatalf("unexpected output length %d", len(out))
	}
	payload := out[grpcHeaderSize:]
	for i, b := range payload {
		if b != []byte("hello")[i]^0xff {
			t.Fatalf("payload not transformed at %d", i)
		}
	}
}

func TestProcessGrpcDataBuffersPartialMessage(t *testing.T) {
	proxy := NewProxyHttpServer()
	proxy.OnGrpcStream().DoFunc(func(stream *H2Stream, msg []byte, endStream bool, ctx *ProxyCtx) ([]byte, error) {
		return msg, nil
	})

	stream := &H2Stream{ID: 1, IsGrpc: true}
	stream.bind(H2FromClient, nil, false)
	state := &h2StreamState{isGrpc: true}
	ctx := &ProxyCtx{Proxy: proxy, H2Stream: stream}

	partial := []byte{0, 0, 0, 0, 10, 'h', 'i'}
	out, modified, err := processGrpcData(partial, state, proxy, ctx, stream, false)
	if err != nil {
		t.Fatal(err)
	}
	if out != nil || modified {
		t.Fatalf("expected buffered partial frame, got out=%v modified=%v", out, modified)
	}
	if len(state.grpcBuf) != len(partial) {
		t.Fatalf("expected buffer length %d, got %d", len(partial), len(state.grpcBuf))
	}
}

func TestFilterGrpcMessageUsesStream(t *testing.T) {
	proxy := NewProxyHttpServer()
	var gotPath string
	proxy.OnGrpcStream().DoFunc(func(stream *H2Stream, msg []byte, endStream bool, ctx *ProxyCtx) ([]byte, error) {
		gotPath = stream.Path()
		return msg, nil
	})

	stream := &H2Stream{
		ID:      5,
		IsGrpc:  true,
		Headers: map[string][]string{":path": {"/svc.Method"}},
	}
	stream.bind(H2FromClient, nil, true)
	ctx := &ProxyCtx{Proxy: proxy, H2Stream: stream}

	if _, err := proxy.filterGrpcMessage(stream, []byte("x"), true, ctx); err != nil {
		t.Fatal(err)
	}
	if gotPath != "/svc.Method" {
		t.Fatalf("Path = %q", gotPath)
	}
}
