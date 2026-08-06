package goproxy

import (
	"net/http"
	"regexp"
	"testing"
)

func TestH2StreamConditions(t *testing.T) {
	headers := http.Header{}
	headers.Set(":method", "POST")
	headers.Set(":path", "/foo.Bar/Method")
	headers.Set(":authority", "example.com")
	headers.Set("content-type", "application/grpc")
	ctx := &ProxyCtx{}

	if !H2MethodIs("POST").HandleH2Stream(headers, ctx) {
		t.Fatal("H2MethodIs POST should match")
	}
	if H2MethodIs("GET").HandleH2Stream(headers, ctx) {
		t.Fatal("H2MethodIs GET should not match")
	}
	if !H2PathMatches(regexp.MustCompile(`/foo\.Bar/`)).HandleH2Stream(headers, ctx) {
		t.Fatal("H2PathMatches should match")
	}
	if !H2HostIs("example.com").HandleH2Stream(headers, ctx) {
		t.Fatal("H2HostIs should match")
	}
	if !IsGrpcStream().HandleH2Stream(headers, ctx) {
		t.Fatal("IsGrpcStream should match")
	}
}

func TestH2StreamAccessors(t *testing.T) {
	headers := http.Header{}
	headers.Set(":method", "POST")
	headers.Set(":path", "/foo.Service/Method")
	headers.Set(":authority", "api.example.com")
	headers.Set("content-type", "application/grpc")

	stream := &H2Stream{
		ID:      7,
		Headers: headers,
		IsGrpc:  true,
	}
	stream.bind(H2FromClient, []byte("payload"), false)

	if stream.ID != 7 {
		t.Fatalf("ID = %d", stream.ID)
	}
	if stream.Method() != "POST" {
		t.Fatalf("Method = %q", stream.Method())
	}
	if stream.Path() != "/foo.Service/Method" {
		t.Fatalf("Path = %q", stream.Path())
	}
	if stream.GrpcMethod() != "/foo.Service/Method" {
		t.Fatalf("GrpcMethod = %q", stream.GrpcMethod())
	}
	if stream.Authority() != "api.example.com" {
		t.Fatalf("Authority = %q", stream.Authority())
	}
	if !stream.IsGrpc {
		t.Fatal("expected grpc stream")
	}
	if string(stream.Data()) != "payload" {
		t.Fatalf("Data = %q", stream.Data())
	}
	if stream.Direction() != H2FromClient {
		t.Fatalf("Direction = %v", stream.Direction())
	}

	event := stream.Event()
	if event.StreamID != 7 || event.Data == nil || event.Headers != nil {
		t.Fatalf("unexpected event: %+v", event)
	}
}

func TestFilterH2Stream(t *testing.T) {
	proxy := NewProxyHttpServer()
	var seen int
	proxy.OnH2Stream(H2MethodIs("POST")).DoFunc(func(stream *H2Stream, ctx *ProxyCtx) (*H2StreamEvent, error) {
		seen++
		event := stream.Event()
		event.Headers.Set("x-modified", "1")
		return event, nil
	})

	stream := &H2Stream{
		ID:      1,
		Headers: http.Header{":method": {"POST"}, ":path": {"/test"}},
	}
	stream.bind(H2FromClient, nil, false)
	ctx := &ProxyCtx{H2Stream: stream, H2Headers: stream.Headers}

	out, err := proxy.filterH2Stream(stream, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if seen != 1 {
		t.Fatalf("handler called %d times, want 1", seen)
	}
	if out.Headers.Get("x-modified") != "1" {
		t.Fatal("expected header modification")
	}
}

func TestFilterH2StreamSkipsOnCondition(t *testing.T) {
	proxy := NewProxyHttpServer()
	proxy.OnH2Stream(H2MethodIs("GET")).DoFunc(func(stream *H2Stream, ctx *ProxyCtx) (*H2StreamEvent, error) {
		event := stream.Event()
		event.Headers.Set("x-modified", "1")
		return event, nil
	})

	stream := &H2Stream{
		Headers: http.Header{":method": {"POST"}},
	}
	stream.bind(H2FromClient, nil, false)
	ctx := &ProxyCtx{H2Stream: stream, H2Headers: stream.Headers}

	out, err := proxy.filterH2Stream(stream, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out.Headers.Get("x-modified") != "" {
		t.Fatal("handler should not have run")
	}
}

func TestFilterH2StreamData(t *testing.T) {
	proxy := NewProxyHttpServer()
	proxy.OnH2Stream().DoFunc(func(stream *H2Stream, ctx *ProxyCtx) (*H2StreamEvent, error) {
		if len(stream.Data()) > 0 {
			event := stream.Event()
			event.Data = append([]byte("prefix:"), event.Data...)
			return event, nil
		}
		return stream.Event(), nil
	})

	stream := &H2Stream{ID: 3}
	stream.bind(H2FromServer, []byte("payload"), false)
	ctx := &ProxyCtx{H2Stream: stream}

	out, err := proxy.filterH2Stream(stream, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(out.Data) != "prefix:payload" {
		t.Fatalf("got %q", out.Data)
	}
}

func TestH2StreamUserData(t *testing.T) {
	proxy := NewProxyHttpServer()
	stream := &H2Stream{
		ID:      1,
		Headers: http.Header{":method": {"POST"}},
	}
	stream.bind(H2FromClient, nil, false)

	proxy.OnH2Stream().DoFunc(func(s *H2Stream, ctx *ProxyCtx) (*H2StreamEvent, error) {
		if s.UserData == nil {
			s.UserData = 1
		} else {
			s.UserData = s.UserData.(int) + 1
		}
		return s.Event(), nil
	})

	ctx := &ProxyCtx{H2Stream: stream, H2Headers: stream.Headers}
	if _, err := proxy.filterH2Stream(stream, ctx); err != nil {
		t.Fatal(err)
	}
	stream.bind(H2FromClient, []byte("data"), false)
	if _, err := proxy.filterH2Stream(stream, ctx); err != nil {
		t.Fatal(err)
	}
	if stream.UserData.(int) != 2 {
		t.Fatalf("UserData = %v, want 2", stream.UserData)
	}
}

func TestEncodeHeaderBlock(t *testing.T) {
	headers := http.Header{
		":method": {"GET"},
		":path":   {"/hello"},
	}
	block, err := encodeHeaderBlock(headers)
	if err != nil {
		t.Fatal(err)
	}
	if len(block) == 0 {
		t.Fatal("expected non-empty header block")
	}
}
