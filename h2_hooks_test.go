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

func TestFilterH2Stream(t *testing.T) {
	proxy := NewProxyHttpServer()
	var seen int
	proxy.OnH2Stream(H2MethodIs("POST")).DoFunc(func(event *H2StreamEvent, ctx *ProxyCtx) (*H2StreamEvent, error) {
		seen++
		event.Headers.Set("x-modified", "1")
		return event, nil
	})

	ctx := &ProxyCtx{H2Headers: http.Header{":method": {"POST"}}}
	event := &H2StreamEvent{
		StreamID:  1,
		Direction: H2FromClient,
		Headers:   http.Header{":method": {"POST"}, ":path": {"/test"}},
	}

	out, err := proxy.filterH2Stream(event, ctx)
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
	proxy.OnH2Stream(H2MethodIs("GET")).DoFunc(func(event *H2StreamEvent, ctx *ProxyCtx) (*H2StreamEvent, error) {
		event.Headers.Set("x-modified", "1")
		return event, nil
	})

	ctx := &ProxyCtx{H2Headers: http.Header{":method": {"POST"}}}
	event := &H2StreamEvent{
		Headers: http.Header{":method": {"POST"}},
	}

	out, err := proxy.filterH2Stream(event, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if out.Headers.Get("x-modified") != "" {
		t.Fatal("handler should not have run")
	}
}

func TestFilterH2StreamData(t *testing.T) {
	proxy := NewProxyHttpServer()
	proxy.OnH2Stream().DoFunc(func(event *H2StreamEvent, ctx *ProxyCtx) (*H2StreamEvent, error) {
		if event.Data != nil {
			event.Data = append([]byte("prefix:"), event.Data...)
		}
		return event, nil
	})

	ctx := &ProxyCtx{}
	event := &H2StreamEvent{
		StreamID:  3,
		Direction: H2FromServer,
		Data:      []byte("payload"),
	}

	out, err := proxy.filterH2Stream(event, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(out.Data) != "prefix:payload" {
		t.Fatalf("got %q", out.Data)
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
