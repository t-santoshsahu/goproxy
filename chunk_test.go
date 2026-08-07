package goproxy

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"testing"
)

func TestIsChunkedTransferEncoding(t *testing.T) {
	h := make(http.Header)
	if isChunkedTransferEncoding(h, nil) {
		t.Fatal("expected false for empty header")
	}
	h.Set("Transfer-Encoding", "chunked")
	if !isChunkedTransferEncoding(h, nil) {
		t.Fatal("expected true for chunked header")
	}
	h.Set("Transfer-Encoding", "gzip, chunked")
	if !isChunkedTransferEncoding(h, nil) {
		t.Fatal("expected true for gzip, chunked")
	}
	if !isChunkedTransferEncoding(h, []string{"chunked"}) {
		t.Fatal("expected true for te slice")
	}
	h2 := make(http.Header)
	if isChunkedTransferEncoding(h2, []string{"gzip"}) {
		t.Fatal("expected false for gzip only te slice")
	}
}

func TestChunkedReaderMultipleChunksAndTrailers(t *testing.T) {
	raw := "" +
		"5\r\n" +
		"hello\r\n" +
		"6\r\n" +
		" world\r\n" +
		"0\r\n" +
		"X-Trailer: value\r\n" +
		"\r\n"
	cr := newChunkedReader(strings.NewReader(raw))

	data, trailers, isLast, err := cr.nextChunk()
	if err != nil {
		t.Fatalf("chunk 0: %v", err)
	}
	if isLast || string(data) != "hello" || trailers != nil {
		t.Fatalf("chunk 0: isLast=%v data=%q trailers=%v", isLast, data, trailers)
	}

	data, trailers, isLast, err = cr.nextChunk()
	if err != nil {
		t.Fatalf("chunk 1: %v", err)
	}
	if isLast || string(data) != " world" {
		t.Fatalf("chunk 1: isLast=%v data=%q", isLast, data)
	}

	data, trailers, isLast, err = cr.nextChunk()
	if err != nil {
		t.Fatalf("last chunk: %v", err)
	}
	if !isLast || len(data) != 0 {
		t.Fatalf("last chunk: isLast=%v data=%q", isLast, data)
	}
	if trailers.Get("X-Trailer") != "value" {
		t.Fatalf("trailers: %v", trailers)
	}
}

func TestChunkHookReadCloserModifiesPayload(t *testing.T) {
	raw := "5\r\nhello\r\n0\r\n\r\n"
	proxy := NewProxyHttpServer()
	proxy.respChunkHandlers = append(proxy.respChunkHandlers, FuncChunkHandler(func(event *ChunkEvent, ctx *ProxyCtx) (*ChunkEvent, error) {
		if !event.IsLast && len(event.Data) > 0 {
			event.Data = bytes.ToUpper(event.Data)
		}
		return event, nil
	}))

	ctx := &ProxyCtx{Proxy: proxy}
	body := newChunkHookReadCloser(io.NopCloser(strings.NewReader(raw)), proxy, ctx, ChunkFromServer)
	out, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "HELLO" {
		t.Fatalf("got %q want HELLO", out)
	}
}

func TestFilterChunkWithConditions(t *testing.T) {
	proxy := NewProxyHttpServer()
	proxy.OnResponse(StatusCodeIs(200)).OnChunkFunc(func(event *ChunkEvent, ctx *ProxyCtx) (*ChunkEvent, error) {
		if len(event.Data) > 0 {
			event.Data = append(event.Data, '!')
		}
		return event, nil
	})
	proxy.OnResponse(StatusCodeIs(404)).OnChunkFunc(func(event *ChunkEvent, ctx *ProxyCtx) (*ChunkEvent, error) {
		event.Data = []byte("blocked")
		return event, nil
	})

	ctx := &ProxyCtx{
		Proxy: proxy,
		Req:   &http.Request{},
		Resp:  &http.Response{StatusCode: 200},
	}
	ctx.ChunkDirection = ChunkFromServer

	out, err := proxy.filterChunk(&ChunkEvent{Data: []byte("ok")}, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(out.Data) != "ok!" {
		t.Fatalf("got %q want ok!", out.Data)
	}

	ctx.Resp.StatusCode = 404
	out, err = proxy.filterChunk(&ChunkEvent{Data: []byte("ok")}, ctx)
	if err != nil {
		t.Fatal(err)
	}
	if string(out.Data) != "blocked" {
		t.Fatalf("404 handler should run, got %q", out.Data)
	}
}

func TestWrapChunkHookBodyPassthroughNoHandlers(t *testing.T) {
	proxy := NewProxyHttpServer()
	ctx := &ProxyCtx{Proxy: proxy}
	body := io.NopCloser(strings.NewReader("5\r\nhello\r\n0\r\n\r\n"))
	h := make(http.Header)
	h.Set("Transfer-Encoding", "chunked")

	out := proxy.wrapChunkHookBody(body, h, nil, ctx, ChunkFromServer)
	if out != body {
		t.Fatal("expected original body when no handlers registered")
	}
}

func TestWrapChunkHookBodyWithHandler(t *testing.T) {
	proxy := NewProxyHttpServer()
	proxy.OnResponse().OnChunkFunc(func(event *ChunkEvent, ctx *ProxyCtx) (*ChunkEvent, error) {
		if len(event.Data) > 0 {
			event.Data = bytes.ReplaceAll(event.Data, []byte("e"), []byte("E"))
		}
		return event, nil
	})

	ctx := &ProxyCtx{Proxy: proxy, Resp: &http.Response{StatusCode: 200}}
	raw := "5\r\nhello\r\n0\r\n\r\n"
	body := io.NopCloser(strings.NewReader(raw))
	h := make(http.Header)
	h.Set("Transfer-Encoding", "chunked")

	wrapped := proxy.wrapChunkHookBody(body, h, nil, ctx, ChunkFromServer)
	out, err := io.ReadAll(wrapped)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "hEllo" {
		t.Fatalf("got %q want hEllo", out)
	}
}

func TestChunkHookReadCloserSkipsNilDataChunk(t *testing.T) {
	raw := "3\r\nfoo\r\n3\r\nbar\r\n0\r\n\r\n"
	proxy := NewProxyHttpServer()
	proxy.respChunkHandlers = append(proxy.respChunkHandlers, FuncChunkHandler(func(event *ChunkEvent, ctx *ProxyCtx) (*ChunkEvent, error) {
		if event.Index == 0 {
			return &ChunkEvent{Data: nil}, nil
		}
		return event, nil
	}))

	ctx := &ProxyCtx{Proxy: proxy}
	body := newChunkHookReadCloser(io.NopCloser(strings.NewReader(raw)), proxy, ctx, ChunkFromServer)
	out, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != "bar" {
		t.Fatalf("got %q want bar", out)
	}
}

func TestBodyLooksChunked(t *testing.T) {
	raw := "5\r\nhello\r\n0\r\n\r\n"
	ok, body := bodyLooksChunked(io.NopCloser(strings.NewReader(raw)))
	if !ok {
		t.Fatal("expected chunked detection")
	}
	out, err := io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != raw {
		t.Fatalf("replay mismatch: got %q", out)
	}

	plain := "plain text"
	ok, body = bodyLooksChunked(io.NopCloser(strings.NewReader(plain)))
	if ok {
		t.Fatal("expected false for plain text")
	}
	out, err = io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != plain {
		t.Fatalf("plain text replay mismatch: got %q want %q", out, plain)
	}

	jsonBody := `{"name":"abc"}`
	ok, body = bodyLooksChunked(io.NopCloser(strings.NewReader(jsonBody)))
	if ok {
		t.Fatal("expected false for JSON without newline")
	}
	out, err = io.ReadAll(body)
	if err != nil {
		t.Fatal(err)
	}
	if string(out) != jsonBody {
		t.Fatalf("JSON replay mismatch: got %q want %q", out, jsonBody)
	}
}
