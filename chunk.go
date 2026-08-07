package goproxy

import (
	"bufio"
	"bytes"
	"io"
	"net/http"
	"strconv"
	"strings"
)

// ChunkDirection indicates whether a chunk travels from the client or the server.
type ChunkDirection int

const (
	ChunkFromClient ChunkDirection = iota
	ChunkFromServer
)

// ChunkEvent is passed to ChunkHandler implementations for each HTTP/1.1 chunk.
type ChunkEvent struct {
	Data     []byte
	Index    int
	IsLast   bool // true for the zero-size terminating chunk (Data empty)
	Trailers http.Header // set on final chunk when trailers follow
}

// ChunkHandler can inspect or modify HTTP/1.1 chunked transfer payloads.
type ChunkHandler interface {
	HandleChunk(event *ChunkEvent, ctx *ProxyCtx) (*ChunkEvent, error)
}

// FuncChunkHandler adapts a function to ChunkHandler.
type FuncChunkHandler func(event *ChunkEvent, ctx *ProxyCtx) (*ChunkEvent, error)

func (f FuncChunkHandler) HandleChunk(event *ChunkEvent, ctx *ProxyCtx) (*ChunkEvent, error) {
	return f(event, ctx)
}

// chunkedReader reads RFC 7230 chunked transfer encoding from a wire-format stream.
type chunkedReader struct {
	br *bufio.Reader
}

func newChunkedReader(r io.Reader) *chunkedReader {
	br, ok := r.(*bufio.Reader)
	if !ok {
		br = bufio.NewReader(r)
	}
	return &chunkedReader{br: br}
}

func (cr *chunkedReader) readChunkSizeLine() (uint64, error) {
	line, err := cr.br.ReadSlice('\n')
	if err != nil {
		return 0, err
	}
	line = bytes.TrimSuffix(line, []byte("\n"))
	line = bytes.TrimSuffix(line, []byte("\r"))
	if i := bytes.IndexByte(line, ';'); i >= 0 {
		line = line[:i]
	}
	line = bytes.TrimSpace(line)
	if len(line) == 0 {
		return 0, io.ErrUnexpectedEOF
	}
	size, err := strconv.ParseUint(string(line), 16, 64)
	if err != nil {
		return 0, err
	}
	return size, nil
}

func (cr *chunkedReader) readTrailers() (http.Header, error) {
	trailers := make(http.Header)
	for {
		line, err := cr.br.ReadSlice('\n')
		if err != nil {
			return nil, err
		}
		line = bytes.TrimSuffix(line, []byte("\n"))
		line = bytes.TrimSuffix(line, []byte("\r"))
		if len(line) == 0 {
			return trailers, nil
		}
		colon := bytes.IndexByte(line, ':')
		if colon < 0 {
			continue
		}
		key := strings.TrimSpace(string(line[:colon]))
		val := strings.TrimSpace(string(line[colon+1:]))
		trailers.Add(key, val)
	}
}

func (cr *chunkedReader) nextChunk() (data []byte, trailers http.Header, isLast bool, err error) {
	size, err := cr.readChunkSizeLine()
	if err != nil {
		return nil, nil, false, err
	}
	if size == 0 {
		trailers, err = cr.readTrailers()
		if err != nil {
			return nil, nil, true, err
		}
		return nil, trailers, true, nil
	}
	data = make([]byte, size)
	if _, err = io.ReadFull(cr.br, data); err != nil {
		return nil, nil, false, err
	}
	// Trailing CRLF after chunk data.
	if _, err = cr.br.ReadSlice('\n'); err != nil {
		return nil, nil, false, err
	}
	return data, nil, false, nil
}

type prependReadCloser struct {
	io.Reader
	io.Closer
}

// bodyLooksChunked peeks at the first line of body to detect wire-format chunked encoding.
// When true, returns a new ReadCloser that replays the peeked bytes.
// When false, also returns a ReadCloser that replays peeked bytes — the original
// reader must not be returned after the peek or those bytes are lost.
func bodyLooksChunked(r io.ReadCloser) (bool, io.ReadCloser) {
	br := bufio.NewReader(r)
	line, err := br.ReadSlice('\n')
	replay := &prependReadCloser{
		Reader: io.MultiReader(bytes.NewReader(line), br),
		Closer: r,
	}
	if err != nil {
		// Incomplete first line (EOF without '\n', or buffer full) cannot be a
		// chunk-size line. Replay whatever was read.
		return false, replay
	}
	trimmed := bytes.TrimSuffix(bytes.TrimSuffix(line, []byte("\n")), []byte("\r"))
	if len(trimmed) == 0 {
		return false, replay
	}
	end := 0
	for end < len(trimmed) && isHex(trimmed[end]) {
		end++
	}
	if end == 0 {
		return false, replay
	}
	if _, err := strconv.ParseUint(string(trimmed[:end]), 16, 64); err != nil {
		return false, replay
	}
	return true, replay
}

func isHex(b byte) bool {
	return (b >= '0' && b <= '9') || (b >= 'a' && b <= 'f') || (b >= 'A' && b <= 'F')
}

func isChunkedTransferEncoding(header http.Header, te []string) bool {
	if len(te) > 0 {
		for _, v := range te {
			if chunkedTEValue(v) {
				return true
			}
		}
	}
	return chunkedTEValue(header.Get("Transfer-Encoding"))
}

func chunkedTEValue(v string) bool {
	for _, part := range strings.Split(v, ",") {
		if strings.EqualFold(strings.TrimSpace(part), "chunked") {
			return true
		}
	}
	return false
}

func (proxy *ProxyHttpServer) wrapChunkHookBody(body io.ReadCloser, header http.Header, te []string, ctx *ProxyCtx, dir ChunkDirection) io.ReadCloser {
	if dir == ChunkFromServer {
		if !proxy.hasRespChunkHandlers() {
			return body
		}
	} else if !proxy.hasReqChunkHandlers() {
		return body
	}

	if !isChunkedTransferEncoding(header, te) {
		ok, newBody := bodyLooksChunked(body)
		body = newBody
		if !ok {
			return body
		}
	}
	return newChunkHookReadCloser(body, proxy, ctx, dir)
}

// chunkHookReadCloser parses wire-format HTTP chunks, invokes chunk handlers, and
// exposes the decoded (de-chunked) modified payload via Read.
type chunkHookReadCloser struct {
	reader   *chunkedReader
	body     io.ReadCloser
	proxy    *ProxyHttpServer
	ctx      *ProxyCtx
	dir      ChunkDirection
	buf      []byte
	index    int
	done     bool
	err      error
	trailers http.Header
	closed   bool
}

func newChunkHookReadCloser(body io.ReadCloser, proxy *ProxyHttpServer, ctx *ProxyCtx, dir ChunkDirection) *chunkHookReadCloser {
	return &chunkHookReadCloser{
		reader: newChunkedReader(body),
		body:   body,
		proxy:  proxy,
		ctx:    ctx,
		dir:    dir,
	}
}

// Trailers returns trailer headers read from the terminating chunk, including any
// modifications made by chunk handlers on the final ChunkEvent.
func (c *chunkHookReadCloser) Trailers() http.Header {
	return c.trailers
}

func (c *chunkHookReadCloser) Read(p []byte) (int, error) {
	for {
		if len(c.buf) > 0 {
			n := copy(p, c.buf)
			c.buf = c.buf[n:]
			return n, nil
		}
		if c.done {
			return 0, io.EOF
		}
		if c.err != nil {
			return 0, c.err
		}
		if err := c.loadNextChunk(); err != nil {
			if err == io.EOF {
				c.done = true
				return 0, io.EOF
			}
			c.err = err
			return 0, err
		}
	}
}

func (c *chunkHookReadCloser) loadNextChunk() error {
	for {
		data, rawTrailers, isLast, err := c.reader.nextChunk()
		if err != nil {
			return err
		}

		event := &ChunkEvent{
			Data:   data,
			Index:  c.index,
			IsLast: isLast,
		}
		if isLast {
			event.Trailers = rawTrailers
		}

		prevDir := c.ctx.ChunkDirection
		prevIndex := c.ctx.ChunkIndex
		c.ctx.ChunkDirection = c.dir
		c.ctx.ChunkIndex = c.index

		out, err := c.proxy.filterChunk(event, c.ctx)
		c.ctx.ChunkDirection = prevDir
		c.ctx.ChunkIndex = prevIndex

		if err != nil {
			return err
		}
		if out == nil {
			if isLast {
				c.done = true
				return io.EOF
			}
			c.index++
			continue
		}

		if isLast {
			c.trailers = out.Trailers
			if c.trailers == nil {
				c.trailers = rawTrailers
			}
			c.done = true
			if len(out.Data) == 0 {
				return io.EOF
			}
			c.buf = out.Data
			return nil
		}

		c.index++
		if out.Data == nil {
			continue
		}
		c.buf = out.Data
		return nil
	}
}

func (c *chunkHookReadCloser) Close() error {
	if c.closed {
		return nil
	}
	c.closed = true
	return c.body.Close()
}

func (proxy *ProxyHttpServer) filterChunk(event *ChunkEvent, ctx *ProxyCtx) (*ChunkEvent, error) {
	var handlers []ChunkHandler
	if ctx.ChunkDirection == ChunkFromServer {
		handlers = proxy.respChunkHandlers
	} else {
		handlers = proxy.reqChunkHandlers
	}
	out := event
	for _, h := range handlers {
		var err error
		out, err = h.HandleChunk(out, ctx)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

func (proxy *ProxyHttpServer) hasRespChunkHandlers() bool {
	return len(proxy.respChunkHandlers) > 0
}

func (proxy *ProxyHttpServer) hasReqChunkHandlers() bool {
	return len(proxy.reqChunkHandlers) > 0
}

func applyChunkTrailers(resp *http.Response) {
	ch, ok := resp.Body.(*chunkHookReadCloser)
	if !ok {
		return
	}
	if t := ch.Trailers(); len(t) > 0 {
		if resp.Trailer == nil {
			resp.Trailer = make(http.Header)
		}
		for k, vs := range t {
			resp.Trailer[k] = append([]string(nil), vs...)
		}
	}
}
