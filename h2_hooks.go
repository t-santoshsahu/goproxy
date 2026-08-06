package goproxy

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"

	"golang.org/x/net/http2"
	"golang.org/x/net/http2/hpack"
)

const (
	defaultHpackTableSize = 4096
	defaultMaxFrameSize   = 16384
)

// H2Direction indicates whether a frame travels from the client or the server.
type H2Direction int

const (
	H2FromClient H2Direction = iota
	H2FromServer
)

// H2StreamEvent is passed to H2StreamHandler implementations.
// Exactly one of Headers or Data is set.
type H2StreamEvent struct {
	StreamID  uint32
	Direction H2Direction
	EndStream bool
	Headers   http.Header
	Data      []byte
}

// H2StreamHandler can inspect or modify HTTP/2 stream headers and data.
type H2StreamHandler interface {
	HandleH2Stream(stream *H2Stream, ctx *ProxyCtx) (*H2StreamEvent, error)
}

// FuncH2StreamHandler adapts a function to H2StreamHandler.
type FuncH2StreamHandler func(stream *H2Stream, ctx *ProxyCtx) (*H2StreamEvent, error)

func (f FuncH2StreamHandler) HandleH2Stream(stream *H2Stream, ctx *ProxyCtx) (*H2StreamEvent, error) {
	return f(stream, ctx)
}

// H2StreamCondition decides whether a registered handler applies to a stream.
type H2StreamCondition interface {
	HandleH2Stream(headers http.Header, ctx *ProxyCtx) bool
}

// H2PathMatches matches the :path pseudo-header against re.
func H2PathMatches(re *regexp.Regexp) H2StreamCondition {
	return h2StreamCondFunc(func(headers http.Header, ctx *ProxyCtx) bool {
		return re.MatchString(headers.Get(":path"))
	})
}

// H2MethodIs matches the :method pseudo-header against one of the given methods.
func H2MethodIs(methods ...string) H2StreamCondition {
	methodSet := make(map[string]bool, len(methods))
	for _, m := range methods {
		methodSet[strings.ToUpper(m)] = true
	}
	return h2StreamCondFunc(func(headers http.Header, ctx *ProxyCtx) bool {
		_, ok := methodSet[strings.ToUpper(headers.Get(":method"))]
		return ok
	})
}

// H2HostIs matches the :authority pseudo-header against one of the given hosts.
func H2HostIs(hosts ...string) H2StreamCondition {
	hostSet := make(map[string]bool, len(hosts))
	for _, h := range hosts {
		hostSet[strings.ToLower(h)] = true
	}
	return h2StreamCondFunc(func(headers http.Header, ctx *ProxyCtx) bool {
		authority := headers.Get(":authority")
		if authority == "" {
			authority = headers.Get("host")
		}
		host := authority
		if i := strings.LastIndex(host, ":"); i != -1 {
			if strings.Count(host, ":") == 1 {
				host = host[:i]
			}
		}
		_, ok := hostSet[strings.ToLower(host)]
		return ok
	})
}

// IsGrpcStream matches streams whose content-type is application/grpc or application/grpc+*.
func IsGrpcStream() H2StreamCondition {
	return h2StreamCondFunc(func(headers http.Header, ctx *ProxyCtx) bool {
		return isGrpcContentType(headers.Get("content-type"))
	})
}

type h2StreamCondFunc func(headers http.Header, ctx *ProxyCtx) bool

func (f h2StreamCondFunc) HandleH2Stream(headers http.Header, ctx *ProxyCtx) bool {
	return f(headers, ctx)
}

type h2StreamState struct {
	headers     http.Header
	headerBlock []byte
	isGrpc      bool
	grpcBuf     []byte
}

type h2Session struct {
	proxy              *ProxyHttpServer
	ctx                *ProxyCtx
	host               string
	upstreamServerName string
	upstreamTLS        *tls.Config
	dial         func(network, addr string) (net.Conn, error)
	clientReader io.Reader
	clientWriter io.Writer

	clientDec *hpack.Decoder
	serverDec *hpack.Decoder

	maxFrameSize uint32

	streams        map[uint32]*h2StreamState
	streamObjects  map[uint32]*H2Stream
	streamsMu      sync.Mutex

	blindRelay bool
}

func newH2Session(tr *H2Transport) *h2Session {
	s := &h2Session{
		proxy:              tr.Proxy,
		ctx:                tr.Ctx,
		host:               tr.Host,
		upstreamServerName: tr.UpstreamServerName,
		upstreamTLS:        tr.UpstreamTLS,
		dial:         tr.Dial,
		clientReader: tr.ClientReader,
		clientWriter: tr.ClientWriter,
		clientDec:    hpack.NewDecoder(defaultHpackTableSize, nil),
		serverDec:    hpack.NewDecoder(defaultHpackTableSize, nil),
		maxFrameSize: defaultMaxFrameSize,
		streams:       make(map[uint32]*h2StreamState),
		streamObjects: make(map[uint32]*H2Stream),
	}
	if s.upstreamTLS == nil {
		s.upstreamTLS = tr.TLSConfig
	}
	if s.dial == nil {
		s.dial = dial
	}
	s.blindRelay = s.proxy == nil || (len(s.proxy.h2StreamHandlers) == 0 && len(s.proxy.grpcStreamHandlers) == 0)
	return s
}

func (s *h2Session) run() error {
	raddr := s.host
	if !strings.Contains(raddr, ":") {
		raddr += ":443"
	}
	rawServerTLS, err := s.dial("tcp", raddr)
	if err != nil {
		return err
	}
	defer rawServerTLS.Close()

	tlsConfig := s.upstreamTLS
	if tlsConfig == nil {
		tlsConfig = &tls.Config{}
	} else {
		tlsConfig = tlsConfig.Clone()
	}
	tlsConfig.NextProtos = []string{http2.NextProtoTLS}
	serverName := s.upstreamServerName
	if serverName == "" {
		serverName = raddr[:strings.LastIndex(raddr, ":")]
	}
	tlsConfig.ServerName = serverName
	rawServerTLS = tls.Client(rawServerTLS, tlsConfig)
	rawTLSConn, ok := rawServerTLS.(*tls.Conn)
	if !ok {
		return errors.New("invalid TLS connection")
	}
	if err = rawTLSConn.HandshakeContext(context.Background()); err != nil {
		return err
	}
	if tlsConfig == nil || !tlsConfig.InsecureSkipVerify {
		if err = rawTLSConn.VerifyHostname(serverName); err != nil {
			return err
		}
	}

	if _, err := io.WriteString(rawServerTLS, http2.ClientPreface); err != nil {
		return err
	}

	serverTLSReader := bufio.NewReader(rawServerTLS)
	cToS := http2.NewFramer(rawServerTLS, s.clientReader)
	sToC := http2.NewFramer(s.clientWriter, serverTLSReader)

	errSToC := make(chan error)
	errCToS := make(chan error)
	go func() {
		for {
			if err := s.proxyFrame(sToC, H2FromServer); err != nil {
				errSToC <- err
				break
			}
		}
	}()
	go func() {
		for {
			if err := s.proxyFrame(cToS, H2FromClient); err != nil {
				errCToS <- err
				break
			}
		}
	}()

	for i := 0; i < 2; i++ {
		select {
		case err := <-errSToC:
			if !errors.Is(err, io.EOF) {
				return err
			}
		case err := <-errCToS:
			if !errors.Is(err, io.EOF) {
				return err
			}
		}
	}
	return nil
}

func (s *h2Session) decoder(dir H2Direction) *hpack.Decoder {
	if dir == H2FromClient {
		return s.clientDec
	}
	return s.serverDec
}

func (s *h2Session) streamState(streamID uint32) *h2StreamState {
	s.streamsMu.Lock()
	defer s.streamsMu.Unlock()
	st, ok := s.streams[streamID]
	if !ok {
		st = &h2StreamState{}
		s.streams[streamID] = st
	}
	return st
}

func (s *h2Session) deleteStream(streamID uint32) {
	s.deleteH2Stream(streamID)
}

func (s *h2Session) proxyFrame(fr *http2.Framer, dir H2Direction) error {
	if s.blindRelay {
		return proxyFrame(fr)
	}

	f, err := fr.ReadFrame()
	if err != nil {
		return err
	}

	switch f.Header().Type {
	case http2.FrameHeaders:
		tf, ok := f.(*http2.HeadersFrame)
		if !ok {
			return ErrInvalidH2Frame
		}
		return s.handleHeaders(fr, tf, dir)
	case http2.FrameContinuation:
		tf, ok := f.(*http2.ContinuationFrame)
		if !ok {
			return ErrInvalidH2Frame
		}
		return s.handleContinuation(fr, tf, dir)
	case http2.FrameData:
		tf, ok := f.(*http2.DataFrame)
		if !ok {
			return ErrInvalidH2Frame
		}
		return s.handleData(fr, tf, dir)
	case http2.FrameSettings:
		tf, ok := f.(*http2.SettingsFrame)
		if !ok {
			return ErrInvalidH2Frame
		}
		if !tf.IsAck() {
			for i := 0; i < tf.NumSettings(); i++ {
				setting := tf.Setting(i)
				if setting.ID == http2.SettingHeaderTableSize {
					s.decoder(dir).SetMaxDynamicTableSize(setting.Val)
				}
				if setting.ID == http2.SettingMaxFrameSize {
					if setting.Val >= 16384 && setting.Val <= 16777215 {
						s.maxFrameSize = setting.Val
					}
				}
			}
		}
		return relayFrame(fr, f)
	case http2.FrameRSTStream:
		tf, ok := f.(*http2.RSTStreamFrame)
		if !ok {
			return ErrInvalidH2Frame
		}
		s.deleteStream(tf.StreamID)
		return relayFrame(fr, f)
	default:
		return relayFrame(fr, f)
	}
}

func (s *h2Session) handleHeaders(fr *http2.Framer, tf *http2.HeadersFrame, dir H2Direction) error {
	st := s.streamState(tf.StreamID)
	st.headerBlock = append(st.headerBlock, tf.HeaderBlockFragment()...)

	if !tf.HeadersEnded() {
		return nil
	}

	block := append([]byte(nil), st.headerBlock...)
	headers, err := s.decoder(dir).DecodeFull(st.headerBlock)
	st.headerBlock = nil
	if err != nil {
		return err
	}

	h := make(http.Header)
	for _, hf := range headers {
		h.Add(hf.Name, hf.Value)
	}

	if st.headers == nil {
		st.headers = h
	} else {
		for k, vs := range h {
			st.headers[k] = append(st.headers[k], vs...)
		}
	}

	if ct := st.headers.Get("content-type"); isGrpcContentType(ct) {
		st.isGrpc = true
	}

	endStream := tf.StreamEnded()
	priority := tf.Priority
	streamID := tf.StreamID

	if s.hasH2Handlers() {
		outHeaders, outEndStream, err := s.invokeH2HeaderHook(st, streamID, dir, endStream, st.headers)
		if err != nil {
			return err
		}
		endStream = outEndStream
		block, err = encodeHeaderBlock(outHeaders)
		if err != nil {
			return err
		}
		st.headers = outHeaders
	}

	terr := writeHeaderBlock(fr, streamID, block, endStream, priority, s.maxFrameSize)
	if endStream {
		s.deleteStream(streamID)
		if terr == nil {
			terr = io.EOF
		}
	}
	return terr
}

func (s *h2Session) handleContinuation(fr *http2.Framer, tf *http2.ContinuationFrame, dir H2Direction) error {
	st := s.streamState(tf.StreamID)
	st.headerBlock = append(st.headerBlock, tf.HeaderBlockFragment()...)

	if !tf.HeadersEnded() {
		return nil
	}

	block := append([]byte(nil), st.headerBlock...)
	headers, err := s.decoder(dir).DecodeFull(st.headerBlock)
	st.headerBlock = nil
	if err != nil {
		return err
	}

	h := make(http.Header)
	for _, hf := range headers {
		h.Add(hf.Name, hf.Value)
	}

	if st.headers == nil {
		st.headers = h
	} else {
		for k, vs := range h {
			st.headers[k] = append(st.headers[k], vs...)
		}
	}

	if ct := st.headers.Get("content-type"); isGrpcContentType(ct) {
		st.isGrpc = true
	}

	streamID := tf.StreamID

	if s.hasH2Handlers() {
		outHeaders, _, err := s.invokeH2HeaderHook(st, streamID, dir, false, st.headers)
		if err != nil {
			return err
		}
		block, err = encodeHeaderBlock(outHeaders)
		if err != nil {
			return err
		}
		st.headers = outHeaders
	}

	return writeContinuationBlock(fr, streamID, block, s.maxFrameSize)
}

func (s *h2Session) handleData(fr *http2.Framer, tf *http2.DataFrame, dir H2Direction) error {
	st := s.streamState(tf.StreamID)
	data := tf.Data()
	endStream := tf.StreamEnded()

	if st.isGrpc && s.proxy != nil && len(s.proxy.grpcStreamHandlers) > 0 {
		stream := s.h2Stream(tf.StreamID, st, dir, data, endStream)
		s.populateH2Ctx(stream)
		out, _, err := processGrpcData(data, st, s.proxy, s.ctx, stream, endStream)
		if err != nil {
			return err
		}
		if out == nil {
			if endStream {
				s.deleteStream(tf.StreamID)
				return fr.WriteData(tf.StreamID, true, nil)
			}
			return nil
		}
		data = out
	}

	if s.hasH2Handlers() {
		out, err := s.invokeH2DataHook(st, tf.StreamID, dir, endStream, data)
		if err != nil {
			return err
		}
		data = out
	}

	terr := fr.WriteData(tf.StreamID, endStream, data)
	if endStream {
		s.deleteStream(tf.StreamID)
		if terr == nil {
			terr = io.EOF
		}
	}
	return terr
}

func (s *h2Session) hasH2Handlers() bool {
	return s.proxy != nil && len(s.proxy.h2StreamHandlers) > 0
}

func (s *h2Session) populateH2Ctx(stream *H2Stream) {
	if s.ctx == nil {
		return
	}
	s.ctx.H2Stream = stream
	s.ctx.H2StreamID = stream.ID
	s.ctx.H2Direction = stream.Direction()
	if stream.Headers != nil {
		s.ctx.H2Headers = stream.Headers.Clone()
	}
	if stream.IsGrpc {
		s.ctx.GrpcMethod = stream.GrpcMethod()
	}
}

func (s *h2Session) invokeH2HeaderHook(st *h2StreamState, streamID uint32, dir H2Direction, endStream bool, headers http.Header) (http.Header, bool, error) {
	stream := s.h2Stream(streamID, st, dir, nil, endStream)
	s.populateH2Ctx(stream)

	out, err := s.proxy.filterH2Stream(stream, s.ctx)
	if err != nil {
		return nil, false, err
	}
	if out == nil {
		return headers, endStream, nil
	}
	if out.Headers == nil {
		return headers, out.EndStream, nil
	}
	return out.Headers, out.EndStream, nil
}

func (s *h2Session) invokeH2DataHook(st *h2StreamState, streamID uint32, dir H2Direction, endStream bool, data []byte) ([]byte, error) {
	stream := s.h2Stream(streamID, st, dir, data, endStream)
	s.populateH2Ctx(stream)

	out, err := s.proxy.filterH2Stream(stream, s.ctx)
	if err != nil {
		return nil, err
	}
	if out == nil || out.Data == nil {
		return data, nil
	}
	return out.Data, nil
}

func encodeHeaderBlock(headers http.Header) ([]byte, error) {
	var buf bytes.Buffer
	enc := hpack.NewEncoder(&buf)
	for name, vals := range headers {
		for _, val := range vals {
			if err := enc.WriteField(hpack.HeaderField{Name: name, Value: val}); err != nil {
				return nil, err
			}
		}
	}
	return buf.Bytes(), nil
}

func writeHeaderBlock(fr *http2.Framer, streamID uint32, block []byte, endStream bool, priority http2.PriorityParam, maxFrameSize uint32) error {
	firstMax := int(maxFrameSize) - 10
	if firstMax < 1 {
		firstMax = 1
	}

	if len(block) <= firstMax {
		return fr.WriteHeaders(http2.HeadersFrameParam{
			StreamID:      streamID,
			BlockFragment: block,
			EndStream:     endStream,
			EndHeaders:    true,
			PadLength:     0,
			Priority:      priority,
		})
	}

	first := block[:firstMax]
	if err := fr.WriteHeaders(http2.HeadersFrameParam{
		StreamID:      streamID,
		BlockFragment: first,
		EndStream:     endStream,
		EndHeaders:    false,
		PadLength:     0,
		Priority:      priority,
	}); err != nil {
		return err
	}
	return writeContinuationBlock(fr, streamID, block[firstMax:], maxFrameSize)
}

func writeContinuationBlock(fr *http2.Framer, streamID uint32, block []byte, maxFrameSize uint32) error {
	contMax := int(maxFrameSize) - 10
	if contMax < 1 {
		contMax = 1
	}

	for len(block) > 0 {
		n := len(block)
		if n > contMax {
			n = contMax
		}
		endHeaders := n == len(block)
		if err := fr.WriteContinuation(streamID, endHeaders, block[:n]); err != nil {
			return err
		}
		block = block[n:]
	}
	return nil
}
