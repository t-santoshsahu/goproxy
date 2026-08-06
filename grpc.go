package goproxy

import (
	"encoding/binary"
	"strings"
)

const grpcHeaderSize = 5

// isGrpcContentType reports whether ct is an application/grpc media type.
func isGrpcContentType(ct string) bool {
	ct = strings.TrimSpace(strings.ToLower(ct))
	if ct == "application/grpc" {
		return true
	}
	return strings.HasPrefix(ct, "application/grpc+")
}

// GrpcMessageHandler can inspect or modify individual gRPC messages on a stream.
type GrpcMessageHandler interface {
	HandleGrpcMessage(stream *H2Stream, msg []byte, endStream bool, ctx *ProxyCtx) ([]byte, error)
}

// FuncGrpcMessageHandler adapts a function to GrpcMessageHandler.
type FuncGrpcMessageHandler func(stream *H2Stream, msg []byte, endStream bool, ctx *ProxyCtx) ([]byte, error)

func (f FuncGrpcMessageHandler) HandleGrpcMessage(stream *H2Stream, msg []byte, endStream bool, ctx *ProxyCtx) ([]byte, error) {
	return f(stream, msg, endStream, ctx)
}

// processGrpcData deframes 5-byte gRPC messages, invokes handlers, and reframes output.
func processGrpcData(data []byte, state *h2StreamState, proxy *ProxyHttpServer, ctx *ProxyCtx, stream *H2Stream, endStream bool) ([]byte, bool, error) {
	if proxy == nil || len(proxy.grpcStreamHandlers) == 0 {
		return data, false, nil
	}

	state.grpcBuf = append(state.grpcBuf, data...)
	var out []byte
	modified := false

	for len(state.grpcBuf) >= grpcHeaderSize {
		compressed := state.grpcBuf[0]
		msgLen := binary.BigEndian.Uint32(state.grpcBuf[1:grpcHeaderSize])
		totalLen := grpcHeaderSize + int(msgLen)
		if len(state.grpcBuf) < totalLen {
			break
		}

		msg := make([]byte, msgLen)
		copy(msg, state.grpcBuf[grpcHeaderSize:totalLen])
		state.grpcBuf = state.grpcBuf[totalLen:]

		msgEndStream := endStream && len(state.grpcBuf) == 0
		newMsg, err := proxy.filterGrpcMessage(stream, msg, msgEndStream, ctx)
		if err != nil {
			return nil, false, err
		}
		if len(newMsg) != len(msg) || !bytesEqual(newMsg, msg) {
			modified = true
		}

		hdr := make([]byte, grpcHeaderSize)
		hdr[0] = compressed
		binary.BigEndian.PutUint32(hdr[1:grpcHeaderSize], uint32(len(newMsg)))
		out = append(out, hdr...)
		out = append(out, newMsg...)
	}

	if endStream && len(state.grpcBuf) > 0 {
		out = append(out, state.grpcBuf...)
		state.grpcBuf = nil
		modified = true
	}

	if len(out) == 0 {
		if len(state.grpcBuf) > 0 {
			return nil, false, nil
		}
		if !modified {
			return data, false, nil
		}
	}

	return out, modified || len(out) != len(data), nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
