package producer

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
)

type OpCode uint8
type StreamId uint16
type Flags uint8
type ErrorCode uint8

// The only responses with body are errors, leave 511 for the error message
const ResponseBodyMaxLength = 512

var Endianness = binary.BigEndian

// Operation codes.
// Use fixed numbers (not iota) to make it harder to break the protocol by moving stuff around.
const (
	StartupOp         OpCode = 1
	ReadyOp           OpCode = 2
	ErrorOp           OpCode = 3
	ProduceOp         OpCode = 4
	ProduceResponseOp OpCode = 5
	HeartbeatOp       OpCode = 6
)

// Flags.
// Use fixed numbers (not iota) to make it harder to break the protocol by moving stuff around.
const (
	WithTimestamp Flags = 0b00000001
)

const (
	ServerError         ErrorCode = 0
	RoutingError        ErrorCode = 1
	LeaderNotFoundError ErrorCode = 2
	ClientError         ErrorCode = 255
)

// Header for producer messages. Order of fields defines the serialization format.
type BinaryHeader struct {
	Version    uint8
	Flags      Flags
	StreamId   StreamId
	Op         OpCode
	BodyLength uint32
	Crc        uint32
}

var HeaderSize = binarySize(BinaryHeader{})

type BinaryRequest interface {
	Marshal(w *bytes.Buffer) error
	BodyLength() int

	// Sets the stream id in a thread safe manner
	SetStreamId(id StreamId)

	// Gets the stream id in a thread safe manner
	StreamId() *StreamId
}

type BinaryResponse interface {
	Op() OpCode
}

// Represents a response without body
func NewEmptyResponse(op OpCode) BinaryResponse {
	return &emptyResponse{op}
}

type emptyResponse struct {
	op OpCode
}

func (r *emptyResponse) Op() OpCode {
	return r.op
}

type ErrorResponse struct {
	Code    ErrorCode
	Message string
}

func NewClientErrorResponse(message string) BinaryResponse {
	return &ErrorResponse{
		Code:    ClientError,
		Message: message,
	}
}

func (r *ErrorResponse) Op() OpCode {
	return ErrorOp
}

func (r *ErrorResponse) ToError() error {
	return fmt.Errorf("Error response %d: %s", r.Code, r.Message)
}

func binarySize(v interface{}) int {
	size := binary.Size(v)
	if size <= 0 {
		panic(fmt.Sprintf("Size of type %v could not be determined", v))
	}
	return size
}

func WriteHeader(w *bytes.Buffer, header *BinaryHeader) error {
	if err := binary.Write(w, Endianness, header); err != nil {
		return err
	}

	const crcByteSize = 4
	buf := w.Bytes()
	headerBuf := buf[len(buf)-HeaderSize:]
	crc := crc32.ChecksumIEEE(headerBuf[:len(headerBuf)-crcByteSize])
	Endianness.PutUint32(headerBuf[len(headerBuf)-crcByteSize:], crc)
	return nil
}

type ProduceRequest struct {
}

func (r *ProduceRequest) Marshal(w *bytes.Buffer) error {
	// TODO: IMPLEMENT
	return nil
}

func (r *ProduceRequest) BodyLength() int {
	// TODO: IMPLEMENT
	return 0
}

func (r *ProduceRequest) SetStreamId(id StreamId) {
	// TODO: IMPLEMENT
}

func (r *ProduceRequest) StreamId() *StreamId {
	// TODO: IMPLEMENT
	return nil
}