package serialization

import (
	"bytes"
	"encoding/binary"
	"io"
	"net/http"
	"time"

	. "github.com/barcostreams/go-client/types"
	"github.com/klauspost/compress/zstd"
)

var endianness = binary.BigEndian

const consumerContentType = "application/vnd.barco.consumermessage"

func unmarshalTopicRecords(r io.Reader) (*TopicRecords, error) {
	var err error
	item := &TopicRecords{}
	item.Topic, err = unmarshalTopicId(r)
	if err != nil {
		return nil, err
	}
	var startOffset int64
	if err = binary.Read(r, endianness, &startOffset); err != nil {
		return nil, err
	}
	payloadLength := int32(0)
	if err = binary.Read(r, endianness, &payloadLength); err != nil {
		return nil, err
	}

	payload := make([]byte, payloadLength)
	if _, err = io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	payloadReader, err := zstd.NewReader(bytes.NewReader(payload))
	if err != nil {
		return nil, err
	}

	// TODO: Buffer pooling
	uncompressed, err := io.ReadAll(payloadReader)
	if err != nil {
		return nil, err
	}

	recordsReader := bytes.NewReader(uncompressed)
	item.Records = make([]Record, 0)
	for recordsReader.Len() > 0 {
		record, err := unmarshalRecord(recordsReader, startOffset+int64(len(item.Records)))
		if err != nil {
			return nil, err
		}
		item.Records = append(item.Records, record)
	}

	return item, nil
}

func ReadOkResponse(resp *http.Response) ([]TopicRecords, error) {
	defer resp.Body.Close()
	var messageLength uint16
	if resp.ContentLength < 2 || resp.Header.Get("Content-Type") != consumerContentType {
		return nil, nil
	}
	binary.Read(resp.Body, endianness, &messageLength)
	result := make([]TopicRecords, 0)
	for i := 0; i < int(messageLength); i++ {
		item, err := unmarshalTopicRecords(resp.Body)
		if err != nil {
			return nil, err
		}
		result = append(result, *item)
	}
	return result, nil
}

func unmarshalTopicId(r io.Reader) (*TopicDataId, error) {
	topic := TopicDataId{}
	topicLength := uint8(0)
	if err := binary.Read(r, endianness, &topic.Token); err != nil {
		return nil, err
	}
	if err := binary.Read(r, endianness, &topic.RangeIndex); err != nil {
		return nil, err
	}
	if err := binary.Read(r, endianness, &topic.Version); err != nil {
		return nil, err
	}
	if err := binary.Read(r, endianness, &topicLength); err != nil {
		return nil, err
	}
	topicName := make([]byte, topicLength)
	n, err := r.Read(topicName)
	if err != nil {
		return nil, err
	}
	topic.Name = string(topicName[:n])
	return &topic, nil
}

func unmarshalRecord(r io.Reader, offset int64) (Record, error) {
	length := uint32(0)
	timestamp := int64(0)
	result := Record{}
	err := binary.Read(r, endianness, &timestamp)
	if err != nil {
		return result, err
	}

	result.Timestamp = time.UnixMicro(timestamp)
	result.Offset = offset

	err = binary.Read(r, endianness, &length)
	body := make([]byte, length)
	n, err := r.Read(body)
	if err != nil {
		return result, err
	}

	result.Body = body[:n]
	return result, nil
}
