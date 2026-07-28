package postgres

import (
	"encoding/binary"
	"testing"

	"github.com/jackc/pglogrepl"
)

func TestDecodeLogicalMessage(t *testing.T) {
	content := []byte(`{"specversion":"1.0","id":"one","source":"urn:test","type":"created"}`)
	data := []byte{'M', 1}
	data = binary.BigEndian.AppendUint64(data, 0x1234)
	data = append(data, []byte("writerelay.v1")...)
	data = append(data, 0)
	data = binary.BigEndian.AppendUint32(data, uint32(len(content)))
	data = append(data, content...)

	message, err := DecodeWALData(data)
	if err != nil {
		t.Fatal(err)
	}
	logical, ok := message.(*pglogrepl.LogicalDecodingMessage)
	if !ok {
		t.Fatalf("decoded %T", message)
	}
	if !logical.Transactional || logical.LSN != 0x1234 || string(logical.Content) != string(content) {
		t.Fatalf("unexpected message: %#v", logical)
	}
}
