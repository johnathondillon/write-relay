package postgres

import (
	"fmt"

	"github.com/jackc/pglogrepl"
)

func DecodeWALData(data []byte) (pglogrepl.Message, error) {
	message, err := pglogrepl.Parse(data)
	if err != nil {
		return nil, fmt.Errorf("%w: decode pgoutput protocol-v1 message: %v", ErrProtocolState, err)
	}
	return message, nil
}
