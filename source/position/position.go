package position

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	sdk "github.com/conduitio/conduit-connector-sdk"
)

const (
	TypeSnapshot Type = iota
	TypeCDC
)

const (
	snapshotPrefixChar = 's'
	cdcPrefixChar      = 'c'
)

type Type int

type Position struct {
	Key       string
	Timestamp time.Time
	Type      Type
}

func (p Position) ToRecordPosition() sdk.Position {
	char := snapshotPrefixChar
	if p.Type == TypeCDC {
		char = cdcPrefixChar
	}
	return []byte(fmt.Sprintf("%s_%c%d", p.Key, char, p.Timestamp.UnixMilli()))
}

func ParseRecordPosition(p sdk.Position) (Position, error) {
	if p == nil {
		// empty Position would have the fields with their default values
		return Position{}, nil
	}
	s := string(p)
	index := strings.LastIndex(s, "_")
	if index == -1 {
		return Position{}, errors.New("invalid position format, no '_' found")
	}
	milliseconds, err := strconv.ParseInt(s[index+2:], 10, 64)
	if err != nil {
		return Position{}, fmt.Errorf("could not parse the position timestamp: %w", err)
	}

	if s[index+1] != cdcPrefixChar && s[index+1] != snapshotPrefixChar {
		return Position{}, fmt.Errorf("invalid position format, no '%c' or '%c' after '_'", snapshotPrefixChar, cdcPrefixChar)
	}
	pType := TypeSnapshot
	if s[index+1] == cdcPrefixChar {
		pType = TypeCDC
	}

	return Position{
		Key:       s[:index],
		Timestamp: time.UnixMilli(milliseconds),
		Type:      pType,
	}, err
}

func ConvertToCDCPosition(p sdk.Position) (sdk.Position, error) {
	cdcPos, err := ParseRecordPosition(p)
	if err != nil {
		return sdk.Position{}, err
	}
	cdcPos.Type = TypeCDC
	return cdcPos.ToRecordPosition(), nil
}
