package cache

import (
	"encoding/json"
	"fmt"
	"github.com/francoispqt/gojay"
)

// decodeStringValue scans one JSON token without modifying the cached row.
// GoJay's in-place escape removal repeatedly shifts the remaining buffer, which
// becomes quadratic for large escaped strings such as JSON stored in SQL text.
func decodeStringValue(decoder *gojay.Decoder) (*string, error) {
	if consumeNullToken(decoder) {
		return nil, nil
	}
	buffer := data(decoder)
	start, ok := nextTokenPosition(buffer, cursor(decoder))
	if !ok || buffer[start] != '"' {
		return nil, fmt.Errorf("expected cached JSON string")
	}
	for end := start + 1; end < len(buffer); end++ {
		if buffer[end] == '\\' {
			end++
			continue
		}
		if buffer[end] != '"' {
			continue
		}
		end++
		if end < len(buffer) {
			switch buffer[end] {
			case ' ', '\n', '\r', '\t', ',', ']', '}':
			default:
				return nil, fmt.Errorf("invalid cached string delimiter")
			}
		}
		var value string
		if err := json.Unmarshal(buffer[start:end], &value); err != nil {
			return nil, err
		}
		setCursor(decoder, end)
		return &value, nil
	}
	return nil, fmt.Errorf("unterminated cached JSON string")
}
