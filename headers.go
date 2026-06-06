// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

package outbox

import (
	"encoding/json"
	"fmt"
)

// encodeHeaders serializes headers for storage in the headers column. An empty
// map is stored as NULL (nil) so untagged messages do not carry an empty JSON
// object. The value is returned as a JSON string, matching how payloads are
// passed to the database driver.
func encodeHeaders(headers map[string]any) (any, error) {
	if len(headers) == 0 {
		return nil, nil
	}

	encoded, err := json.Marshal(headers)
	if err != nil {
		return nil, fmt.Errorf("%w: marshal headers", err)
	}

	return string(encoded), nil
}

// decodeHeaders parses a headers column value back into a map. It tolerates the
// representations a SQL driver may return for a JSON column: nil, string or
// []byte.
func decodeHeaders(src any) (map[string]any, error) {
	var raw []byte

	switch v := src.(type) {
	case nil:
		return nil, nil
	case []byte:
		raw = v
	case string:
		raw = []byte(v)
	default:
		return nil, fmt.Errorf("unsupported headers column type %T", src)
	}

	if len(raw) == 0 {
		return nil, nil
	}

	headers := make(map[string]any)
	if err := json.Unmarshal(raw, &headers); err != nil {
		return nil, fmt.Errorf("%w: unmarshal headers", err)
	}

	return headers, nil
}
