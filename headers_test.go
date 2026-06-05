// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

package outbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeHeaders(t *testing.T) {
	t.Parallel()

	t.Run("empty is null", func(t *testing.T) {
		t.Parallel()

		got, err := encodeHeaders(nil)
		require.NoError(t, err)
		assert.Nil(t, got)
	})

	t.Run("non-empty is json string", func(t *testing.T) {
		t.Parallel()

		got, err := encodeHeaders(map[string]any{"trace_id": "abc"})
		require.NoError(t, err)
		assert.JSONEq(t, `{"trace_id":"abc"}`, got.(string))
	})
}

func TestDecodeHeaders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		src     any
		want    map[string]any
		wantErr bool
	}{
		{name: "nil", src: nil, want: nil},
		{name: "empty bytes", src: []byte{}, want: nil},
		{name: "string", src: `{"k":"v"}`, want: map[string]any{"k": "v"}},
		{name: "bytes", src: []byte(`{"k":"v"}`), want: map[string]any{"k": "v"}},
		{name: "bad json", src: `{`, wantErr: true},
		{name: "unsupported type", src: 42, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := decodeHeaders(tt.src)
			if tt.wantErr {
				assert.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestHeadersRoundTrip(t *testing.T) {
	t.Parallel()

	original := map[string]any{"trace_id": "t-1", "retries": float64(2)}

	encoded, err := encodeHeaders(original)
	require.NoError(t, err)

	decoded, err := decodeHeaders(encoded)
	require.NoError(t, err)
	assert.Equal(t, original, decoded)
}
