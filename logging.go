// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

package outbox

import "github.com/rs/zerolog/log"

// component is attached to every log line emitted by this package so logs can be
// filtered by `component="outbox"` in Loki without relying on high-cardinality
// labels. The output format (JSON, sampling, level) stays the caller's choice.
const component = "outbox"

// logger is derived from the global zerolog logger so it inherits whatever the
// application configured, while always carrying the component field.
var logger = log.With().Str("component", component).Logger()
