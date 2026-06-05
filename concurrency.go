// SPDX-FileCopyrightText: 2025 Nimble Tech
// SPDX-License-Identifier: MIT

package outbox

import "context"

// orDone wraps a channel so that ranging over it stops as soon as the context
// is canceled, draining the caller from a blocked receive without leaking the
// producing goroutine's reader. It replaces an external concurrency helper to
// keep the package free of extra dependencies.
func orDone[T any](ctx context.Context, in <-chan T) <-chan T {
	out := make(chan T)

	go func() {
		defer close(out)

		for {
			select {
			case <-ctx.Done():
				return
			case v, ok := <-in:
				if !ok {
					return
				}

				select {
				case out <- v:
				case <-ctx.Done():
					return
				}
			}
		}
	}()

	return out
}
