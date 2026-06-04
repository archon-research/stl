package aavelike_position_tracker

import "github.com/archon-research/stl/stl-verify/internal/ports/outbound"

// Option configures NewService.
type Option func(*serviceOptions)

type serviceOptions struct {
	multicallerWrap func(outbound.Multicaller) outbound.Multicaller
}

// WithMulticallerWrap installs a decorator around the internal Multicall3
// client. Used to plug in the SC-call archiver without leaking adapter
// imports into the service.
func WithMulticallerWrap(wrap func(outbound.Multicaller) outbound.Multicaller) Option {
	return func(o *serviceOptions) {
		o.multicallerWrap = wrap
	}
}
