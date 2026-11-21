package domain

import "errors"

// ErrPermanent marks an error that must not be retried; the job transitions straight to failed.
var ErrPermanent = errors.New("permanent error")

type permanentError struct{ err error }

func (e *permanentError) Error() string { return e.err.Error() }

func (e *permanentError) Unwrap() []error { return []error{e.err, ErrPermanent} }

// Permanent wraps err so that errors.Is(err, ErrPermanent) is true while preserving the original chain.
// Permanent(nil) returns nil.
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return &permanentError{err: err}
}

// IsPermanent reports whether err (or anything it wraps) is permanent.
func IsPermanent(err error) bool { return errors.Is(err, ErrPermanent) }
