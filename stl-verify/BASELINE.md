# Integration-test baseline marker (VEC-565)

Throwaway file. It exists only so `detect-changes` marks this branch as a Go change
and runs `go-ci`, giving three attempts on one exact commit for the pre-change
integration-test baseline. No Go source is touched, so the Go build cache stays warm
and the measurement reflects a normal PR.

Delete this file (and the branch) once the baseline is recorded on VEC-565.
