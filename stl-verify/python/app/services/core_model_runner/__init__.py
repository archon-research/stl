"""CORE model runner service — the body of one cronjob tick.

Intentionally re-exports nothing. Temporal's sandbox re-imports
`workflow.py`, which imports this package on the way; pulling `service` in
here would drag numpy into the sandbox and fail worker startup.
"""
