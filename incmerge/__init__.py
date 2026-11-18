"""incmerge - idempotent hourly CSV incremental-table builder.

Public entry points live in :mod:`incmerge.merge`; the HTTP dashboard is in
:mod:`incmerge.web`.  Everything is backed by a single SQLite database so a
local sample run exercises the exact same code paths as the CLI.
"""

__version__ = "0.1.0"
