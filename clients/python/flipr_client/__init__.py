# flipr-client -- the reference python client for flipr.
#
# A PACKAGE since 2026-09-01, and it was three loose copies before that. The
# Go client next door has been its own module from the start, the TypeScript
# one is a published package, and Python was the odd one out: kingfisher and
# peacock each held a hand-copied duplicate, peacock's header naming the
# kingfisher commit it was taken from. The three had drifted to 214, 175 and
# 143 lines, and the copy carrying the instruction "do not diverge here; fixes
# land in kingfisher first" was pointing at a repo that was not the source.
#
# Nothing here is new behaviour except the backoff. The point is that there is
# now one of it, with a version on it.
#
# WHY THE IMPORTS DID NOT CHANGE: consumers write `import flipr_client` and
# then `flipr_client.FliprClient(...)`. Re-exporting here keeps every one of
# those working unchanged, so adopting the package is a dependency line and a
# deletion, not an edit to call sites.

from ._client import FliprClient, FliprDown, FliprLocked, FliprRefused, FlagMissing, ConfigError, Policy, client_header, source_hash
from ._net import set_logger

__all__ = ["FliprClient", "FliprDown", "FliprLocked", "FliprRefused", "FlagMissing", "ConfigError", "Policy", "client_header", "source_hash", "set_logger"]

# Bumped when the WIRE BEHAVIOUR or the semantics change, not when a comment
# does. Consumers pin a git tag; see USING.md.
__version__ = "1.1.0"
