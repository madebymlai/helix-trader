#!/usr/bin/env bash
# Checks that internal links in markdown docs resolve on disk.
#
# Runs lychee in offline mode to catch broken relative paths, anchors, and
# cross-references without hitting the network. External URLs are ignored
# here; they're covered by the periodic `make check-docs-links` audit.
#
# If lychee is not installed, the hook exits 0 with a warning so that
# contributors without the cargo toolchain can still commit.

set -euo pipefail

YELLOW='\033[0;33m'
NC='\033[0m'

if ! command -v lychee > /dev/null 2>&1; then
  echo -e "${YELLOW}Warning:${NC} lychee not found, skipping offline link check" >&2
  echo "  Install with: cargo install lychee --locked" >&2
  exit 0
fi

repo_root=$(git rev-parse --show-toplevel)
cd "$repo_root"

# Check every tracked input because an edited target can break links from unchanged files
git ls-files -- '*.md' ':(exclude)crates/lse/docs-snapshot/**' |
  lychee \
    --no-progress \
    --offline \
    --include-fragments \
    --root-dir "$repo_root" \
    --fallback-extensions md \
    --exclude-path target \
    --files-from -
