#!/usr/bin/env bash
# Render every Mermaid block in every Markdown file; exit non-zero on any syntax error.
# Usage: scripts/check_mermaid.sh [root]
# Env:   MMDC (default: npx mermaid-cli), PUPPETEER_CONFIG (default: scripts/puppeteer-config.json)
set -uo pipefail
ROOT="${1:-.}"
HERE="$(cd "$(dirname "$0")" && pwd)"
MMDC="${MMDC:-npx -y @mermaid-js/mermaid-cli@12.0.0}"
CFG="${PUPPETEER_CONFIG:-$HERE/puppeteer-config.json}"
OUT="$(mktemp -d)"
checked=0; failed=0
while IFS= read -r file; do
  grep -q '```mermaid' "$file" || continue
  checked=$((checked + 1))
  if ! $MMDC -q -p "$CFG" -i "$file" -o "$OUT/out.md" >"$OUT/log" 2>&1; then
    echo "::error file=$file::invalid Mermaid diagram"
    grep -E "Error|Parse error|Expecting" "$OUT/log" | head -5
    failed=$((failed + 1))
  fi
done < <(find "$ROOT" -name '*.md' -not -path '*/node_modules/*' -not -path '*/.git/*' -not -path '*/.venv/*')
echo "mermaid: $checked files checked, $failed failed"
[ "$failed" -eq 0 ]
