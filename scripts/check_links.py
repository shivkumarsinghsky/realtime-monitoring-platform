#!/usr/bin/env python3
"""Fail if a Markdown file links to a relative path (or #anchor) that does not exist.

External http(s) links are not fetched; this keeps CI deterministic and offline.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(sys.argv[1] if len(sys.argv) > 1 else ".").resolve()
LINK = re.compile(r"(?<!!)\[[^\]]*\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
HEADING = re.compile(r"^#{1,6}\s+(.*)$")
SKIP_DIRS = {".git", "node_modules", ".venv", "venv", "dist", "build", ".pytest_cache"}


def slugify(heading: str) -> str:
    """Approximation of GitHub's heading anchor algorithm."""
    text = re.sub(r"`([^`]*)`", r"\1", heading.strip()).lower()
    text = re.sub(r"[^\w\- ]", "", text)
    return text.replace(" ", "-")


def anchors(path: Path) -> set[str]:
    result: set[str] = set()
    counts: dict[str, int] = {}
    in_code = False
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.lstrip().startswith("```"):
            in_code = not in_code
            continue
        m = HEADING.match(line)
        if m and not in_code:
            slug = slugify(m.group(1))
            n = counts.get(slug, 0)
            result.add(slug if n == 0 else f"{slug}-{n}")
            counts[slug] = n + 1
    return result


def md_files() -> list[Path]:
    return [p for p in ROOT.rglob("*.md") if not any(part in SKIP_DIRS for part in p.parts)]


def main() -> int:
    errors: list[str] = []
    for md in md_files():
        in_code = False
        for lineno, line in enumerate(md.read_text(encoding="utf-8").splitlines(), 1):
            if line.lstrip().startswith("```"):
                in_code = not in_code
            if in_code:
                continue
            for target in LINK.findall(line):
                if re.match(r"^[a-z]+:", target):
                    continue
                path_part, _, anchor = target.partition("#")
                dest = (md.parent / path_part).resolve() if path_part else md
                if not dest.exists():
                    errors.append(f"{md.relative_to(ROOT)}:{lineno}: missing {target}")
                elif anchor and dest.suffix == ".md" and anchor not in anchors(dest):
                    errors.append(f"{md.relative_to(ROOT)}:{lineno}: missing anchor {target}")
    for e in errors:
        print(e)
    print(f"checked {len(md_files())} markdown files, {len(errors)} broken links")
    return 1 if errors else 0


if __name__ == "__main__":
    raise SystemExit(main())
