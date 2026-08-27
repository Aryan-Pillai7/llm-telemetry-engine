#!/usr/bin/env python3
"""Verify that everything the documentation points at actually exists.

Two things rot silently and identically: a markdown link to a file that moved,
and a Mermaid `click` target that used to be a module. Both keep rendering
perfectly. The link is just dead, and nobody notices until someone follows it.

This is the documentation-shaped version of the check this project keeps
applying elsewhere: the mechanism (the diagram, the link) still runs and still
looks right while pointing at nothing.

Deliberately dependency-free — it runs in the same CI job as the lint and guard
checks, with no Node and no browser. It validates *targets*, not Mermaid
grammar; grammar is validated against the real parser when a diagram changes.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

# Local-only files, deliberately gitignored.
SKIP_FILES = {"CLAUDE.md", "plan.md", "decisions.md"}

LINK_RE = re.compile(r"\[([^\]]+)\]\(([^)\s]+)(?:\s+\"[^\"]*\")?\)")
MERMAID_RE = re.compile(r"```mermaid\n(.*?)```", re.DOTALL)
CLICK_RE = re.compile(r'^\s*click\s+\S+\s+"([^"]+)"', re.MULTILINE)


def markdown_files() -> list[Path]:
    files = sorted(REPO_ROOT.glob("*.md")) + sorted(REPO_ROOT.glob("docs/*.md"))
    return [f for f in files if f.name not in SKIP_FILES]


def check_links(path: Path) -> list[str]:
    problems: list[str] = []
    for match in LINK_RE.finditer(path.read_text(encoding="utf-8")):
        target = match.group(2)
        if target.startswith(("http://", "https://", "#", "mailto:")):
            continue
        resolved = (path.parent / target.split("#")[0]).resolve()
        if not resolved.exists():
            problems.append(f"{path.name}: link [{match.group(1)}] -> {target}")
    return problems


def check_mermaid_clicks(path: Path) -> list[str]:
    """Every clickable diagram node must point at a file that exists."""
    problems: list[str] = []
    for block in MERMAID_RE.findall(path.read_text(encoding="utf-8")):
        for target in CLICK_RE.findall(block):
            if target.startswith(("http://", "https://")):
                continue
            if not (REPO_ROOT / target).exists():
                problems.append(f"{path.name}: mermaid click -> {target}")
    return problems


def check_fences(path: Path) -> list[str]:
    """An unbalanced fence swallows the rest of the document into a code block."""
    text = path.read_text(encoding="utf-8")
    if text.count("```") % 2 != 0:
        return [f"{path.name}: unbalanced ``` fence"]
    return []


def main() -> int:
    problems: list[str] = []
    files = markdown_files()

    for path in files:
        problems += check_fences(path)
        problems += check_links(path)
        problems += check_mermaid_clicks(path)

    clicks = sum(
        len(CLICK_RE.findall(block))
        for path in files
        for block in MERMAID_RE.findall(path.read_text(encoding="utf-8"))
    )
    print(f"checked {len(files)} markdown file(s), {clicks} clickable diagram node(s)")

    if problems:
        print(f"\n{len(problems)} broken reference(s):")
        for problem in problems:
            print(f"  {problem}")
        return 1

    print("every link and diagram target resolves")
    return 0


if __name__ == "__main__":
    sys.exit(main())
