#!/usr/bin/env python3
"""Find guards that are defined but never called from the path they protect.

This is a distinct failure mode from the one this project keeps catching. A
check that examines the wrong layer at least runs, and its output can be
compared against reality. A check that is never invoked produces *no output at
all* — there is nothing to notice, nothing to contradict, and every test around
it still passes. Runtime testing cannot find it. Only reading the code can, so
that reading is automated here.

Precedent: `_source_fingerprint` in the cold-tier exporter was written to catch
"row counts match but the values are misaligned", complete with a docstring
explaining why counts alone are insufficient — and was not called from
`export_window`. The exporter verified counts only, exactly the gap the function
existed to close, and every test passed.

What this reports:

  * guard-shaped functions (verify/validate/check/assert/ensure/require/guard)
    that nothing outside their own module ever references;
  * guard-shaped functions referenced *only* by tests, which means the
    production path does not use them — a check that only the test suite calls
    is a check that never protects anything in production.

Neither is automatically a bug: a guard may legitimately be public API, or be
invoked through the CLI by name. The point is that each one gets looked at
deliberately rather than assumed live.
"""

from __future__ import annotations

import ast
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
SRC = REPO_ROOT / "src"
TESTS = REPO_ROOT / "tests"
SCRIPTS = REPO_ROOT / "scripts"

# Names that announce an intent to protect something.
GUARD_PREFIXES = (
    "verify",
    "validate",
    "check",
    "assert",
    "ensure",
    "require",
    "guard",
    "_verify",
    "_validate",
    "_check",
    "_assert",
    "_ensure",
    "_require",
)
GUARD_SUBSTRINGS = ("fingerprint", "sanity", "invariant", "consistency")

# Guards whose non-production status has been reviewed and is deliberate. The
# reason lives here rather than in a reviewer's memory, and adding an entry is a
# visible act in a diff. A guard that is merely inconvenient does not belong on
# this list -- the point of the tool is that the decision gets made once,
# explicitly, instead of by omission.
REVIEWED_TEST_ONLY: dict[str, str] = {
    "assert_dimensions_are_disjoint": (
        "Invariant over module constants; can only break by editing that file, "
        "so a test run is the earliest possible check. Asserting at import time "
        "would cost every process start and vanish under python -O."
    ),
}


@dataclass
class Definition:
    name: str
    module: str
    lineno: int
    doc_first_line: str = ""


@dataclass
class Report:
    never_referenced: list[Definition] = field(default_factory=list)
    only_tests: list[Definition] = field(default_factory=list)
    reviewed: list[Definition] = field(default_factory=list)

    @property
    def total(self) -> int:
        return len(self.never_referenced) + len(self.only_tests)


def _is_guard_shaped(name: str) -> bool:
    lowered = name.lower()
    return lowered.startswith(GUARD_PREFIXES) or any(s in lowered for s in GUARD_SUBSTRINGS)


def _python_files(*roots: Path) -> list[Path]:
    files: list[Path] = []
    for root in roots:
        if root.is_dir():
            files.extend(sorted(root.rglob("*.py")))
    return files


def _definitions(files: list[Path]) -> list[Definition]:
    found: list[Definition] = []
    for path in files:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef | ast.AsyncFunctionDef) and _is_guard_shaped(
                node.name
            ):
                doc = ast.get_docstring(node) or ""
                found.append(
                    Definition(
                        name=node.name,
                        module=str(path.relative_to(REPO_ROOT)).replace("\\", "/"),
                        lineno=node.lineno,
                        doc_first_line=doc.strip().split("\n")[0][:70],
                    )
                )
    return found


def _references(files: list[Path]) -> dict[str, set[str]]:
    """Map each referenced name to the set of modules referencing it.

    Counts three things, all of which mean "this is live":

      * a call, by bare name or through an attribute;
      * a plain attribute or name load -- a guard exposed as a ``@property`` is
        read as ``obj.checksum``, never called, and counting only calls reported
        it as dead;
      * an import.

    Under-counting here is worse than over-counting: a detector that reports
    healthy code gets switched off, and then it is not there for the one real
    finding.
    """
    refs: dict[str, set[str]] = defaultdict(set)
    for path in files:
        module = str(path.relative_to(REPO_ROOT)).replace("\\", "/")
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in ast.walk(tree):
            if isinstance(node, ast.Call):
                target = node.func
                if isinstance(target, ast.Name):
                    refs[target.id].add(module)
                elif isinstance(target, ast.Attribute):
                    refs[target.attr].add(module)
            elif isinstance(node, ast.Attribute):
                refs[node.attr].add(module)
            elif isinstance(node, ast.Name):
                refs[node.id].add(module)
            elif isinstance(node, ast.ImportFrom):
                for alias in node.names:
                    refs[alias.name].add(module)
    return refs


def audit() -> Report:
    src_files = _python_files(SRC)
    all_files = _python_files(SRC, TESTS, SCRIPTS)

    definitions = _definitions(src_files)
    references = _references(all_files)
    report = Report()

    for definition in definitions:
        if definition.name in REVIEWED_TEST_ONLY:
            report.reviewed.append(definition)
            continue

        used_in = references.get(definition.name, set())
        # A guard invoked from elsewhere in its own module is live. Excluding
        # the defining module reported `validate()` and `assert_drained()` as
        # test-only when both are called from the production path a few lines
        # below their definition.
        used_in_production = {m for m in used_in if not m.startswith("tests/")}

        if not used_in_production:
            if used_in:
                report.only_tests.append(definition)
            else:
                report.never_referenced.append(definition)

    return report


def main() -> int:
    report = audit()

    if report.never_referenced:
        print("GUARDS NEVER CALLED ANYWHERE")
        print("  These produce no output, so nothing contradicts them and every test passes.")
        for d in report.never_referenced:
            print(f"    {d.module}:{d.lineno}  {d.name}()")
            if d.doc_first_line:
                print(f"        {d.doc_first_line}")
        print()

    if report.only_tests:
        print("GUARDS CALLED ONLY BY TESTS")
        print("  The production path does not invoke these, so they protect nothing at runtime.")
        for d in report.only_tests:
            print(f"    {d.module}:{d.lineno}  {d.name}()")
            if d.doc_first_line:
                print(f"        {d.doc_first_line}")
        print()

    if report.reviewed:
        print("REVIEWED AND DELIBERATE")
        for d in report.reviewed:
            print(f"    {d.module}:{d.lineno}  {d.name}()")
            print(f"        {REVIEWED_TEST_ONLY[d.name]}")
        print()

    if report.total == 0:
        print("No unreviewed inert guards.")
        return 0

    print(f"{report.total} guard(s) need review.")
    print("Each may be legitimate (public API, CLI-invoked). The point is to look.")
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
