#!/usr/bin/env python3
"""Verify the exact contents and hashes of an offline wheelhouse."""

from __future__ import annotations

import hashlib
import re
import sys
from pathlib import Path


MANIFEST_NAME = "SHA256SUMS"
MANIFEST_LINE = re.compile(
    r"^(?P<digest>[0-9a-fA-F]{64})  (?P<name>[A-Za-z0-9][A-Za-z0-9._+~-]*\.whl)$"
)


def verify_wheelhouse(root: Path) -> None:
    if not root.is_dir() or root.is_symlink():
        raise ValueError(f"wheelhouse is not a regular directory: {root}")

    manifest = root / MANIFEST_NAME
    if manifest.is_symlink() or not manifest.is_file():
        raise ValueError("wheelhouse SHA256SUMS is missing or is not a regular file")

    entries: dict[str, str] = {}
    try:
        lines = manifest.read_text(encoding="utf-8").splitlines()
    except (OSError, UnicodeError) as exc:
        raise ValueError("wheelhouse SHA256SUMS cannot be read") from exc

    if not lines:
        raise ValueError("wheelhouse SHA256SUMS is empty")
    for line in lines:
        match = MANIFEST_LINE.fullmatch(line)
        if match is None:
            raise ValueError("wheelhouse SHA256SUMS contains a malformed entry")
        name = match.group("name")
        if name in entries:
            raise ValueError(f"wheelhouse SHA256SUMS contains a duplicate entry: {name}")
        entries[name] = match.group("digest").lower()

    children = list(root.iterdir())
    if any(child.is_symlink() for child in children):
        raise ValueError("wheelhouse must not contain symbolic links")
    unexpected = sorted(
        child.name
        for child in children
        if child.name != MANIFEST_NAME and not child.is_file()
    )
    if unexpected:
        raise ValueError(f"wheelhouse contains unexpected non-file entries: {', '.join(unexpected)}")

    actual = sorted(child.name for child in children if child.name != MANIFEST_NAME)
    expected = sorted(entries)
    if actual != expected:
        missing = sorted(set(expected) - set(actual))
        extra = sorted(set(actual) - set(expected))
        details = []
        if missing:
            details.append(f"missing={','.join(missing)}")
        if extra:
            details.append(f"unlisted={','.join(extra)}")
        raise ValueError("wheelhouse files do not exactly match SHA256SUMS (" + "; ".join(details) + ")")

    for name, expected_digest in entries.items():
        digest = hashlib.sha256()
        try:
            with (root / name).open("rb") as handle:
                for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                    digest.update(chunk)
        except OSError as exc:
            raise ValueError(f"wheelhouse wheel cannot be read: {name}") from exc
        if digest.hexdigest() != expected_digest:
            raise ValueError(f"wheelhouse SHA256 mismatch: {name}")


def main(argv: list[str] | None = None) -> int:
    args = list(argv if argv is not None else sys.argv[1:])
    if len(args) != 1:
        print(f"usage: {Path(sys.argv[0]).name} WHEELHOUSE", file=sys.stderr)
        return 2
    try:
        verify_wheelhouse(Path(args[0]))
    except ValueError as exc:
        print(f"wheelhouse: {exc}", file=sys.stderr)
        return 1
    print("Wheelhouse manifest and SHA256 hashes verified.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
