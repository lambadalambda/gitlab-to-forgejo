from __future__ import annotations

import gzip
from collections.abc import Iterator
from pathlib import Path
from typing import IO


class CopyParseError(ValueError):
    pass


def _open_text(path: Path) -> IO[str]:
    if path.suffix == ".gz":
        return gzip.open(path, "rt", encoding="utf-8", errors="replace")
    return path.open("r", encoding="utf-8", errors="replace")


def _parse_copy_header(line: str) -> tuple[str, list[str]]:
    # Example:
    #   COPY public.notes (note, noteable_type, ..., "position", ...) FROM stdin;
    if not line.startswith("COPY public."):
        raise CopyParseError(f"not a COPY header: {line!r}")

    prefix = "COPY public."
    rest = line[len(prefix) :]
    table = rest.split(" ", 1)[0]

    open_paren = line.find("(")
    close_paren = line.rfind(")")
    if open_paren == -1 or close_paren == -1 or close_paren < open_paren:
        raise CopyParseError(f"malformed COPY header (missing parens): {line!r}")

    cols_raw = line[open_paren + 1 : close_paren]
    columns = [c.strip().strip('"') for c in cols_raw.split(",")]
    if not columns or any(not c for c in columns):
        raise CopyParseError(f"malformed COPY header (empty column): {line!r}")
    return table, columns


def _decode_copy_field(value: str) -> str:
    # Postgres COPY FROM text format uses backslash escapes.
    out: list[str] = []
    i = 0
    while i < len(value):
        ch = value[i]
        if ch != "\\":
            out.append(ch)
            i += 1
            continue

        # Trailing backslash, keep as-is.
        if i + 1 >= len(value):
            out.append("\\")
            break

        nxt = value[i + 1]
        if nxt == "b":
            out.append("\b")
            i += 2
        elif nxt == "f":
            out.append("\f")
            i += 2
        elif nxt == "n":
            out.append("\n")
            i += 2
        elif nxt == "r":
            out.append("\r")
            i += 2
        elif nxt == "t":
            out.append("\t")
            i += 2
        elif nxt == "v":
            out.append("\v")
            i += 2
        elif nxt == "\\":
            out.append("\\")
            i += 2
        elif nxt == "x" and i + 3 < len(value):
            h1 = value[i + 2]
            h2 = value[i + 3]
            try:
                out.append(chr(int(h1 + h2, 16)))
                i += 4
            except ValueError:
                out.append(nxt)
                i += 2
        elif nxt.isdigit():
            # Octal escape: up to 3 digits, including nxt.
            j = i + 1
            digits = []
            while j < len(value) and len(digits) < 3 and value[j].isdigit():
                digits.append(value[j])
                j += 1
            try:
                out.append(chr(int("".join(digits), 8)))
                i = j
            except ValueError:
                out.append(nxt)
                i += 2
        else:
            # Unknown escape, drop backslash per COPY behavior.
            out.append(nxt)
            i += 2

    return "".join(out)


def iter_copy_rows(
    db_path: Path | str,
    *,
    tables: set[str] | None = None,
) -> Iterator[tuple[str, dict[str, str | None]]]:
    """
    Stream rows from a GitLab `database.sql(.gz)` dump.

    Yields (table_name, row_dict) for each row in each COPY block.
    `table_name` matches the unqualified name in `COPY public.<table>`.
    """
    path = Path(db_path)
    in_copy: str | None = None
    columns: list[str] | None = None
    capture = False

    with _open_text(path) as f:
        for line in f:
            if in_copy is None:
                if line.startswith("COPY public."):
                    tbl, cols = _parse_copy_header(line)
                    in_copy = tbl
                    columns = cols
                    capture = tables is None or tbl in tables
                continue

            # inside a COPY block
            if line.startswith("\\."):
                in_copy = None
                columns = None
                capture = False
                continue

            if not capture:
                continue

            assert columns is not None
            fields = line.rstrip("\n").split("\t")
            if len(fields) != len(columns):
                raise CopyParseError(
                    f"COPY row column mismatch for table={in_copy}: "
                    f"expected {len(columns)} fields, got {len(fields)}"
                )

            row: dict[str, str | None] = {}
            for col, raw in zip(columns, fields, strict=True):
                if raw == r"\N":
                    row[col] = None
                else:
                    row[col] = _decode_copy_field(raw)

            yield in_copy, row
