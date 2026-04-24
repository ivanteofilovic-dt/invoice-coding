"""Tests for GL TSV filtering and iteration."""

from __future__ import annotations

from pathlib import Path

import pytest

from invoice_processing.bq_gl_lines import (
    GL_EXPORT_COLUMNS,
    decode_gl_export_bytes,
    gl_row_passes_filters,
    iter_filtered_gl_rows,
)


def _header_line() -> str:
    return "\t".join(GL_EXPORT_COLUMNS)


def _row_cells(**kwargs: str) -> str:
    cells = [kwargs.get(c, "") for c in GL_EXPORT_COLUMNS]
    return "\t".join(cells)


def test_gl_row_passes_filters_keeps_when_supplier_number_only() -> None:
    row = {
        "SUPPLIER_NUMBER": "123",
        "SUPPLIER_CUSTMER_NAME": "",
        "GL_LINE_DESCRIPTION": "normal text",
    }
    assert gl_row_passes_filters(row) is True


def test_gl_row_passes_filters_keeps_when_customer_name_only() -> None:
    row = {
        "SUPPLIER_NUMBER": "",
        "SUPPLIER_CUSTMER_NAME": "Acme AB",
        "GL_LINE_DESCRIPTION": "invoice line",
    }
    assert gl_row_passes_filters(row) is True


def test_gl_row_passes_filters_keeps_when_whitespace_trimmed_supplier() -> None:
    row = {
        "SUPPLIER_NUMBER": "  99  ",
        "SUPPLIER_CUSTMER_NAME": "",
        "GL_LINE_DESCRIPTION": "x",
    }
    assert gl_row_passes_filters(row) is True


def test_gl_row_passes_filters_drops_when_both_suppliers_empty() -> None:
    row = {
        "SUPPLIER_NUMBER": "",
        "SUPPLIER_CUSTMER_NAME": "   ",
        "GL_LINE_DESCRIPTION": "has description",
    }
    assert gl_row_passes_filters(row) is False


def test_gl_row_passes_filters_drops_when_both_missing() -> None:
    row: dict[str, str | None] = {"GL_LINE_DESCRIPTION": "x"}
    assert gl_row_passes_filters(row) is False


def test_gl_row_passes_filters_drops_ankreg_case_insensitive() -> None:
    row = {
        "SUPPLIER_NUMBER": "1",
        "SUPPLIER_CUSTMER_NAME": "",
        "GL_LINE_DESCRIPTION": "223488658 / AnKrEg VAT 202602",
    }
    assert gl_row_passes_filters(row) is False


def test_gl_row_passes_filters_drops_account_starting_with_24() -> None:
    row = {
        "SUPPLIER_NUMBER": "1",
        "SUPPLIER_CUSTMER_NAME": "",
        "ACCOUNT": "24001",
        "GL_LINE_DESCRIPTION": "ok",
    }
    assert gl_row_passes_filters(row) is False


def test_gl_row_passes_filters_keeps_account_not_in_24_range() -> None:
    row = {
        "SUPPLIER_NUMBER": "1",
        "SUPPLIER_CUSTMER_NAME": "",
        "ACCOUNT": "62301",
        "GL_LINE_DESCRIPTION": "ok",
    }
    assert gl_row_passes_filters(row) is True


def test_gl_row_passes_filters_ankreg_substring() -> None:
    row = {
        "SUPPLIER_NUMBER": "1",
        "SUPPLIER_CUSTMER_NAME": "",
        "GL_LINE_DESCRIPTION": "prefix ankreg suffix",
    }
    assert gl_row_passes_filters(row) is False


def test_decode_gl_export_bytes_utf8() -> None:
    assert decode_gl_export_bytes("hello".encode()) == "hello"


def test_decode_gl_export_bytes_latin1_byte_f6() -> None:
    # 0xf6 is invalid as a lone UTF-8 start byte; valid in cp1252 / Latin-1 as "ö".
    s = decode_gl_export_bytes(b"prefix\xf6suffix")
    assert "ö" in s
    assert s == "prefixösuffix"


def test_decode_gl_export_bytes_preferred_strict() -> None:
    assert decode_gl_export_bytes("x".encode("utf-8"), preferred="utf-8") == "x"


def test_iter_filtered_gl_rows_latin1_supplier_field(tmp_path: Path) -> None:
    p = tmp_path / "latin1.txt"
    header = _header_line()
    body = _row_cells(
        SUPPLIER_NUMBER="S1",
        SUPPLIER_CUSTMER_NAME="",
        GL_LINE_DESCRIPTION="ok",
    )
    # "ö" in GL_LINE_DESCRIPTION as Latin-1 bytes
    body_cells = body.split("\t")
    idx = GL_EXPORT_COLUMNS.index("GL_LINE_DESCRIPTION")
    body_cells[idx] = "Br\xf6d"  # Bröd
    body = "\t".join(body_cells)
    p.write_bytes((header + "\n" + body + "\n").encode("iso8859-1"))

    rows = list(iter_filtered_gl_rows(p))
    assert len(rows) == 1
    assert rows[0]["GL_LINE_DESCRIPTION"] == "Bröd"


def test_iter_filtered_gl_rows_end_to_end(tmp_path: Path) -> None:
    p = tmp_path / "GL_202601.txt"
    content = "\n".join(
        [
            _header_line(),
            _row_cells(
                SUPPLIER_NUMBER="",
                SUPPLIER_CUSTMER_NAME="",
                GL_LINE_DESCRIPTION="skip no supplier",
            ),
            _row_cells(
                SUPPLIER_NUMBER="S1",
                SUPPLIER_CUSTMER_NAME="",
                GL_LINE_DESCRIPTION="keep me",
            ),
            _row_cells(
                SUPPLIER_NUMBER="S2",
                SUPPLIER_CUSTMER_NAME="",
                GL_LINE_DESCRIPTION="has ANKREG in text",
            ),
            _row_cells(
                SUPPLIER_NUMBER="",
                SUPPLIER_CUSTMER_NAME="Vendor",
                GL_LINE_DESCRIPTION="also keep",
            ),
        ]
    )
    p.write_text(content, encoding="utf-8")

    rows = list(iter_filtered_gl_rows(p))
    assert len(rows) == 2
    assert rows[0]["SUPPLIER_NUMBER"] == "S1"
    assert rows[0]["GL_LINE_DESCRIPTION"] == "keep me"
    assert rows[0]["source_file"] == "GL_202601.txt"
    assert "UTC" in rows[0]["loaded_at"]
    assert rows[1]["SUPPLIER_CUSTMER_NAME"] == "Vendor"
    assert rows[1]["GL_LINE_DESCRIPTION"] == "also keep"


@pytest.mark.parametrize(
    "desc",
    ["", None],
    ids=["empty", "none"],
)
def test_gl_row_passes_filters_supplier_ok_empty_description(desc: str | None) -> None:
    row = {
        "SUPPLIER_NUMBER": "1",
        "SUPPLIER_CUSTMER_NAME": "",
        "GL_LINE_DESCRIPTION": desc,
    }
    assert gl_row_passes_filters(row) is True
