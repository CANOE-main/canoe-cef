"""
Small helper to batch-upsert canoe_schema model instances.

canoe_schema.base.CanoeBaseModel provides bulk helpers for insert/insert-or-
ignore/replace, but not for upsert - this fills that one gap using the same
"build one parameterized template from the first row" pattern the package
itself uses in to_bulk_insert_sql / bulk_insert_or_ignore_sql.
"""

from __future__ import annotations

from typing import Any, Sequence

from canoe_schema.base import CanoeBaseModel


def bulk_upsert_sql(
    rows: Sequence[CanoeBaseModel],
    *,
    conflict_columns: Sequence[str],
    update_columns: Sequence[str] | None = None,
    include_nulls: bool = False,
    include_defaults: bool = True,
) -> tuple[str, list[tuple[Any, ...]]]:
    """Build one parameterized upsert statement + param list for a batch of
    same-shaped rows of the same model type."""
    if not rows:
        raise ValueError("rows must not be empty")

    row_type = type(rows[0])
    if not all(type(row) is row_type for row in rows):
        raise TypeError(
            f"All rows must be the same type, got: "
            f"{', '.join(sorted({type(r).__name__ for r in rows}))}"
        )

    template_sql, first_params = rows[0].to_upsert_sql(
        conflict_columns=conflict_columns,
        update_columns=update_columns,
        include_nulls=include_nulls,
        include_defaults=include_defaults,
    )
    params = [first_params]
    for row in rows[1:]:
        sql, row_params = row.to_upsert_sql(
            conflict_columns=conflict_columns,
            update_columns=update_columns,
            include_nulls=include_nulls,
            include_defaults=include_defaults,
        )
        if sql != template_sql:
            raise ValueError(
                "Rows produced different SQL. Use include_nulls/include_defaults "
                "consistently so all rows align."
            )
        params.append(row_params)

    return template_sql, params


def upsert_many(cursor, rows: Sequence[CanoeBaseModel], *, conflict_columns: Sequence[str] | None = None) -> None:
    """Upsert a batch of same-shaped rows, keyed by their own primary key by default."""
    if not rows:
        return
    if conflict_columns is None:
        conflict_columns = list(rows[0].__primary_key__)
    sql, params = bulk_upsert_sql(rows, conflict_columns=conflict_columns)
    cursor.executemany(sql, params)
