"""Shared period-label conversion used across the pipeline and the test-DB builder."""


def period_index(period: int) -> int:
    """Convert a CANOE period label into the value stored in the database."""
    return period - 5
