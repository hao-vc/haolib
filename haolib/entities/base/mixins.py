"""Entities base."""

from datetime import datetime


class DateTimeEntity:
    """DateTime mixin."""

    created_at: datetime
    updated_at: datetime
