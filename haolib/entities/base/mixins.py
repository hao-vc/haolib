"""Entities base."""

import abc
from datetime import datetime


class DateTimeEntity(abc.ABC):
    """DateTime mixin."""

    created_at: datetime
    updated_at: datetime
