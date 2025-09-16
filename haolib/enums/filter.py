"""Filter types."""

from haolib.enums.base import BaseEnum


class FilterType(BaseEnum):
    """Filter type."""

    eq = "eq"
    """Equals."""
    ne = "ne"
    """Not equals."""
    in_list = "in_list"
    """In list."""
    not_in_list = "not_in_list"
    """Not in list."""
    gt = "gt"
    """Greater than."""
    ge = "ge"
    """Greater than or equal."""
    lt = "lt"
    """Less than."""
    le = "le"
    """Less than or equal."""
    like = "like"
    """Like."""
    ilike = "ilike"
    """Case-insensitive like."""
    order_by = "order_by"
    """Order by (ASC or DESC)."""
    skip = "skip"
    """Skip auto-generated filter."""
    func = "func"
    """Function filter."""


class OrderByType(BaseEnum):
    """Order by filter type."""

    ASC = "asc"
    DESC = "desc"


class IntervalUnit(BaseEnum):
    """Interval unit."""

    SECOND = "second"
    MINUTE = "minute"
    HOUR = "hour"
    DAY = "day"
    WEEK = "week"
    MONTH = "month"
    QUARTER = "quarter"
    YEAR = "year"
