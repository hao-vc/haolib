"""Test rattrs."""

from __future__ import annotations

import pytest

from haolib.utils.rattrs import rgetattr


class A:
    """Test rattrs."""

    b: B

    def __init__(self, b: B) -> None:
        self.b = b


class B:
    """Test rattrs."""

    c: C

    def __init__(self, c: C) -> None:
        self.c = c


class C:
    """Test rattrs."""

    def __init__(self) -> None:
        pass


def test_rgetattr() -> None:
    """Test rgetattr."""
    obj = A(B(C()))
    assert rgetattr(obj, "b.c") == obj.b.c
    with pytest.raises(AttributeError):
        rgetattr(obj, "b.d")
