"""Test hash password."""

from haolib.utils.hash_password import hash_password, verify_password


def test_hash_password() -> None:
    """Test hash password."""
    assert verify_password("test", hash_password("test"))
    assert not verify_password("test", hash_password("test2"))
