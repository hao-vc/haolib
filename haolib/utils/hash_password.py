"""Hash password."""

import bcrypt


def hash_password(password: str) -> str:
    """Hash a password."""
    salt = bcrypt.gensalt()

    hashed_password = bcrypt.hashpw(password=password.encode("utf-8"), salt=salt)

    return hashed_password.decode("utf-8")


def verify_password(password: str, hashed_password: str) -> bool:
    """Verify a password."""
    return bcrypt.checkpw(password=password.encode("utf-8"), hashed_password=hashed_password.encode("utf-8"))
