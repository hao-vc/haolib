"""Encryption service."""

from cryptography.fernet import Fernet


class FernetEncryptor:
    """Fernet encryptor."""

    def __init__(self, secret_key: str) -> None:
        """Initialize the Fernet encryptor."""
        self._fernet = Fernet(secret_key)

    def encrypt(self, data: bytes) -> bytes:
        """Encrypt data.

        Args:
            data: The data to encrypt.

        Returns:
            The encrypted data.

        """
        return self._fernet.encrypt(data)

    def decrypt(self, data: bytes) -> bytes:
        """Decrypt data.

        Args:
            data: The data to decrypt.

        Returns:
            The decrypted data.

        """
        return self._fernet.decrypt(data)
