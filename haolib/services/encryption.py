"""Encryption service."""

from cryptography.fernet import Fernet


class EncryptionService:
    """Encryption service."""

    def __init__(self, secret_key: str) -> None:
        """Initialize the encryption service."""
        self._fernet = Fernet(secret_key)

    def encrypt(self, data: str) -> str:
        """Encrypt data.

        Args:
            data: The data to encrypt.

        Returns:
            The encrypted data.

        """
        encrypted_data = self._fernet.encrypt(data.encode())
        return encrypted_data.decode()

    def decrypt(self, data: str) -> str:
        """Decrypt data.

        Args:
            data: The data to decrypt.

        Returns:
            The decrypted data.

        """
        return self._fernet.decrypt(data.encode()).decode()
