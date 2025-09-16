"""Async PyJWKClient."""

from functools import lru_cache
from ssl import SSLContext
from typing import Any

import httpx
from jwt import PyJWK, PyJWKClientConnectionError, PyJWKClientError, PyJWKSet
from jwt import decode_complete as decode_token
from jwt.jwk_set_cache import JWKSetCache


class AsyncPyJWKClient:
    """PyJWKClient."""

    def __init__(
        self,
        uri: str,
        cache_keys: bool = False,  # noqa: FBT001 FBT002
        max_cached_keys: int = 16,
        cache_jwk_set: bool = True,  # noqa: FBT001 FBT002
        lifespan: int = 300,
        headers: dict[str, Any] | None = None,
        timeout: int = 30,
        ssl_context: SSLContext | None = None,
        *,
        http_client: httpx.AsyncClient | None = None,
    ) -> None:
        """Initialize the AsyncPyJWKClient."""
        if headers is None:
            headers = {}
        if http_client is None:
            http_client = httpx.AsyncClient(timeout=timeout, verify=ssl_context if ssl_context is not None else True)

        self.uri = uri
        self.jwk_set_cache: JWKSetCache | None = None
        self.headers = headers
        self.timeout = timeout
        self.ssl_context = ssl_context
        self.http_client = http_client

        if cache_jwk_set:
            # Init jwt set cache with default or given lifespan.
            # Default lifespan is 300 seconds (5 minutes).
            if lifespan <= 0:
                message = f'Lifespan must be greater than 0, the input is "{lifespan}"'
                raise PyJWKClientError(message)
            self.jwk_set_cache = JWKSetCache(lifespan)
        else:
            self.jwk_set_cache = None

        if cache_keys:
            # Cache signing keys
            # Ignore mypy (https://github.com/python/mypy/issues/2427)
            self.get_signing_key = lru_cache(maxsize=max_cached_keys)(self.get_signing_key)  # type: ignore[method-assign]

    async def fetch_data(self) -> Any:
        """Fetch data from the url."""
        jwk_set: Any = None
        try:
            r = await self.http_client.get(url=self.uri, headers=self.headers)
            jwk_set = r.json()

        except Exception as e:
            message = f'Fail to fetch data from the url, err: "{e}"'
            raise PyJWKClientConnectionError(message) from e
        else:
            return jwk_set
        finally:
            if self.jwk_set_cache is not None:
                self.jwk_set_cache.put(jwk_set)

    async def get_jwk_set(self, refresh: bool = False) -> PyJWKSet:  # noqa: FBT001 FBT002
        """Get JWK set."""
        data = None
        if self.jwk_set_cache is not None and not refresh:
            data = self.jwk_set_cache.get()

        if data is None:
            data = await self.fetch_data()

        if not isinstance(data, dict):
            raise PyJWKClientError("The JWKS endpoint did not return a JSON object")

        return PyJWKSet.from_dict(data)

    async def get_signing_keys(self, refresh: bool = False) -> list[PyJWK]:  # noqa: FBT001 FBT002
        """Get signing keys."""
        jwk_set = await self.get_jwk_set(refresh)
        signing_keys = [
            jwk_set_key
            for jwk_set_key in jwk_set.keys
            if jwk_set_key.public_key_use in ["sig", None] and jwk_set_key.key_id
        ]

        if not signing_keys:
            raise PyJWKClientError("The JWKS endpoint did not contain any signing keys")

        return signing_keys

    async def get_signing_key(self, kid: str) -> PyJWK:
        """Get signing key."""
        signing_keys = await self.get_signing_keys()
        signing_key = self.match_kid(signing_keys, kid)

        if not signing_key:
            # If no matching signing key from the jwk set, refresh the jwk set and try again.
            signing_keys = await self.get_signing_keys(refresh=True)
            signing_key = self.match_kid(signing_keys, kid)

            if not signing_key:
                message = f'Unable to find a signing key that matches: "{kid}"'
                raise PyJWKClientError(message)

        return signing_key

    async def get_signing_key_from_jwt(self, token: str) -> PyJWK:
        """Get signing key from JWT."""
        unverified = decode_token(token, options={"verify_signature": False})
        header = unverified["header"]
        return await self.get_signing_key(header.get("kid"))

    @staticmethod
    def match_kid(signing_keys: list[PyJWK], kid: str) -> PyJWK | None:
        """Match kid."""
        signing_key = None

        for key in signing_keys:
            if key.key_id == kid:
                signing_key = key
                break

        return signing_key
