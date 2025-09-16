"""HTTP schemes."""

from typing import Annotated

from fastapi import HTTPException, Request
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from fastapi.security.utils import get_authorization_scheme_param
from typing_extensions import Doc


class HTTPBearerWithCustomError(HTTPBearer):
    """HTTPBearer with custom unauthorized error."""

    def __init__(
        self,
        exception_to_raise: type[HTTPException],
        *,
        bearerFormat: Annotated[str | None, Doc("Bearer token format.")] = None,  # noqa: N803
        scheme_name: Annotated[
            str | None,
            Doc(
                """
                Security scheme name.

                It will be included in the generated OpenAPI (e.g. visible at `/docs`).
                """,
            ),
        ] = None,
        description: Annotated[
            str | None,
            Doc(
                """
                Security scheme description.

                It will be included in the generated OpenAPI (e.g. visible at `/docs`).
                """,
            ),
        ] = None,
        auto_error: Annotated[
            bool,
            Doc(
                """
                By default, if the HTTP Bearer token is not provided (in an
                `Authorization` header), `HTTPBearer` will automatically cancel the
                request and send the client an error.

                If `auto_error` is set to `False`, when the HTTP Bearer token
                is not available, instead of erroring out, the dependency result will
                be `None`.

                This is useful when you want to have optional authentication.

                It is also useful when you want to have authentication that can be
                provided in one of multiple optional ways (for example, in an HTTP
                Bearer token or in a cookie).
                """,
            ),
        ] = True,
    ) -> None:
        super().__init__(
            bearerFormat=bearerFormat,
            scheme_name=scheme_name,
            description=description,
            auto_error=auto_error,
        )
        self._exception_to_raise = exception_to_raise

    async def __call__(self, request: Request) -> HTTPAuthorizationCredentials | None:
        """Call the HTTPBearer scheme."""
        authorization = request.headers.get("Authorization")
        scheme, credentials = get_authorization_scheme_param(authorization)
        if not (authorization and scheme and credentials):
            if self.auto_error:
                raise self._exception_to_raise  # type: ignore[reportGeneralTypeIssues]
            return None
        return HTTPAuthorizationCredentials(scheme=scheme, credentials=credentials)
