"""S3 config."""

from pydantic import AnyHttpUrl, Field
from pydantic_settings import BaseSettings


class S3Config(BaseSettings):
    """S3 configuration.

    This config is used to configure the S3 client.

    Attributes:
        aws_access_key_id (str): The AWS access key ID for authenticating API requests.
            Can be set via AWS_ACCESS_KEY_ID environment variable.
        aws_secret_access_key (str): The AWS secret access key for authenticating API requests.
            Can be set via AWS_SECRET_ACCESS_KEY environment variable.
        aws_session_token (str | None): The AWS session token for temporary credentials.
            Can be set via AWS_SESSION_TOKEN environment variable. Defaults to None.
        aws_region (str | None): The AWS region where the S3 bucket is located.
            Can be set via AWS_DEFAULT_REGION or AWS_REGION environment variable.
            Example: 'us-east-1'. Defaults to None.
        aws_profile (str | None): The AWS profile name to use from ~/.aws/credentials and ~/.aws/config.
            Can be set via AWS_PROFILE environment variable. Defaults to None.
        aws_account_id (str | None): The AWS account ID. Can be set via AWS_ACCOUNT_ID environment variable.
            Defaults to None.
        endpoint_url (HttpUrl | None): The complete URL to the S3 service.
            Useful for specifying custom endpoints or S3-compatible services (e.g., MinIO, LocalStack).
            Can be set via AWS_ENDPOINT_URL or S3_ENDPOINT_URL environment variable.
            Default is None, which uses the standard AWS endpoint.
        use_ssl (bool): Whether to use SSL/TLS for secure connections.
            Can be set via AWS_USE_SSL or S3_USE_SSL environment variable. Defaults to True.
        verify (bool | str | None): Controls SSL certificate verification.
            If True, verifies the server's certificate (default).
            If False, SSL verification is disabled (not recommended for production).
            If a string, it's the path to a CA bundle to use for verification.
            Can be set via AWS_VERIFY or S3_VERIFY environment variable. Defaults to None.

    """

    aws_access_key_id: str = Field(description="The AWS access key ID for authenticating API requests.")
    aws_secret_access_key: str = Field(description="The AWS secret access key for authenticating API requests.")
    aws_session_token: str | None = Field(default=None, description="The AWS session token for temporary credentials.")
    aws_region: str | None = Field(
        default=None, description="The AWS region where the S3 bucket is located (e.g., 'us-east-1')."
    )
    aws_profile: str | None = Field(
        default=None, description="The AWS profile name to use from ~/.aws/credentials and ~/.aws/config."
    )
    aws_account_id: str | None = Field(default=None, description="The AWS account ID.")
    endpoint_url: AnyHttpUrl | None = Field(
        default=None,
        description="The complete URL to the S3 service. Useful for custom endpoints or S3-compatible services.",
    )
    use_ssl: bool = Field(default=True, description="Whether to use SSL/TLS for secure connections.")
    verify: bool | str | None = Field(
        default=None,
        description="Controls SSL certificate verification. True to verify, False to disable, or path to CA bundle.",
    )
