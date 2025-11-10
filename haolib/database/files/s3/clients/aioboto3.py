"""AIOboto3 S3 client."""

from datetime import datetime
from types import TracebackType
from typing import Any, Self

import aioboto3  # type: ignore[import-untyped]
from botocore.exceptions import ClientError  # type: ignore[import-untyped]

from haolib.database.files.s3.clients.abstract import (
    S3AccessDeniedClientException,
    S3BucketAlreadyExistsClientException,
    S3BucketAlreadyOwnedByYouClientException,
    S3BucketNotEmptyClientException,
    S3InvalidBucketNameClientException,
    S3InvalidObjectStateClientException,
    S3InvalidRequestClientException,
    S3InvalidSecurityClientException,
    S3InvalidTokenClientException,
    S3NoSuchBucketClientException,
    S3NoSuchCORSConfigurationClientException,
    S3NoSuchKeyClientException,
    S3NoSuchLifecycleConfigurationClientException,
    S3NoSuchPolicyClientException,
    S3ObjectNotInActiveTierClientException,
    S3PreconditionFailedClientException,
    S3RequestTimeoutClientException,
    S3ServiceClientException,
)
from haolib.database.files.s3.clients.pydantic import (
    S3AccessControlPolicy,
    S3Bucket,
    S3CommonPrefix,
    S3CopyObjectResponse,
    S3CopyObjectResult,
    S3CORSConfiguration,
    S3CORSRule,
    S3CreateBucketConfiguration,
    S3CreateBucketResponse,
    S3DeleteBucketCorsResponse,
    S3DeleteBucketLifecycleResponse,
    S3DeleteBucketPolicyResponse,
    S3DeleteBucketResponse,
    S3DeleteObjectResponse,
    S3DeleteObjectsDelete,
    S3DeleteObjectsResponse,
    S3DeleteObjectsResponseDeletedItem,
    S3DeleteObjectsResponseErrorItem,
    S3GetBucketAclResponse,
    S3GetBucketCorsResponse,
    S3GetBucketLifecycleConfigurationResponse,
    S3GetBucketPolicyResponse,
    S3GetObjectAclResponse,
    S3GetObjectResponse,
    S3Grant,
    S3Grantee,
    S3LifecycleConfiguration,
    S3LifecycleExpiration,
    S3LifecycleRule,
    S3LifecycleRuleFilter,
    S3LifecycleTransition,
    S3ListBucketsResponse,
    S3ListObjectsResponse,
    S3ListObjectsV2Response,
    S3NoncurrentVersionExpiration,
    S3NoncurrentVersionTransition,
    S3Object,
    S3ObjectLockConfiguration,
    S3ObjectLockLegalHold,
    S3ObjectLockRetention,
    S3Owner,
    S3PutBucketAclResponse,
    S3PutBucketCorsResponse,
    S3PutBucketLifecycleConfigurationResponse,
    S3PutBucketPolicyResponse,
    S3PutObjectAclResponse,
    S3PutObjectLegalHoldResponse,
    S3PutObjectLockConfigurationResponse,
    S3PutObjectResponse,
    S3PutObjectRetentionResponse,
)


class Aioboto3S3Client:
    """AIOboto3 S3 client.

    This client wraps aioboto3 to provide async S3 operations.
    It implements all methods from the AbstractS3Client protocol.
    """

    def __init__(
        self,
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        aws_session_token: str | None = None,
        region_name: str | None = None,
        profile_name: str | None = None,
        aws_account_id: str | None = None,
        use_ssl: bool = True,
        verify: bool | None = None,
        endpoint_url: str | None = None,
    ) -> None:
        """Initialize the client.

        Args:
            aws_access_key_id: AWS access key ID.
            aws_secret_access_key: AWS secret access key.
            aws_session_token: AWS session token.
            region_name: AWS region name.
            profile_name: AWS profile name.
            aws_account_id: AWS account ID.
            use_ssl: Whether to use SSL.
            verify: Whether to verify SSL certificates.
            endpoint_url: Custom endpoint URL.

        """
        self._session = aioboto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token,
            region_name=region_name,
            profile_name=profile_name,
            aws_account_id=aws_account_id,
        )
        self._use_ssl = use_ssl
        self._verify = verify
        self._endpoint_url = endpoint_url
        self._client: Any = None

    async def __aenter__(self) -> Self:
        """Enter the context manager."""
        client_kwargs: dict[str, Any] = {}
        if self._endpoint_url:
            client_kwargs["endpoint_url"] = self._endpoint_url
        if self._verify is not None:
            client_kwargs["verify"] = self._verify
        if not self._use_ssl:
            client_kwargs["use_ssl"] = False

        self._client = await self._session.client("s3", **client_kwargs).__aenter__()
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Exit the context manager."""
        if self._client:
            await self._client.__aexit__(exc_type, exc_value, traceback)

    def _build_kwargs(self, **kwargs: Any) -> dict[str, Any]:
        """Build kwargs for boto3 calls, filtering out None values."""
        return {k: v for k, v in kwargs.items() if v is not None}

    def _handle_client_error(self, error: ClientError) -> None:
        """Map boto3 ClientError to custom S3 exceptions.

        Args:
            error: The boto3 ClientError to map.

        Raises:
            S3BucketAlreadyExistsClientException: If bucket already exists.
            S3BucketAlreadyOwnedByYouClientException: If bucket is already owned by you.
            S3NoSuchBucketClientException: If bucket does not exist.
            S3NoSuchKeyClientException: If object key does not exist.
            S3AccessDeniedClientException: If access is denied.
            S3BucketNotEmptyClientException: If bucket is not empty.
            S3NoSuchLifecycleConfigurationClientException: If lifecycle configuration does not exist.
            S3NoSuchCORSConfigurationClientException: If CORS configuration does not exist.
            S3NoSuchPolicyClientException: If policy does not exist.
            S3InvalidBucketNameClientException: If bucket name is invalid.
            S3ObjectNotInActiveTierClientException: If object is not in active tier.
            S3InvalidObjectStateClientException: If object is in invalid state.
            S3PreconditionFailedClientException: If precondition check fails.
            S3InvalidSecurityClientException: If security credentials are invalid.
            S3InvalidTokenClientException: If token is invalid.
            S3RequestTimeoutClientException: If request times out.
            S3InvalidRequestClientException: If request is invalid.
            S3ServiceClientException: For other S3 service errors.

        """
        error_code = error.response.get("Error", {}).get("Code", "")
        error_message = error.response.get("Error", {}).get("Message", str(error))

        exception_map: dict[str, type[Exception]] = {
            "BucketAlreadyExists": S3BucketAlreadyExistsClientException,
            "BucketAlreadyOwnedByYou": S3BucketAlreadyOwnedByYouClientException,
            "NoSuchBucket": S3NoSuchBucketClientException,
            "NoSuchKey": S3NoSuchKeyClientException,
            "AccessDenied": S3AccessDeniedClientException,
            "BucketNotEmpty": S3BucketNotEmptyClientException,
            "NoSuchLifecycleConfiguration": S3NoSuchLifecycleConfigurationClientException,
            "NoSuchCORSConfiguration": S3NoSuchCORSConfigurationClientException,
            "NoSuchBucketPolicy": S3NoSuchPolicyClientException,
            "InvalidBucketName": S3InvalidBucketNameClientException,
            "ObjectNotInActiveTierError": S3ObjectNotInActiveTierClientException,
            "InvalidObjectState": S3InvalidObjectStateClientException,
            "PreconditionFailed": S3PreconditionFailedClientException,
            "InvalidSecurity": S3InvalidSecurityClientException,
            "InvalidToken": S3InvalidTokenClientException,
            "RequestTimeout": S3RequestTimeoutClientException,
            "InvalidRequest": S3InvalidRequestClientException,
            "NotImplemented": S3InvalidRequestClientException,  # MinIO may return this for unsupported features
            "InvalidBucketState": S3InvalidRequestClientException,  # MinIO may return this for object lock
        }

        exception_class = exception_map.get(error_code, S3ServiceClientException)
        raise exception_class(error_message) from error

    async def copy_object(
        self,
        bucket: str,
        copy_source: str,
        key: str,
        acl: str | None = None,
        cache_control: str | None = None,
        checksum_algorithm: str | None = None,
        content_disposition: str | None = None,
        content_encoding: str | None = None,
        content_language: str | None = None,
        content_type: str | None = None,
        copy_source_if_match: str | None = None,
        copy_source_if_modified_since: datetime | None = None,
        copy_source_if_none_match: str | None = None,
        copy_source_if_unmodified_since: datetime | None = None,
        expires: datetime | None = None,
        grant_full_control: str | None = None,
        grant_read: str | None = None,
        grant_read_acp: str | None = None,
        grant_write_acp: str | None = None,
        if_match: str | None = None,
        if_none_match: str | None = None,
        metadata: dict[str, str] | None = None,
        metadata_directive: str | None = None,
        tagging_directive: str | None = None,
        server_side_encryption: str | None = None,
        storage_class: str | None = None,
        website_redirect_location: str | None = None,
        sse_customer_algorithm: str | None = None,
        sse_customer_key: str | None = None,
        sse_kms_key_id: str | None = None,
        sse_kms_encryption_context: str | None = None,
        bucket_key_enabled: bool | None = None,
        copy_source_sse_customer_algorithm: str | None = None,
        copy_source_sse_customer_key: str | None = None,
        request_payer: str | None = None,
        tagging: str | None = None,
        object_lock_mode: str | None = None,
        object_lock_retain_until_date: datetime | None = None,
        object_lock_legal_hold_status: str | None = None,
        expected_bucket_owner: str | None = None,
        expected_source_bucket_owner: str | None = None,
    ) -> S3CopyObjectResponse:
        """Copy an object from one bucket to another."""
        kwargs = self._build_kwargs(
            Bucket=bucket,
            CopySource=copy_source,
            Key=key,
            ACL=acl,
            CacheControl=cache_control,
            ChecksumAlgorithm=checksum_algorithm,
            ContentDisposition=content_disposition,
            ContentEncoding=content_encoding,
            ContentLanguage=content_language,
            ContentType=content_type,
            CopySourceIfMatch=copy_source_if_match,
            CopySourceIfModifiedSince=copy_source_if_modified_since,
            CopySourceIfNoneMatch=copy_source_if_none_match,
            CopySourceIfUnmodifiedSince=copy_source_if_unmodified_since,
            Expires=expires,
            GrantFullControl=grant_full_control,
            GrantRead=grant_read,
            GrantReadACP=grant_read_acp,
            GrantWriteACP=grant_write_acp,
            IfMatch=if_match,
            IfNoneMatch=if_none_match,
            Metadata=metadata,
            MetadataDirective=metadata_directive,
            TaggingDirective=tagging_directive,
            ServerSideEncryption=server_side_encryption,
            StorageClass=storage_class,
            WebsiteRedirectLocation=website_redirect_location,
            SSECustomerAlgorithm=sse_customer_algorithm,
            SSECustomerKey=sse_customer_key,
            SSEKMSKeyId=sse_kms_key_id,
            SSEKMSEncryptionContext=sse_kms_encryption_context,
            BucketKeyEnabled=bucket_key_enabled,
            CopySourceSSECustomerAlgorithm=copy_source_sse_customer_algorithm,
            CopySourceSSECustomerKey=copy_source_sse_customer_key,
            RequestPayer=request_payer,
            Tagging=tagging,
            ObjectLockMode=object_lock_mode,
            ObjectLockRetainUntilDate=object_lock_retain_until_date,
            ObjectLockLegalHoldStatus=object_lock_legal_hold_status,
            ExpectedBucketOwner=expected_bucket_owner,
            ExpectedSourceBucketOwner=expected_source_bucket_owner,
        )
        try:
            response = await self._client.copy_object(**kwargs)
            copy_object_result = None
            if copy_result := response.get("CopyObjectResult"):
                copy_object_result = S3CopyObjectResult(
                    etag=copy_result.get("ETag"),
                    last_modified=copy_result.get("LastModified"),
                    checksum_crc32=copy_result.get("ChecksumCRC32"),
                    checksum_crc32c=copy_result.get("ChecksumCRC32C"),
                    checksum_sha1=copy_result.get("ChecksumSHA1"),
                    checksum_sha256=copy_result.get("ChecksumSHA256"),
                )
            return S3CopyObjectResponse(
                copy_object_result=copy_object_result,
                copy_source_version_id=response.get("CopySourceVersionId"),
                expiration=response.get("Expiration"),
                request_charged=response.get("RequestCharged"),
                server_side_encryption=response.get("ServerSideEncryption"),
                sse_customer_algorithm=response.get("SSECustomerAlgorithm"),
                sse_customer_key_md5=response.get("SSECustomerKeyMD5"),
                sse_kms_key_id=response.get("SSEKMSKeyId"),
                sse_kms_encryption_context=response.get("SSEKMSEncryptionContext"),
                bucket_key_enabled=response.get("BucketKeyEnabled"),
                version_id=response.get("VersionId"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def create_bucket(  # noqa: C901, PLR0912
        self,
        bucket: str,
        acl: str | None = None,
        create_bucket_configuration: S3CreateBucketConfiguration | None = None,
        grant_full_control: str | None = None,
        grant_read: str | None = None,
        grant_read_acp: str | None = None,
        grant_write: str | None = None,
        grant_write_acp: str | None = None,
        object_lock_enabled_for_bucket: bool | None = None,
        object_ownership: str | None = None,
    ) -> S3CreateBucketResponse:
        """Create a new bucket."""
        kwargs: dict[str, Any] = {"Bucket": bucket}
        if acl:
            kwargs["ACL"] = acl
        if create_bucket_configuration:
            config: dict[str, Any] = {}
            if (
                hasattr(create_bucket_configuration, "location_constraint")
                and create_bucket_configuration.location_constraint
            ):
                config["LocationConstraint"] = create_bucket_configuration.location_constraint
            if hasattr(create_bucket_configuration, "location") and create_bucket_configuration.location:
                config["Location"] = {
                    "Type": create_bucket_configuration.location.type,
                    "Name": create_bucket_configuration.location.name,
                }
            if hasattr(create_bucket_configuration, "bucket") and create_bucket_configuration.bucket:
                config["Bucket"] = {
                    "DataRedundancy": create_bucket_configuration.bucket.data_redundancy,
                    "Type": create_bucket_configuration.bucket.type,
                }
            if hasattr(create_bucket_configuration, "tags") and create_bucket_configuration.tags:
                config["TagSet"] = [{"Key": tag.key, "Value": tag.value} for tag in create_bucket_configuration.tags]
            if config:
                kwargs["CreateBucketConfiguration"] = config
        if grant_full_control:
            kwargs["GrantFullControl"] = grant_full_control
        if grant_read:
            kwargs["GrantRead"] = grant_read
        if grant_read_acp:
            kwargs["GrantReadACP"] = grant_read_acp
        if grant_write:
            kwargs["GrantWrite"] = grant_write
        if grant_write_acp:
            kwargs["GrantWriteACP"] = grant_write_acp
        if object_lock_enabled_for_bucket is not None:
            kwargs["ObjectLockEnabledForBucket"] = object_lock_enabled_for_bucket
        if object_ownership:
            kwargs["ObjectOwnership"] = object_ownership

        try:
            response = await self._client.create_bucket(**kwargs)

            return S3CreateBucketResponse(
                location=response.get("Location"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def delete_bucket(
        self,
        bucket: str,
        expected_bucket_owner: str | None = None,
    ) -> S3DeleteBucketResponse:
        """Delete a bucket."""
        kwargs = self._build_kwargs(Bucket=bucket, ExpectedBucketOwner=expected_bucket_owner)
        try:
            response = await self._client.delete_bucket(**kwargs)
            return S3DeleteBucketResponse(
                request_charged=response.get("RequestCharged"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def delete_bucket_cors(
        self, bucket: str, expected_bucket_owner: str | None = None
    ) -> S3DeleteBucketCorsResponse:
        """Delete the CORS configuration for a bucket."""
        kwargs = self._build_kwargs(Bucket=bucket, ExpectedBucketOwner=expected_bucket_owner)
        try:
            response = await self._client.delete_bucket_cors(**kwargs)
            return S3DeleteBucketCorsResponse(
                request_charged=response.get("RequestCharged"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def delete_bucket_lifecycle(
        self, bucket: str, expected_bucket_owner: str | None = None
    ) -> S3DeleteBucketLifecycleResponse:
        """Delete the lifecycle configuration for a bucket."""
        kwargs = self._build_kwargs(Bucket=bucket, ExpectedBucketOwner=expected_bucket_owner)
        try:
            response = await self._client.delete_bucket_lifecycle(**kwargs)
            return S3DeleteBucketLifecycleResponse(
                request_charged=response.get("RequestCharged"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def delete_bucket_policy(
        self, bucket: str, expected_bucket_owner: str | None = None
    ) -> S3DeleteBucketPolicyResponse:
        """Delete the policy for a bucket."""
        kwargs = self._build_kwargs(Bucket=bucket, ExpectedBucketOwner=expected_bucket_owner)
        try:
            response = await self._client.delete_bucket_policy(**kwargs)
            return S3DeleteBucketPolicyResponse(
                request_charged=response.get("RequestCharged"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def delete_object(
        self,
        bucket: str,
        key: str,
        mfa: str | None = None,
        version_id: str | None = None,
        request_payer: str | None = None,
        bypass_governance_retention: bool | None = None,
        expected_bucket_owner: str | None = None,
        if_match: str | None = None,
        if_match_last_modified_time: datetime | None = None,
        if_match_size: int | None = None,
    ) -> S3DeleteObjectResponse:
        """Delete an object from a bucket."""
        kwargs = self._build_kwargs(
            Bucket=bucket,
            Key=key,
            MFA=mfa,
            VersionId=version_id,
            RequestPayer=request_payer,
            BypassGovernanceRetention=bypass_governance_retention,
            ExpectedBucketOwner=expected_bucket_owner,
            IfMatch=if_match,
            IfMatchLastModifiedTime=if_match_last_modified_time,
            IfMatchSize=if_match_size,
        )
        try:
            response = await self._client.delete_object(**kwargs)
            return S3DeleteObjectResponse(
                delete_marker=response.get("DeleteMarker"),
                version_id=response.get("VersionId"),
                request_charged=response.get("RequestCharged"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def delete_objects(
        self,
        bucket: str,
        delete: S3DeleteObjectsDelete,
        mfa: str | None = None,
        request_payer: str | None = None,
        bypass_governance_retention: bool | None = None,
        expected_bucket_owner: str | None = None,
        checksum_algorithm: str | None = None,
    ) -> S3DeleteObjectsResponse:
        """Delete objects from a bucket."""
        delete_dict: dict[str, Any] = {
            "Objects": [
                {
                    "Key": obj.key,
                    **({"VersionId": obj.version_id} if obj.version_id else {}),
                }
                for obj in delete.objects
            ]
        }
        if hasattr(delete, "quiet") and delete.quiet is not None:
            delete_dict["Quiet"] = delete.quiet

        kwargs = self._build_kwargs(
            Bucket=bucket,
            Delete=delete_dict,
            MFA=mfa,
            RequestPayer=request_payer,
            BypassGovernanceRetention=bypass_governance_retention,
            ExpectedBucketOwner=expected_bucket_owner,
            ChecksumAlgorithm=checksum_algorithm,
        )
        try:
            response = await self._client.delete_objects(**kwargs)
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

        # Build response according to S3DeleteObjectsResponse protocol
        deleted_items = [
            S3DeleteObjectsResponseDeletedItem(
                key=item["Key"],
                version_id=item.get("VersionId", ""),
                delete_marker=item.get("DeleteMarker", False),
                delete_marker_version_id=item.get("DeleteMarkerVersionId", ""),
            )
            for item in response.get("Deleted", [])
        ]
        error_items = [
            S3DeleteObjectsResponseErrorItem(
                key=item["Key"],
                version_id=item.get("VersionId", ""),
                code=item["Code"],
                message=item["Message"],
            )
            for item in response.get("Errors", [])
        ]

        return S3DeleteObjectsResponse(
            deleted=deleted_items,
            error=error_items,
            request_charged=response.get("RequestCharged"),
        )

    async def get_bucket_acl(
        self,
        bucket: str,
        expected_bucket_owner: str | None = None,
    ) -> S3GetBucketAclResponse:
        """Get the ACL for a bucket."""
        kwargs = self._build_kwargs(Bucket=bucket, ExpectedBucketOwner=expected_bucket_owner)
        try:
            response = await self._client.get_bucket_acl(**kwargs)
            grants = None
            if grants_data := response.get("Grants"):
                grants = [
                    S3Grant(
                        grantee=S3Grantee(
                            display_name=grant.get("Grantee", {}).get("DisplayName"),
                            email_address=grant.get("Grantee", {}).get("EmailAddress"),
                            id=grant.get("Grantee", {}).get("ID"),
                            type=grant.get("Grantee", {}).get("Type"),
                            uri=grant.get("Grantee", {}).get("URI"),
                        )
                        if grant.get("Grantee")
                        else None,
                        permission=grant.get("Permission"),
                    )
                    for grant in grants_data
                ]
            owner = None
            if owner_data := response.get("Owner"):
                owner = S3Owner(
                    display_name=owner_data.get("DisplayName"),
                    id=owner_data.get("ID"),
                )
            return S3GetBucketAclResponse(
                grants=grants,
                owner=owner,
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def get_bucket_cors(
        self,
        bucket: str,
        expected_bucket_owner: str | None = None,
    ) -> S3GetBucketCorsResponse:
        """Get the CORS configuration for a bucket."""
        kwargs = self._build_kwargs(Bucket=bucket, ExpectedBucketOwner=expected_bucket_owner)
        try:
            response = await self._client.get_bucket_cors(**kwargs)
            cors_rules = None
            if cors_rules_data := response.get("CORSRules"):
                cors_rules = [
                    S3CORSRule(
                        allowed_headers=rule.get("AllowedHeaders"),
                        allowed_methods=rule.get("AllowedMethods", []),
                        allowed_origins=rule.get("AllowedOrigins", []),
                        expose_headers=rule.get("ExposeHeaders"),
                        id=rule.get("ID"),
                        max_age_seconds=rule.get("MaxAgeSeconds"),
                    )
                    for rule in cors_rules_data
                ]
            return S3GetBucketCorsResponse(
                cors_rules=cors_rules,
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def get_bucket_lifecycle_configuration(
        self,
        bucket: str,
        expected_bucket_owner: str | None = None,
    ) -> S3GetBucketLifecycleConfigurationResponse:
        """Get the lifecycle configuration for a bucket."""
        kwargs = self._build_kwargs(Bucket=bucket, ExpectedBucketOwner=expected_bucket_owner)
        try:
            response = await self._client.get_bucket_lifecycle_configuration(**kwargs)
            rules = None
            if rules_data := response.get("Rules"):
                rules = [
                    S3LifecycleRule(
                        id=rule.get("Id", ""),
                        status=rule.get("Status", "Enabled"),
                        abort_incomplete_multipart_upload=rule.get("AbortIncompleteMultipartUpload"),
                        expiration=S3LifecycleExpiration(
                            date=rule.get("Expiration", {}).get("Date"),
                            days=rule.get("Expiration", {}).get("Days"),
                            expired_object_delete_marker=rule.get("Expiration", {}).get("ExpiredObjectDeleteMarker"),
                        )
                        if rule.get("Expiration")
                        else None,
                        filter=S3LifecycleRuleFilter(
                            and_=rule.get("Filter", {}).get("And"),
                            prefix=rule.get("Filter", {}).get("Prefix"),
                            tag=rule.get("Filter", {}).get("Tag"),
                            object_size_greater_than=rule.get("Filter", {}).get("ObjectSizeGreaterThan"),
                            object_size_less_than=rule.get("Filter", {}).get("ObjectSizeLessThan"),
                        )
                        if rule.get("Filter")
                        else None,
                        noncurrent_version_expiration=S3NoncurrentVersionExpiration(
                            noncurrent_days=rule.get("NoncurrentVersionExpiration", {}).get("NoncurrentDays"),
                            newer_noncurrent_versions=rule.get("NoncurrentVersionExpiration", {}).get(
                                "NewerNoncurrentVersions"
                            ),
                        )
                        if rule.get("NoncurrentVersionExpiration")
                        else None,
                        noncurrent_version_transitions=[
                            S3NoncurrentVersionTransition(
                                noncurrent_days=trans.get("NoncurrentDays"),
                                storage_class=trans.get("StorageClass"),
                                newer_noncurrent_versions=trans.get("NewerNoncurrentVersions"),
                            )
                            for trans in rule.get("NoncurrentVersionTransitions", [])
                        ]
                        if rule.get("NoncurrentVersionTransitions")
                        else None,
                        transitions=[
                            S3LifecycleTransition(
                                date=trans.get("Date"),
                                days=trans.get("Days"),
                                storage_class=trans.get("StorageClass"),
                            )
                            for trans in rule.get("Transitions", [])
                        ]
                        if rule.get("Transitions")
                        else None,
                    )
                    for rule in rules_data
                ]
            return S3GetBucketLifecycleConfigurationResponse(rules=rules)
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchLifecycleConfiguration":
                return S3GetBucketLifecycleConfigurationResponse(rules=[])
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def get_bucket_policy(
        self,
        bucket: str,
        expected_bucket_owner: str | None = None,
    ) -> S3GetBucketPolicyResponse:
        """Get the policy for a bucket."""
        kwargs = self._build_kwargs(Bucket=bucket, ExpectedBucketOwner=expected_bucket_owner)
        try:
            response = await self._client.get_bucket_policy(**kwargs)
            return S3GetBucketPolicyResponse(
                policy=response.get("Policy"),
                revision_id=response.get("RevisionId"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def get_object(
        self,
        bucket: str,
        key: str,
        if_match: str | None = None,
        if_modified_since: datetime | None = None,
        if_none_match: str | None = None,
        if_unmodified_since: datetime | None = None,
        range_header: str | None = None,
        response_cache_control: str | None = None,
        response_content_disposition: str | None = None,
        response_content_encoding: str | None = None,
        response_content_language: str | None = None,
        response_content_type: str | None = None,
        response_expires: datetime | None = None,
        version_id: str | None = None,
        sse_customer_algorithm: str | None = None,
        sse_customer_key: str | None = None,
        request_payer: str | None = None,
        expected_bucket_owner: str | None = None,
        checksum_mode: str | None = None,
    ) -> S3GetObjectResponse:
        """Get an object from a bucket."""
        kwargs = self._build_kwargs(
            Bucket=bucket,
            Key=key,
            IfMatch=if_match,
            IfModifiedSince=if_modified_since,
            IfNoneMatch=if_none_match,
            IfUnmodifiedSince=if_unmodified_since,
            Range=range_header,
            ResponseCacheControl=response_cache_control,
            ResponseContentDisposition=response_content_disposition,
            ResponseContentEncoding=response_content_encoding,
            ResponseContentLanguage=response_content_language,
            ResponseContentType=response_content_type,
            ResponseExpires=response_expires,
            VersionId=version_id,
            SSECustomerAlgorithm=sse_customer_algorithm,
            SSECustomerKey=sse_customer_key,
            RequestPayer=request_payer,
            ExpectedBucketOwner=expected_bucket_owner,
            ChecksumMode=checksum_mode,
        )
        try:
            response = await self._client.get_object(**kwargs)
            body = await response["Body"].read()
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

        return S3GetObjectResponse(
            body=body,
            delete_marker=response.get("DeleteMarker"),
            accept_ranges=response.get("AcceptRanges"),
            expiration=response.get("Expiration"),
            restore=response.get("Restore"),
            last_modified=response.get("LastModified"),
            content_length=response.get("ContentLength"),
            etag=response.get("ETag"),
            checksum_crc32=response.get("ChecksumCRC32"),
            checksum_crc32c=response.get("ChecksumCRC32C"),
            checksum_sha1=response.get("ChecksumSHA1"),
            checksum_sha256=response.get("ChecksumSHA256"),
            missing_meta=response.get("MissingMeta"),
            version_id=response.get("VersionId"),
            cache_control=response.get("CacheControl"),
            content_disposition=response.get("ContentDisposition"),
            content_encoding=response.get("ContentEncoding"),
            content_language=response.get("ContentLanguage"),
            content_range=response.get("ContentRange"),
            content_type=response.get("ContentType"),
            expires=response.get("Expires"),
            website_redirect_location=response.get("WebsiteRedirectLocation"),
            server_side_encryption=response.get("ServerSideEncryption"),
            metadata=response.get("Metadata"),
            sse_customer_algorithm=response.get("SSECustomerAlgorithm"),
            sse_customer_key_md5=response.get("SSECustomerKeyMD5"),
            sse_kms_key_id=response.get("SSEKMSKeyId"),
            bucket_key_enabled=response.get("BucketKeyEnabled"),
            storage_class=response.get("StorageClass"),
            request_charged=response.get("RequestCharged"),
            replication_status=response.get("ReplicationStatus"),
            parts_count=response.get("PartsCount"),
            tag_count=response.get("TagCount"),
            object_lock_mode=response.get("ObjectLockMode"),
            object_lock_retain_until_date=response.get("ObjectLockRetainUntilDate"),
            object_lock_legal_hold_status=response.get("ObjectLockLegalHoldStatus"),
        )

    async def get_object_acl(
        self,
        bucket: str,
        key: str,
        version_id: str | None = None,
        request_payer: str | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3GetObjectAclResponse:
        """Get the ACL for an object."""
        kwargs = self._build_kwargs(
            Bucket=bucket,
            Key=key,
            VersionId=version_id,
            RequestPayer=request_payer,
            ExpectedBucketOwner=expected_bucket_owner,
        )
        try:
            response = await self._client.get_object_acl(**kwargs)
            grants = None
            if grants_data := response.get("Grants"):
                grants = [
                    S3Grant(
                        grantee=S3Grantee(
                            display_name=grant.get("Grantee", {}).get("DisplayName"),
                            email_address=grant.get("Grantee", {}).get("EmailAddress"),
                            id=grant.get("Grantee", {}).get("ID"),
                            type=grant.get("Grantee", {}).get("Type"),
                            uri=grant.get("Grantee", {}).get("URI"),
                        )
                        if grant.get("Grantee")
                        else None,
                        permission=grant.get("Permission"),
                    )
                    for grant in grants_data
                ]
            owner = None
            if owner_data := response.get("Owner"):
                owner = S3Owner(
                    display_name=owner_data.get("DisplayName"),
                    id=owner_data.get("ID"),
                )
            return S3GetObjectAclResponse(
                grants=grants,
                owner=owner,
                request_charged=response.get("RequestCharged"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def list_buckets(
        self,
    ) -> S3ListBucketsResponse:
        """List all buckets owned by the authenticated sender of the request."""
        try:
            response = await self._client.list_buckets()
            buckets = []
            if buckets_data := response.get("Buckets"):
                buckets = [
                    S3Bucket(
                        name=bucket.get("Name"),
                        creation_date=bucket.get("CreationDate"),
                    )
                    for bucket in buckets_data
                ]
            owner = None
            if owner_data := response.get("Owner"):
                owner = S3Owner(
                    display_name=owner_data.get("DisplayName"),
                    id=owner_data.get("ID"),
                )
            return S3ListBucketsResponse(
                buckets=buckets if buckets else None,
                owner=owner,
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def list_objects(
        self,
        bucket: str,
        delimiter: str | None = None,
        encoding_type: str | None = None,
        marker: str | None = None,
        max_keys: int | None = None,
        prefix: str | None = None,
        request_payer: str | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3ListObjectsResponse:
        """List some or all (up to 1,000) of the objects in a bucket."""
        kwargs = self._build_kwargs(
            Bucket=bucket,
            Delimiter=delimiter,
            EncodingType=encoding_type,
            Marker=marker,
            MaxKeys=max_keys,
            Prefix=prefix,
            RequestPayer=request_payer,
            ExpectedBucketOwner=expected_bucket_owner,
        )
        try:
            response = await self._client.list_objects(**kwargs)
            contents = None
            if contents_data := response.get("Contents"):
                contents = [
                    S3Object(
                        key=obj.get("Key"),
                        last_modified=obj.get("LastModified"),
                        etag=obj.get("ETag"),
                        checksum_algorithm=obj.get("ChecksumAlgorithm"),
                        size=obj.get("Size"),
                        storage_class=obj.get("StorageClass"),
                        owner=S3Owner(
                            display_name=obj.get("Owner", {}).get("DisplayName"),
                            id=obj.get("Owner", {}).get("ID"),
                        )
                        if obj.get("Owner")
                        else None,
                    )
                    for obj in contents_data
                ]
            common_prefixes = None
            if common_prefixes_data := response.get("CommonPrefixes"):
                common_prefixes = [S3CommonPrefix(prefix=prefix.get("Prefix")) for prefix in common_prefixes_data]
            return S3ListObjectsResponse(
                is_truncated=response.get("IsTruncated"),
                marker=response.get("Marker"),
                next_marker=response.get("NextMarker"),
                contents=contents,
                name=response.get("Name"),
                prefix=response.get("Prefix"),
                delimiter=response.get("Delimiter"),
                max_keys=response.get("MaxKeys"),
                common_prefixes=common_prefixes,
                encoding_type=response.get("EncodingType"),
                request_charged=response.get("RequestCharged"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def list_objects_v2(
        self,
        bucket: str,
        delimiter: str | None = None,
        encoding_type: str | None = None,
        max_keys: int | None = None,
        prefix: str | None = None,
        continuation_token: str | None = None,
        fetch_owner: bool | None = None,
        start_after: str | None = None,
        request_payer: str | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3ListObjectsV2Response:
        """List some or all (up to 1,000) of the objects in a bucket using version 2 API."""
        kwargs = self._build_kwargs(
            Bucket=bucket,
            Delimiter=delimiter,
            EncodingType=encoding_type,
            MaxKeys=max_keys,
            Prefix=prefix,
            ContinuationToken=continuation_token,
            FetchOwner=fetch_owner,
            StartAfter=start_after,
            RequestPayer=request_payer,
            ExpectedBucketOwner=expected_bucket_owner,
        )
        try:
            response = await self._client.list_objects_v2(**kwargs)
            contents = None
            if contents_data := response.get("Contents"):
                contents = [
                    S3Object(
                        key=obj.get("Key"),
                        last_modified=obj.get("LastModified"),
                        etag=obj.get("ETag"),
                        checksum_algorithm=obj.get("ChecksumAlgorithm"),
                        size=obj.get("Size"),
                        storage_class=obj.get("StorageClass"),
                        owner=S3Owner(
                            display_name=obj.get("Owner", {}).get("DisplayName"),
                            id=obj.get("Owner", {}).get("ID"),
                        )
                        if obj.get("Owner")
                        else None,
                    )
                    for obj in contents_data
                ]
            common_prefixes = None
            if common_prefixes_data := response.get("CommonPrefixes"):
                common_prefixes = [S3CommonPrefix(prefix=prefix.get("Prefix")) for prefix in common_prefixes_data]
            return S3ListObjectsV2Response(
                is_truncated=response.get("IsTruncated"),
                contents=contents,
                name=response.get("Name"),
                prefix=response.get("Prefix"),
                delimiter=response.get("Delimiter"),
                max_keys=response.get("MaxKeys"),
                common_prefixes=common_prefixes,
                encoding_type=response.get("EncodingType"),
                key_count=response.get("KeyCount"),
                continuation_token=response.get("ContinuationToken"),
                next_continuation_token=response.get("NextContinuationToken"),
                start_after=response.get("StartAfter"),
                request_charged=response.get("RequestCharged"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def put_bucket_acl(  # noqa: C901, PLR0912
        self,
        bucket: str,
        acl: str | None = None,
        access_control_policy: S3AccessControlPolicy | None = None,
        content_md5: str | None = None,
        checksum_algorithm: str | None = None,
        grant_full_control: str | None = None,
        grant_read: str | None = None,
        grant_read_acp: str | None = None,
        grant_write: str | None = None,
        grant_write_acp: str | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3PutBucketAclResponse:
        """Put the ACL for a bucket."""
        kwargs: dict[str, Any] = {"Bucket": bucket}
        if acl:
            kwargs["ACL"] = acl
        if access_control_policy:
            policy: dict[str, Any] = {}
            if hasattr(access_control_policy, "grants") and access_control_policy.grants:
                policy["Grants"] = access_control_policy.grants
            if hasattr(access_control_policy, "owner") and access_control_policy.owner:
                policy["Owner"] = access_control_policy.owner
            if policy:
                kwargs["AccessControlPolicy"] = policy
        if content_md5:
            kwargs["ContentMD5"] = content_md5
        if checksum_algorithm:
            kwargs["ChecksumAlgorithm"] = checksum_algorithm
        if grant_full_control:
            kwargs["GrantFullControl"] = grant_full_control
        if grant_read:
            kwargs["GrantRead"] = grant_read
        if grant_read_acp:
            kwargs["GrantReadACP"] = grant_read_acp
        if grant_write:
            kwargs["GrantWrite"] = grant_write
        if grant_write_acp:
            kwargs["GrantWriteACP"] = grant_write_acp
        if expected_bucket_owner:
            kwargs["ExpectedBucketOwner"] = expected_bucket_owner

        try:
            response = await self._client.put_bucket_acl(**kwargs)
            return S3PutBucketAclResponse(
                request_charged=response.get("RequestCharged"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def put_bucket_cors(
        self,
        bucket: str,
        cors_configuration: S3CORSConfiguration,
        content_md5: str | None = None,
        checksum_algorithm: str | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3PutBucketCorsResponse:
        """Put the CORS configuration for a bucket."""
        cors_dict: dict[str, Any] = {
            "CORSRules": [
                {
                    "AllowedMethods": rule.allowed_methods,
                    "AllowedOrigins": rule.allowed_origins,
                    **({"AllowedHeaders": rule.allowed_headers} if rule.allowed_headers else {}),
                    **({"ExposeHeaders": rule.expose_headers} if rule.expose_headers else {}),
                    **({"Id": rule.id} if rule.id else {}),
                    **({"MaxAgeSeconds": rule.max_age_seconds} if rule.max_age_seconds else {}),
                }
                for rule in cors_configuration.cors_rules
            ]
        }
        kwargs: dict[str, Any] = {"Bucket": bucket, "CORSConfiguration": cors_dict}
        if content_md5:
            kwargs["ContentMD5"] = content_md5
        if checksum_algorithm:
            kwargs["ChecksumAlgorithm"] = checksum_algorithm
        if expected_bucket_owner:
            kwargs["ExpectedBucketOwner"] = expected_bucket_owner

        try:
            response = await self._client.put_bucket_cors(**kwargs)
            return S3PutBucketCorsResponse(
                request_charged=response.get("RequestCharged"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def put_bucket_lifecycle_configuration(  # noqa: C901, PLR0912, PLR0915
        self,
        bucket: str,
        lifecycle_configuration: S3LifecycleConfiguration | None = None,
        checksum_algorithm: str | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3PutBucketLifecycleConfigurationResponse:
        """Put the lifecycle configuration for a bucket."""
        kwargs: dict[str, Any] = {"Bucket": bucket}
        if lifecycle_configuration:
            rules_list: list[dict[str, Any]] = []
            for rule in lifecycle_configuration.rules:
                rule_dict: dict[str, Any] = {
                    "ID": rule.id,  # boto3 expects "ID" not "Id"
                    "Status": rule.status,
                }
                if rule.abort_incomplete_multipart_upload:
                    rule_dict["AbortIncompleteMultipartUpload"] = rule.abort_incomplete_multipart_upload
                if rule.expiration:
                    expiration_dict: dict[str, Any] = {}
                    if rule.expiration.date:
                        expiration_dict["Date"] = rule.expiration.date
                    if rule.expiration.days is not None:
                        expiration_dict["Days"] = rule.expiration.days
                    if rule.expiration.expired_object_delete_marker is not None:
                        expiration_dict["ExpiredObjectDeleteMarker"] = rule.expiration.expired_object_delete_marker
                    if expiration_dict:
                        rule_dict["Expiration"] = expiration_dict
                if rule.filter:
                    filter_dict: dict[str, Any] = {}
                    if rule.filter.and_:
                        filter_dict["And"] = rule.filter.and_
                    if rule.filter.prefix is not None:
                        filter_dict["Prefix"] = rule.filter.prefix
                    if rule.filter.tag:
                        filter_dict["Tag"] = rule.filter.tag
                    if rule.filter.object_size_greater_than is not None:
                        filter_dict["ObjectSizeGreaterThan"] = rule.filter.object_size_greater_than
                    if rule.filter.object_size_less_than is not None:
                        filter_dict["ObjectSizeLessThan"] = rule.filter.object_size_less_than
                    # Only add Filter if it has at least one non-None field
                    # Empty filter is invalid in S3
                    if filter_dict:
                        rule_dict["Filter"] = filter_dict
                if rule.noncurrent_version_expiration:
                    nve_dict: dict[str, Any] = {}
                    if rule.noncurrent_version_expiration.noncurrent_days is not None:
                        nve_dict["NoncurrentDays"] = rule.noncurrent_version_expiration.noncurrent_days
                    if rule.noncurrent_version_expiration.newer_noncurrent_versions is not None:
                        nve_dict["NewerNoncurrentVersions"] = (
                            rule.noncurrent_version_expiration.newer_noncurrent_versions
                        )
                    if nve_dict:
                        rule_dict["NoncurrentVersionExpiration"] = nve_dict
                if rule.noncurrent_version_transitions:
                    nvt_list: list[dict[str, Any]] = []
                    for nvt in rule.noncurrent_version_transitions:
                        nvt_dict: dict[str, Any] = {}
                        if nvt.noncurrent_days is not None:
                            nvt_dict["NoncurrentDays"] = nvt.noncurrent_days
                        if nvt.storage_class:
                            nvt_dict["StorageClass"] = nvt.storage_class
                        if nvt.newer_noncurrent_versions is not None:
                            nvt_dict["NewerNoncurrentVersions"] = nvt.newer_noncurrent_versions
                        if nvt_dict:
                            nvt_list.append(nvt_dict)
                    if nvt_list:
                        rule_dict["NoncurrentVersionTransitions"] = nvt_list
                if rule.transitions:
                    trans_list: list[dict[str, Any]] = []
                    for trans in rule.transitions:
                        trans_dict: dict[str, Any] = {}
                        if trans.date:
                            trans_dict["Date"] = trans.date
                        if trans.days is not None:
                            trans_dict["Days"] = trans.days
                        if trans.storage_class:
                            trans_dict["StorageClass"] = trans.storage_class
                        if trans_dict:
                            trans_list.append(trans_dict)
                    if trans_list:
                        rule_dict["Transitions"] = trans_list
                rules_list.append(rule_dict)
            kwargs["LifecycleConfiguration"] = {"Rules": rules_list}
        if checksum_algorithm:
            kwargs["ChecksumAlgorithm"] = checksum_algorithm
        if expected_bucket_owner:
            kwargs["ExpectedBucketOwner"] = expected_bucket_owner

        try:
            response = await self._client.put_bucket_lifecycle_configuration(**kwargs)
            return S3PutBucketLifecycleConfigurationResponse(
                request_charged=response.get("RequestCharged"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def put_bucket_policy(
        self,
        bucket: str,
        policy: str,
        content_md5: str | None = None,
        checksum_algorithm: str | None = None,
        confirm_remove_self_bucket_access: bool | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3PutBucketPolicyResponse:
        """Put the policy for a bucket."""
        kwargs = self._build_kwargs(
            Bucket=bucket,
            Policy=policy,
            ContentMD5=content_md5,
            ChecksumAlgorithm=checksum_algorithm,
            ConfirmRemoveSelfBucketAccess=confirm_remove_self_bucket_access,
            ExpectedBucketOwner=expected_bucket_owner,
        )
        try:
            response = await self._client.put_bucket_policy(**kwargs)
            return S3PutBucketPolicyResponse(
                request_charged=response.get("RequestCharged"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def put_object(  # noqa: C901, PLR0912, PLR0915
        self,
        bucket: str,
        key: str,
        body: bytes | str | None = None,
        acl: str | None = None,
        cache_control: str | None = None,
        content_disposition: str | None = None,
        content_encoding: str | None = None,
        content_language: str | None = None,
        content_length: int | None = None,
        content_md5: str | None = None,
        content_type: str | None = None,
        checksum_algorithm: str | None = None,
        checksum_crc32: str | None = None,
        checksum_crc32c: str | None = None,
        checksum_sha1: str | None = None,
        checksum_sha256: str | None = None,
        expires: datetime | None = None,
        grant_full_control: str | None = None,
        grant_read: str | None = None,
        grant_read_acp: str | None = None,
        grant_write_acp: str | None = None,
        metadata: dict[str, str] | None = None,
        server_side_encryption: str | None = None,
        storage_class: str | None = None,
        website_redirect_location: str | None = None,
        sse_customer_algorithm: str | None = None,
        sse_customer_key: str | None = None,
        sse_kms_key_id: str | None = None,
        sse_kms_encryption_context: str | None = None,
        bucket_key_enabled: bool | None = None,
        request_payer: str | None = None,
        tagging: str | None = None,
        object_lock_mode: str | None = None,
        object_lock_retain_until_date: datetime | None = None,
        object_lock_legal_hold_status: str | None = None,
        expected_bucket_owner: str | None = None,
        metadata_directive: str | None = None,
    ) -> S3PutObjectResponse:
        """Put an object into a bucket."""
        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if body is not None:
            if isinstance(body, str):
                kwargs["Body"] = body.encode("utf-8")
            else:
                kwargs["Body"] = body
        if acl:
            kwargs["ACL"] = acl
        if cache_control:
            kwargs["CacheControl"] = cache_control
        if content_disposition:
            kwargs["ContentDisposition"] = content_disposition
        if content_encoding:
            kwargs["ContentEncoding"] = content_encoding
        if content_language:
            kwargs["ContentLanguage"] = content_language
        if content_length is not None:
            kwargs["ContentLength"] = content_length
        if content_md5:
            kwargs["ContentMD5"] = content_md5
        if content_type:
            kwargs["ContentType"] = content_type
        if checksum_algorithm:
            kwargs["ChecksumAlgorithm"] = checksum_algorithm
        if checksum_crc32:
            kwargs["ChecksumCRC32"] = checksum_crc32
        if checksum_crc32c:
            kwargs["ChecksumCRC32C"] = checksum_crc32c
        if checksum_sha1:
            kwargs["ChecksumSHA1"] = checksum_sha1
        if checksum_sha256:
            kwargs["ChecksumSHA256"] = checksum_sha256
        if expires:
            kwargs["Expires"] = expires
        if grant_full_control:
            kwargs["GrantFullControl"] = grant_full_control
        if grant_read:
            kwargs["GrantRead"] = grant_read
        if grant_read_acp:
            kwargs["GrantReadACP"] = grant_read_acp
        if grant_write_acp:
            kwargs["GrantWriteACP"] = grant_write_acp
        if metadata:
            kwargs["Metadata"] = metadata
        if server_side_encryption:
            kwargs["ServerSideEncryption"] = server_side_encryption
        if storage_class:
            kwargs["StorageClass"] = storage_class
        if website_redirect_location:
            kwargs["WebsiteRedirectLocation"] = website_redirect_location
        if sse_customer_algorithm:
            kwargs["SSECustomerAlgorithm"] = sse_customer_algorithm
        if sse_customer_key:
            kwargs["SSECustomerKey"] = sse_customer_key
        if sse_kms_key_id:
            kwargs["SSEKMSKeyId"] = sse_kms_key_id
        if sse_kms_encryption_context:
            kwargs["SSEKMSEncryptionContext"] = sse_kms_encryption_context
        if bucket_key_enabled is not None:
            kwargs["BucketKeyEnabled"] = bucket_key_enabled
        if request_payer:
            kwargs["RequestPayer"] = request_payer
        if tagging:
            kwargs["Tagging"] = tagging
        if object_lock_mode:
            kwargs["ObjectLockMode"] = object_lock_mode
        if object_lock_retain_until_date:
            kwargs["ObjectLockRetainUntilDate"] = object_lock_retain_until_date
        if object_lock_legal_hold_status:
            kwargs["ObjectLockLegalHoldStatus"] = object_lock_legal_hold_status
        if expected_bucket_owner:
            kwargs["ExpectedBucketOwner"] = expected_bucket_owner
        if metadata_directive:
            kwargs["MetadataDirective"] = metadata_directive

        try:
            response = await self._client.put_object(**kwargs)
            return S3PutObjectResponse(
                etag=response.get("ETag"),
                checksum_crc32=response.get("ChecksumCRC32"),
                checksum_crc32c=response.get("ChecksumCRC32C"),
                checksum_sha1=response.get("ChecksumSHA1"),
                checksum_sha256=response.get("ChecksumSHA256"),
                expiration=response.get("Expiration"),
                request_charged=response.get("RequestCharged"),
                sse_customer_algorithm=response.get("SSECustomerAlgorithm"),
                sse_customer_key_md5=response.get("SSECustomerKeyMD5"),
                sse_kms_key_id=response.get("SSEKMSKeyId"),
                sse_kms_encryption_context=response.get("SSEKMSEncryptionContext"),
                bucket_key_enabled=response.get("BucketKeyEnabled"),
                server_side_encryption=response.get("ServerSideEncryption"),
                version_id=response.get("VersionId"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def put_object_acl(  # noqa: C901, PLR0912
        self,
        bucket: str,
        key: str,
        acl: str | None = None,
        access_control_policy: S3AccessControlPolicy | None = None,
        content_md5: str | None = None,
        checksum_algorithm: str | None = None,
        grant_full_control: str | None = None,
        grant_read: str | None = None,
        grant_read_acp: str | None = None,
        grant_write: str | None = None,
        grant_write_acp: str | None = None,
        request_payer: str | None = None,
        version_id: str | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3PutObjectAclResponse:
        """Put the ACL for an object."""
        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if acl:
            kwargs["ACL"] = acl
        if access_control_policy:
            policy: dict[str, Any] = {}
            if hasattr(access_control_policy, "grants") and access_control_policy.grants:
                policy["Grants"] = access_control_policy.grants
            if hasattr(access_control_policy, "owner") and access_control_policy.owner:
                policy["Owner"] = access_control_policy.owner
            if policy:
                kwargs["AccessControlPolicy"] = policy
        if content_md5:
            kwargs["ContentMD5"] = content_md5
        if checksum_algorithm:
            kwargs["ChecksumAlgorithm"] = checksum_algorithm
        if grant_full_control:
            kwargs["GrantFullControl"] = grant_full_control
        if grant_read:
            kwargs["GrantRead"] = grant_read
        if grant_read_acp:
            kwargs["GrantReadACP"] = grant_read_acp
        if grant_write:
            kwargs["GrantWrite"] = grant_write
        if grant_write_acp:
            kwargs["GrantWriteACP"] = grant_write_acp
        if request_payer:
            kwargs["RequestPayer"] = request_payer
        if version_id:
            kwargs["VersionId"] = version_id
        if expected_bucket_owner:
            kwargs["ExpectedBucketOwner"] = expected_bucket_owner

        try:
            response = await self._client.put_object_acl(**kwargs)
            return S3PutObjectAclResponse(
                request_charged=response.get("RequestCharged"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def put_object_legal_hold(
        self,
        bucket: str,
        key: str,
        legal_hold: S3ObjectLockLegalHold | None = None,
        request_payer: str | None = None,
        version_id: str | None = None,
        expected_bucket_owner: str | None = None,
        content_md5: str | None = None,
        checksum_algorithm: str | None = None,
    ) -> S3PutObjectLegalHoldResponse:
        """Put the legal hold for an object."""
        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if legal_hold:
            legal_hold_dict: dict[str, Any] = {}
            if hasattr(legal_hold, "status") and legal_hold.status:
                legal_hold_dict["Status"] = legal_hold.status
            if legal_hold_dict:
                kwargs["LegalHold"] = legal_hold_dict
        if request_payer:
            kwargs["RequestPayer"] = request_payer
        if version_id:
            kwargs["VersionId"] = version_id
        if expected_bucket_owner:
            kwargs["ExpectedBucketOwner"] = expected_bucket_owner
        if content_md5:
            kwargs["ContentMD5"] = content_md5
        if checksum_algorithm:
            kwargs["ChecksumAlgorithm"] = checksum_algorithm

        try:
            response = await self._client.put_object_legal_hold(**kwargs)
            return S3PutObjectLegalHoldResponse(
                request_charged=response.get("RequestCharged"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def put_object_lock_configuration(  # noqa: C901, PLR0912
        self,
        bucket: str,
        object_lock_configuration: S3ObjectLockConfiguration | None = None,
        request_payer: str | None = None,
        token: str | None = None,
        content_md5: str | None = None,
        checksum_algorithm: str | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3PutObjectLockConfigurationResponse:
        """Put the lock configuration for a bucket."""
        kwargs: dict[str, Any] = {"Bucket": bucket}
        if object_lock_configuration:
            config_dict: dict[str, Any] = {}
            if (
                hasattr(object_lock_configuration, "object_lock_enabled")
                and object_lock_configuration.object_lock_enabled
            ):
                config_dict["ObjectLockEnabled"] = object_lock_configuration.object_lock_enabled
            if hasattr(object_lock_configuration, "rule") and object_lock_configuration.rule:
                rule_dict: dict[str, Any] = {}
                if (
                    hasattr(object_lock_configuration.rule, "default_retention")
                    and object_lock_configuration.rule.default_retention
                ):
                    retention_dict: dict[str, Any] = {}
                    if (
                        hasattr(object_lock_configuration.rule.default_retention, "mode")
                        and object_lock_configuration.rule.default_retention.mode
                    ):
                        retention_dict["Mode"] = object_lock_configuration.rule.default_retention.mode
                    if (
                        hasattr(object_lock_configuration.rule.default_retention, "days")
                        and object_lock_configuration.rule.default_retention.days
                    ):
                        retention_dict["Days"] = object_lock_configuration.rule.default_retention.days
                    if (
                        hasattr(object_lock_configuration.rule.default_retention, "years")
                        and object_lock_configuration.rule.default_retention.years
                    ):
                        retention_dict["Years"] = object_lock_configuration.rule.default_retention.years
                    if retention_dict:
                        rule_dict["DefaultRetention"] = retention_dict
                if rule_dict:
                    config_dict["Rule"] = rule_dict
            if config_dict:
                kwargs["ObjectLockConfiguration"] = config_dict
        if request_payer:
            kwargs["RequestPayer"] = request_payer
        if token:
            kwargs["Token"] = token
        if content_md5:
            kwargs["ContentMD5"] = content_md5
        if checksum_algorithm:
            kwargs["ChecksumAlgorithm"] = checksum_algorithm
        if expected_bucket_owner:
            kwargs["ExpectedBucketOwner"] = expected_bucket_owner

        try:
            response = await self._client.put_object_lock_configuration(**kwargs)
            return S3PutObjectLockConfigurationResponse(
                request_charged=response.get("RequestCharged"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking

    async def put_object_retention(  # noqa: C901
        self,
        bucket: str,
        key: str,
        retention: S3ObjectLockRetention | None = None,
        request_payer: str | None = None,
        version_id: str | None = None,
        bypass_governance_retention: bool | None = None,
        expected_bucket_owner: str | None = None,
        content_md5: str | None = None,
        checksum_algorithm: str | None = None,
    ) -> S3PutObjectRetentionResponse:
        """Put the retention for an object."""
        kwargs: dict[str, Any] = {"Bucket": bucket, "Key": key}
        if retention:
            retention_dict: dict[str, Any] = {}
            if hasattr(retention, "mode") and retention.mode:
                retention_dict["Mode"] = retention.mode
            if hasattr(retention, "retain_until_date") and retention.retain_until_date:
                retention_dict["RetainUntilDate"] = retention.retain_until_date
            if retention_dict:
                kwargs["Retention"] = retention_dict
        if request_payer:
            kwargs["RequestPayer"] = request_payer
        if version_id:
            kwargs["VersionId"] = version_id
        if bypass_governance_retention is not None:
            kwargs["BypassGovernanceRetention"] = bypass_governance_retention
        if expected_bucket_owner:
            kwargs["ExpectedBucketOwner"] = expected_bucket_owner
        if content_md5:
            kwargs["ContentMD5"] = content_md5
        if checksum_algorithm:
            kwargs["ChecksumAlgorithm"] = checksum_algorithm

        try:
            response = await self._client.put_object_retention(**kwargs)
            return S3PutObjectRetentionResponse(
                request_charged=response.get("RequestCharged"),
            )
        except ClientError as e:
            self._handle_client_error(e)
            raise  # This should never be reached, but for type checking
