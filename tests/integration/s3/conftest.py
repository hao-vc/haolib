"""Conftest for S3 tests."""

import urllib.parse
from collections.abc import AsyncGenerator
from datetime import UTC, datetime
from types import TracebackType
from typing import Any, Literal, Self

import pytest
import pytest_asyncio
from dishka import AsyncContainer

from haolib.database.files.s3.clients.abstract import (
    AbstractS3Client,
    S3BucketAlreadyExistsClientException,
    S3BucketNotEmptyClientException,
    S3NoSuchBucketClientException,
    S3NoSuchCORSConfigurationClientException,
    S3NoSuchKeyClientException,
    S3NoSuchLifecycleConfigurationClientException,
    S3NoSuchPolicyClientException,
)
from haolib.database.files.s3.clients.aioboto3 import (
    Aioboto3S3Client,
)
from haolib.database.files.s3.clients.pydantic import (
    S3AccessControlPolicy,
    S3Bucket,
    S3CopyObjectResponse,
    S3CopyObjectResult,
    S3CORSConfiguration,
    S3CORSRule,
    S3CreateBucketConfiguration,
    S3CreateBucketResponse,
    S3DefaultRetention,
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
    S3ObjectLockRule,
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
from tests.integration.conftest import MockAppConfig


class MockS3Client(AbstractS3Client):
    """Mock S3 client for testing."""

    def __init__(self, endpoint_url: str | None = None) -> None:
        """Initialize the mock client.

        Args:
            endpoint_url: Custom S3 endpoint URL (e.g., for MinIO or LocalStack).

        """
        self._buckets: dict[str, dict[str, Any]] = {}
        self._objects: dict[tuple[str, str], dict[str, Any]] = {}
        self._bucket_acls: dict[str, S3GetBucketAclResponse] = {}
        self._object_acls: dict[tuple[str, str], S3GetObjectAclResponse] = {}
        self._bucket_cors: dict[str, S3GetBucketCorsResponse] = {}
        self._bucket_lifecycle: dict[str, S3GetBucketLifecycleConfigurationResponse] = {}
        self._bucket_policies: dict[str, S3GetBucketPolicyResponse] = {}
        self._object_lock_configs: dict[str, S3ObjectLockConfiguration] = {}
        self._endpoint_url = endpoint_url

    async def __aenter__(self) -> Self:
        """Enter the context manager."""
        return self

    async def __aexit__(
        self, exc_type: type[BaseException] | None, exc_value: BaseException | None, traceback: TracebackType | None
    ) -> None:
        """Exit the context manager."""
        return

    async def copy_object(
        self,
        bucket: str,
        copy_source: str,
        key: str,
        acl: Literal[
            "private",
            "public-read",
            "public-read-write",
            "authenticated-read",
            "aws-exec-read",
            "bucket-owner-read",
            "bucket-owner-full-control",
        ]
        | None = None,
        cache_control: str | None = None,
        checksum_algorithm: Literal["CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME"] | None = None,
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
        metadata_directive: Literal["COPY", "REPLACE"] | None = None,
        tagging_directive: Literal["COPY", "REPLACE"] | None = None,
        server_side_encryption: Literal["AES256", "aws:fsx", "aws:kms", "aws:kms:dsse"] | None = None,
        storage_class: Literal[
            "STANDARD",
            "REDUCED_REDUNDANCY",
            "STANDARD_IA",
            "ONEZONE_IA",
            "INTELLIGENT_TIERING",
            "GLACIER",
            "DEEP_ARCHIVE",
            "OUTPOSTS",
            "GLACIER_IR",
            "SNOW",
            "EXPRESS_ONEZONE",
            "FSX_OPENZFS",
        ]
        | None = None,
        website_redirect_location: str | None = None,
        sse_customer_algorithm: str | None = None,
        sse_customer_key: str | None = None,
        sse_kms_key_id: str | None = None,
        sse_kms_encryption_context: str | None = None,
        bucket_key_enabled: bool | None = None,
        copy_source_sse_customer_algorithm: str | None = None,
        copy_source_sse_customer_key: str | None = None,
        request_payer: Literal["requester", "requester_payer"] | None = None,
        tagging: str | None = None,
        object_lock_mode: Literal["GOVERNANCE", "COMPLIANCE"] | None = None,
        object_lock_retain_until_date: datetime | None = None,
        object_lock_legal_hold_status: Literal["ON", "OFF"] | None = None,
        expected_bucket_owner: str | None = None,
        expected_source_bucket_owner: str | None = None,
    ) -> S3CopyObjectResponse:
        """Copy an object."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        source_bucket, source_key = copy_source.split("/", 1)

        if (source_bucket, source_key) not in self._objects:
            raise S3NoSuchKeyClientException(f"Object {copy_source} does not exist")

        source_obj = self._objects[(source_bucket, source_key)]

        self._objects[(bucket, key)] = {**source_obj, "key": key}

        return S3CopyObjectResponse(
            copy_object_result=S3CopyObjectResult(
                etag="mock-etag",
                last_modified=datetime.now(UTC),
            ),
            version_id="mock-version-id",
        )

    async def create_bucket(
        self,
        bucket: str,
        acl: Literal["private", "public-read", "public-read-write", "authenticated-read"] | None = None,
        create_bucket_configuration: S3CreateBucketConfiguration | None = None,
        grant_full_control: str | None = None,
        grant_read: str | None = None,
        grant_read_acp: str | None = None,
        grant_write: str | None = None,
        grant_write_acp: str | None = None,
        object_lock_enabled_for_bucket: bool | None = None,
        object_ownership: Literal["BucketOwnerPreferred", "ObjectWriter", "BucketOwnerEnforced"] | None = None,
    ) -> S3CreateBucketResponse:
        """Create a bucket."""
        if bucket in self._buckets:
            raise S3BucketAlreadyExistsClientException(f"Bucket {bucket} already exists")

        self._buckets[bucket] = {"name": bucket, "created": datetime.now(UTC)}

        return S3CreateBucketResponse(location=f"/{bucket}")

    async def delete_bucket(
        self,
        bucket: str,
        expected_bucket_owner: str | None = None,
    ) -> S3DeleteBucketResponse:
        """Delete a bucket."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        # Check if bucket has objects
        if any((b, k) in self._objects for b, k in self._objects if b == bucket):
            raise S3BucketNotEmptyClientException(f"Bucket {bucket} is not empty")

        del self._buckets[bucket]

        return S3DeleteBucketResponse()

    async def delete_bucket_cors(
        self,
        bucket: str,
        expected_bucket_owner: str | None = None,
    ) -> S3DeleteBucketCorsResponse:
        """Delete bucket CORS."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        if bucket not in self._bucket_cors:
            raise S3NoSuchCORSConfigurationClientException(f"CORS config for {bucket} does not exist")

        del self._bucket_cors[bucket]

        return S3DeleteBucketCorsResponse()

    async def delete_bucket_lifecycle(
        self,
        bucket: str,
        expected_bucket_owner: str | None = None,
    ) -> S3DeleteBucketLifecycleResponse:
        """Delete bucket lifecycle."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        if bucket not in self._bucket_lifecycle:
            raise S3NoSuchLifecycleConfigurationClientException(f"Lifecycle config for {bucket} does not exist")

        del self._bucket_lifecycle[bucket]

        return S3DeleteBucketLifecycleResponse()

    async def delete_bucket_policy(
        self,
        bucket: str,
        expected_bucket_owner: str | None = None,
    ) -> S3DeleteBucketPolicyResponse:
        """Delete bucket policy."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        if bucket not in self._bucket_policies:
            raise S3NoSuchPolicyClientException(f"Policy for {bucket} does not exist")

        del self._bucket_policies[bucket]

        return S3DeleteBucketPolicyResponse()

    async def delete_object(
        self,
        bucket: str,
        key: str,
        mfa: str | None = None,
        version_id: str | None = None,
        request_payer: Literal["requester", "requester_payer"] | None = None,
        bypass_governance_retention: bool | None = None,
        expected_bucket_owner: str | None = None,
        if_match: str | None = None,
        if_match_last_modified_time: datetime | None = None,
        if_match_size: int | None = None,
    ) -> S3DeleteObjectResponse:
        """Delete an object."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        # S3 doesn't raise an exception when deleting a non-existent object
        # It just succeeds (this is correct S3 behavior)
        if (bucket, key) in self._objects:
            del self._objects[(bucket, key)]

        return S3DeleteObjectResponse(delete_marker=False, version_id="mock-version-id")

    async def delete_objects(
        self,
        bucket: str,
        delete: S3DeleteObjectsDelete,
        mfa: str | None = None,
        request_payer: Literal["requester", "requester_payer"] | None = None,
        bypass_governance_retention: bool | None = None,
        expected_bucket_owner: str | None = None,
        checksum_algorithm: Literal["CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME"] | None = None,
    ) -> S3DeleteObjectsResponse:
        """Delete objects."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        deleted = []
        errors = []

        for obj in delete.objects:
            if (bucket, obj.key) in self._objects:
                del self._objects[(bucket, obj.key)]
                deleted.append(
                    S3DeleteObjectsResponseDeletedItem(
                        key=obj.key,
                        version_id=obj.version_id or "",
                        delete_marker=False,
                        delete_marker_version_id="",
                    )
                )
            else:
                errors.append(
                    S3DeleteObjectsResponseErrorItem(
                        key=obj.key,
                        version_id=obj.version_id or "",
                        code="NoSuchKey",
                        message="Not found",
                    )
                )

        return S3DeleteObjectsResponse(deleted=deleted, error=errors)

    async def get_bucket_acl(
        self,
        bucket: str,
        expected_bucket_owner: str | None = None,
    ) -> S3GetBucketAclResponse:
        """Get bucket ACL."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        default_acl = S3GetBucketAclResponse(
            grants=[],
            owner=S3Owner(display_name="mock-owner", id="mock-id"),
        )
        return self._bucket_acls.get(bucket, default_acl)

    async def get_bucket_cors(
        self,
        bucket: str,
        expected_bucket_owner: str | None = None,
    ) -> S3GetBucketCorsResponse:
        """Get bucket CORS."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        if bucket not in self._bucket_cors:
            raise S3NoSuchCORSConfigurationClientException(f"CORS config for {bucket} does not exist")

        return self._bucket_cors[bucket]

    async def get_bucket_lifecycle_configuration(
        self,
        bucket: str,
        expected_bucket_owner: str | None = None,
    ) -> S3GetBucketLifecycleConfigurationResponse:
        """Get bucket lifecycle."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        if bucket not in self._bucket_lifecycle:
            return S3GetBucketLifecycleConfigurationResponse(rules=[])

        return self._bucket_lifecycle[bucket]

    async def get_bucket_policy(
        self,
        bucket: str,
        expected_bucket_owner: str | None = None,
    ) -> S3GetBucketPolicyResponse:
        """Get bucket policy."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        if bucket not in self._bucket_policies:
            raise S3NoSuchPolicyClientException(f"Policy for {bucket} does not exist")

        return self._bucket_policies[bucket]

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
        request_payer: Literal["requester", "requester_payer"] | None = None,
        expected_bucket_owner: str | None = None,
        checksum_mode: Literal["ENABLED"] | None = None,
    ) -> S3GetObjectResponse:
        """Get an object."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        if (bucket, key) not in self._objects:
            raise S3NoSuchKeyClientException(f"Object {key} does not exist in bucket {bucket}")

        obj = self._objects[(bucket, key)]

        return S3GetObjectResponse(
            body=obj.get("body", b""),
            content_length=len(obj.get("body", b"")),
            etag=obj.get("etag", "mock-etag"),
            last_modified=obj.get("last_modified", datetime.now(UTC)),
            content_type=obj.get("content_type", "application/octet-stream"),
        )

    async def get_object_acl(
        self,
        bucket: str,
        key: str,
        version_id: str | None = None,
        request_payer: Literal["requester", "requester_payer"] | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3GetObjectAclResponse:
        """Get object ACL."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        if (bucket, key) not in self._objects:
            raise S3NoSuchKeyClientException(f"Object {key} does not exist in bucket {bucket}")

        default_acl = S3GetObjectAclResponse(
            grants=[],
            owner=S3Owner(display_name="mock-owner", id="mock-id"),
        )
        return self._object_acls.get((bucket, key), default_acl)

    async def list_buckets(
        self,
    ) -> S3ListBucketsResponse:
        """List buckets."""
        buckets = [S3Bucket(name=name, creation_date=info["created"]) for name, info in self._buckets.items()]

        return S3ListBucketsResponse(
            buckets=buckets,
            owner=S3Owner(display_name="mock-owner", id="mock-id"),
        )

    async def list_objects(
        self,
        bucket: str,
        delimiter: str | None = None,
        encoding_type: Literal["url"] | None = None,
        marker: str | None = None,
        max_keys: int | None = None,
        prefix: str | None = None,
        request_payer: Literal["requester", "requester_payer"] | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3ListObjectsResponse:
        """List objects."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        prefix = prefix or ""
        objects = [
            S3Object(
                key=key,
                last_modified=obj.get("last_modified", datetime.now(UTC)),
                size=len(obj.get("body", b"")),
            )
            for (b, key), obj in self._objects.items()
            if b == bucket and key.startswith(prefix)
        ]

        return S3ListObjectsResponse(contents=objects, is_truncated=False)

    async def list_objects_v2(
        self,
        bucket: str,
        delimiter: str | None = None,
        encoding_type: Literal["url"] | None = None,
        max_keys: int | None = None,
        prefix: str | None = None,
        continuation_token: str | None = None,
        fetch_owner: bool | None = None,
        start_after: str | None = None,
        request_payer: Literal["requester", "requester_payer"] | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3ListObjectsV2Response:
        """List objects v2."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        prefix = prefix or ""
        objects = [
            S3Object(
                key=key,
                last_modified=obj.get("last_modified", datetime.now(UTC)),
                size=len(obj.get("body", b"")),
            )
            for (b, key), obj in self._objects.items()
            if b == bucket and key.startswith(prefix)
        ]

        return S3ListObjectsV2Response(contents=objects, is_truncated=False, key_count=len(objects))

    async def put_bucket_acl(
        self,
        bucket: str,
        acl: Literal[
            "private",
            "public-read",
            "public-read-write",
            "authenticated-read",
            "aws-exec-read",
            "bucket-owner-read",
            "bucket-owner-full-control",
        ]
        | None = None,
        access_control_policy: S3AccessControlPolicy | None = None,
        content_md5: str | None = None,
        checksum_algorithm: Literal["CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME"] | None = None,
        grant_full_control: str | None = None,
        grant_read: str | None = None,
        grant_read_acp: str | None = None,
        grant_write: str | None = None,
        grant_write_acp: str | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3PutBucketAclResponse:
        """Put bucket ACL."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        # Store ACL
        acl_response = S3GetBucketAclResponse(
            grants=[],
            owner=S3Owner(display_name="mock-owner", id="mock-id"),
        )

        self._bucket_acls[bucket] = acl_response

        return S3PutBucketAclResponse()

    async def put_bucket_cors(
        self,
        bucket: str,
        cors_configuration: S3CORSConfiguration,
        content_md5: str | None = None,
        checksum_algorithm: Literal["CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME"] | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3PutBucketCorsResponse:
        """Put bucket CORS."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        cors_response = S3GetBucketCorsResponse(
            cors_rules=[
                S3CORSRule(
                    allowed_methods=list(rule.allowed_methods),
                    allowed_origins=list(rule.allowed_origins),
                    allowed_headers=list(rule.allowed_headers) if rule.allowed_headers else None,
                    expose_headers=list(rule.expose_headers) if rule.expose_headers else None,
                    id=rule.id,
                    max_age_seconds=rule.max_age_seconds,
                )
                for rule in cors_configuration.cors_rules
            ]
        )

        self._bucket_cors[bucket] = cors_response

        return S3PutBucketCorsResponse()

    async def put_bucket_lifecycle_configuration(
        self,
        bucket: str,
        lifecycle_configuration: S3LifecycleConfiguration | None = None,
        checksum_algorithm: Literal["CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME"] | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3PutBucketLifecycleConfigurationResponse:
        """Put bucket lifecycle."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        if lifecycle_configuration:
            lifecycle_response = S3GetBucketLifecycleConfigurationResponse(
                rules=[
                    S3LifecycleRule(
                        id=rule.id,
                        status=rule.status,
                        expiration=S3LifecycleExpiration(
                            date=rule.expiration.date,
                            days=rule.expiration.days,
                            expired_object_delete_marker=rule.expiration.expired_object_delete_marker,
                        )
                        if rule.expiration
                        else None,
                        filter=S3LifecycleRuleFilter(
                            and_=rule.filter.and_,
                            prefix=rule.filter.prefix,
                            tag=rule.filter.tag,
                            object_size_greater_than=rule.filter.object_size_greater_than,
                            object_size_less_than=rule.filter.object_size_less_than,
                        )
                        if rule.filter
                        else None,
                        noncurrent_version_expiration=S3NoncurrentVersionExpiration(
                            noncurrent_days=rule.noncurrent_version_expiration.noncurrent_days,
                            newer_noncurrent_versions=rule.noncurrent_version_expiration.newer_noncurrent_versions,
                        )
                        if rule.noncurrent_version_expiration
                        else None,
                        noncurrent_version_transitions=[
                            S3NoncurrentVersionTransition(
                                noncurrent_days=trans.noncurrent_days,
                                storage_class=trans.storage_class,
                                newer_noncurrent_versions=trans.newer_noncurrent_versions,
                            )
                            for trans in rule.noncurrent_version_transitions
                        ]
                        if rule.noncurrent_version_transitions
                        else None,
                        transitions=[
                            S3LifecycleTransition(
                                date=trans.date,
                                days=trans.days,
                                storage_class=trans.storage_class,
                            )
                            for trans in rule.transitions
                        ]
                        if rule.transitions
                        else None,
                    )
                    for rule in lifecycle_configuration.rules
                ]
            )

            self._bucket_lifecycle[bucket] = lifecycle_response

        return S3PutBucketLifecycleConfigurationResponse()

    async def put_bucket_policy(
        self,
        bucket: str,
        policy: str,
        content_md5: str | None = None,
        checksum_algorithm: Literal["CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME"] | None = None,
        confirm_remove_self_bucket_access: bool | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3PutBucketPolicyResponse:
        """Put bucket policy."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        policy_response = S3GetBucketPolicyResponse(policy=policy, revision_id="mock-revision-id")

        self._bucket_policies[bucket] = policy_response

        return S3PutBucketPolicyResponse()

    async def put_object(
        self,
        bucket: str,
        key: str,
        body: bytes | str | None = None,
        acl: Literal[
            "private",
            "public-read",
            "public-read-write",
            "authenticated-read",
            "aws-exec-read",
            "bucket-owner-read",
            "bucket-owner-full-control",
        ]
        | None = None,
        cache_control: str | None = None,
        content_disposition: str | None = None,
        content_encoding: str | None = None,
        content_language: str | None = None,
        content_length: int | None = None,
        content_md5: str | None = None,
        content_type: str | None = None,
        checksum_algorithm: Literal["CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME"] | None = None,
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
        server_side_encryption: Literal["AES256", "aws:fsx", "aws:kms", "aws:kms:dsse"] | None = None,
        storage_class: Literal[
            "STANDARD",
            "REDUCED_REDUNDANCY",
            "STANDARD_IA",
            "ONEZONE_IA",
            "INTELLIGENT_TIERING",
            "GLACIER",
            "DEEP_ARCHIVE",
            "OUTPOSTS",
            "GLACIER_IR",
            "SNOW",
            "EXPRESS_ONEZONE",
            "FSX_OPENZFS",
        ]
        | None = None,
        website_redirect_location: str | None = None,
        sse_customer_algorithm: str | None = None,
        sse_customer_key: str | None = None,
        sse_kms_key_id: str | None = None,
        sse_kms_encryption_context: str | None = None,
        bucket_key_enabled: bool | None = None,
        request_payer: Literal["requester", "requester_payer"] | None = None,
        tagging: str | None = None,
        object_lock_mode: Literal["GOVERNANCE", "COMPLIANCE"] | None = None,
        object_lock_retain_until_date: datetime | None = None,
        object_lock_legal_hold_status: Literal["ON", "OFF"] | None = None,
        expected_bucket_owner: str | None = None,
        metadata_directive: Literal["COPY", "REPLACE"] | None = None,
    ) -> S3PutObjectResponse:
        """Put an object."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        body_bytes = body.encode() if isinstance(body, str) else (body or b"")

        self._objects[(bucket, key)] = {
            "body": body_bytes,
            "etag": "mock-etag",
            "last_modified": datetime.now(UTC),
            "content_type": content_type or "application/octet-stream",
        }

        return S3PutObjectResponse(etag="mock-etag", version_id="mock-version-id")

    async def put_object_acl(
        self,
        bucket: str,
        key: str,
        acl: Literal[
            "private",
            "public-read",
            "public-read-write",
            "authenticated-read",
            "aws-exec-read",
            "bucket-owner-read",
            "bucket-owner-full-control",
        ]
        | None = None,
        access_control_policy: S3AccessControlPolicy | None = None,
        content_md5: str | None = None,
        checksum_algorithm: Literal["CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME"] | None = None,
        grant_full_control: str | None = None,
        grant_read: str | None = None,
        grant_read_acp: str | None = None,
        grant_write: str | None = None,
        grant_write_acp: str | None = None,
        request_payer: Literal["requester", "requester_payer"] | None = None,
        version_id: str | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3PutObjectAclResponse:
        """Put object ACL."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        if (bucket, key) not in self._objects:
            raise S3NoSuchKeyClientException(f"Object {key} does not exist in bucket {bucket}")

        acl_response = S3GetObjectAclResponse(
            grants=[],
            owner=S3Owner(display_name="mock-owner", id="mock-id"),
        )

        self._object_acls[(bucket, key)] = acl_response

        return S3PutObjectAclResponse()

    async def put_object_legal_hold(
        self,
        bucket: str,
        key: str,
        legal_hold: S3ObjectLockLegalHold | None = None,
        request_payer: Literal["requester", "requester_payer"] | None = None,
        version_id: str | None = None,
        expected_bucket_owner: str | None = None,
        content_md5: str | None = None,
        checksum_algorithm: Literal["CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME"] | None = None,
    ) -> S3PutObjectLegalHoldResponse:
        """Put object legal hold."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        if (bucket, key) not in self._objects:
            raise S3NoSuchKeyClientException(f"Object {key} does not exist in bucket {bucket}")

        return S3PutObjectLegalHoldResponse()

    async def put_object_lock_configuration(
        self,
        bucket: str,
        object_lock_configuration: S3ObjectLockConfiguration | None = None,
        request_payer: Literal["requester", "requester_payer"] | None = None,
        token: str | None = None,
        content_md5: str | None = None,
        checksum_algorithm: Literal["CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME"] | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3PutObjectLockConfigurationResponse:
        """Put object lock configuration."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        if object_lock_configuration:
            self._object_lock_configs[bucket] = S3ObjectLockConfiguration(
                object_lock_enabled=object_lock_configuration.object_lock_enabled,
                rule=S3ObjectLockRule(
                    default_retention=S3DefaultRetention(
                        mode=object_lock_configuration.rule.default_retention.mode,
                        days=object_lock_configuration.rule.default_retention.days,
                        years=object_lock_configuration.rule.default_retention.years,
                    )
                    if object_lock_configuration.rule and object_lock_configuration.rule.default_retention
                    else None,
                ),
            )

        return S3PutObjectLockConfigurationResponse()

    async def put_object_retention(
        self,
        bucket: str,
        key: str,
        retention: S3ObjectLockRetention | None = None,
        request_payer: Literal["requester", "requester_payer"] | None = None,
        version_id: str | None = None,
        bypass_governance_retention: bool | None = None,
        expected_bucket_owner: str | None = None,
        content_md5: str | None = None,
        checksum_algorithm: Literal["CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME"] | None = None,
    ) -> S3PutObjectRetentionResponse:
        """Put object retention."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        if (bucket, key) not in self._objects:
            raise S3NoSuchKeyClientException(f"Object {key} does not exist in bucket {bucket}")

        return S3PutObjectRetentionResponse()

    def generate_presigned_url(
        self,
        bucket: str,
        key: str,
        client_method: Literal["get_object", "put_object"] = "get_object",
        expires_in: int = 3600,
        version_id: str | None = None,
        response_content_type: str | None = None,
        response_content_disposition: str | None = None,
        response_content_encoding: str | None = None,
        response_content_language: str | None = None,
        response_cache_control: str | None = None,
        response_expires: datetime | None = None,
        content_type: str | None = None,
        content_md5: str | None = None,
        metadata: dict[str, str] | None = None,
        server_side_encryption: Literal["AES256", "aws:fsx", "aws:kms", "aws:kms:dsse"] | None = None,
        sse_customer_algorithm: str | None = None,
        sse_customer_key: str | None = None,
        sse_kms_key_id: str | None = None,
        acl: Literal[
            "private",
            "public-read",
            "public-read-write",
            "authenticated-read",
            "aws-exec-read",
            "bucket-owner-read",
            "bucket-owner-full-control",
        ]
        | None = None,
    ) -> str:
        """Generate a presigned URL for an S3 object (mock implementation)."""
        if bucket not in self._buckets:
            raise S3NoSuchBucketClientException(f"Bucket {bucket} does not exist")

        if client_method == "get_object" and (bucket, key) not in self._objects:
            raise S3NoSuchKeyClientException(f"Object {key} does not exist in bucket {bucket}")

        # Generate a mock presigned URL
        # In a real implementation, this would be a signed URL from AWS
        # For testing purposes, we'll return a URL that includes the bucket and key
        expires_timestamp = int(datetime.now(UTC).timestamp() + expires_in)
        params: dict[str, str] = {
            "AWSAccessKeyId": "MOCK_ACCESS_KEY",
            "Expires": str(expires_timestamp),
            "Signature": "MOCK_SIGNATURE",
        }

        # Add parameters based on client_method (similar to real implementation)
        if client_method == "get_object":
            if version_id:
                params["VersionId"] = version_id
            if response_content_type:
                params["ResponseContentType"] = response_content_type
            if response_content_disposition:
                params["ResponseContentDisposition"] = response_content_disposition
            if response_content_encoding:
                params["ResponseContentEncoding"] = response_content_encoding
            if response_content_language:
                params["ResponseContentLanguage"] = response_content_language
            if response_cache_control:
                params["ResponseCacheControl"] = response_cache_control
            if response_expires:
                params["ResponseExpires"] = response_expires.isoformat()
            if sse_customer_algorithm:
                params["SSECustomerAlgorithm"] = sse_customer_algorithm
            if sse_customer_key:
                params["SSECustomerKey"] = sse_customer_key
        elif client_method == "put_object":
            if content_type:
                params["ContentType"] = content_type
            if content_md5:
                params["ContentMD5"] = content_md5
            if metadata:
                # Metadata is passed as headers in presigned URLs
                for meta_key, meta_value in metadata.items():
                    params[f"x-amz-meta-{meta_key}"] = meta_value
            if server_side_encryption:
                params["ServerSideEncryption"] = server_side_encryption
            if sse_customer_algorithm:
                params["SSECustomerAlgorithm"] = sse_customer_algorithm
            if sse_customer_key:
                params["SSECustomerKey"] = sse_customer_key
            if sse_kms_key_id:
                params["SSEKMSKeyId"] = sse_kms_key_id
            if acl:
                params["ACL"] = acl

        query_string = urllib.parse.urlencode(params)

        # Use custom endpoint if provided, otherwise use default AWS S3 format
        if self._endpoint_url:
            # For custom endpoints (e.g., MinIO, LocalStack), use the endpoint directly
            # Format: http://localhost:9000/bucket/key?params
            base_url = self._endpoint_url.rstrip("/")
            return f"{base_url}/{bucket}/{urllib.parse.quote(key, safe='/')}?{query_string}"
        # Default AWS S3 format
        return f"https://{bucket}.s3.amazonaws.com/{urllib.parse.quote(key, safe='/')}?{query_string}"


@pytest_asyncio.fixture(params=["mock", "aioboto3"])
async def s3_client(request: pytest.FixtureRequest, container: AsyncContainer) -> AsyncGenerator[AbstractS3Client]:
    """Fixture for S3 client implementations.

    Args:
        request: Pytest request fixture.
        container: Dishka async container fixture.

    Returns:
        S3 client implementation (mock or aioboto3).

    """
    if request.param == "mock":
        config = await container.get(MockAppConfig)
        endpoint_url = str(config.s3.endpoint_url) if config.s3.endpoint_url else None
        async with MockS3Client(endpoint_url=endpoint_url) as client:
            yield client

    if request.param == "aioboto3":
        config = await container.get(MockAppConfig)
        async with Aioboto3S3Client(
            aws_access_key_id=config.s3.aws_access_key_id,
            aws_secret_access_key=config.s3.aws_secret_access_key,
            aws_session_token=config.s3.aws_session_token,
            region_name=config.s3.aws_region,
            profile_name=config.s3.aws_profile,
            aws_account_id=config.s3.aws_account_id,
            use_ssl=config.s3.use_ssl,
            endpoint_url=str(config.s3.endpoint_url) if config.s3.endpoint_url else None,
        ) as client:
            yield client


@pytest_asyncio.fixture(params=["mock", "aioboto3"])
async def clean_all_buckets(s3_client: AbstractS3Client) -> None:
    """Clean all buckets."""
    response = await s3_client.list_buckets()

    if response.buckets is None:
        return

    for bucket in response.buckets or []:
        if bucket.name is None:
            continue
        try:
            await s3_client.delete_bucket(bucket.name)
        except S3BucketNotEmptyClientException:
            objects = await s3_client.list_objects(bucket.name)
            for obj in objects.contents or []:
                if obj.key is None:
                    continue
                await s3_client.delete_object(bucket.name, obj.key)

            await s3_client.delete_bucket(bucket.name)
