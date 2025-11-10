"""Pydantic S3 client models."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import Literal

from pydantic import BaseModel


class S3CreateBucketConfigurationLocation(BaseModel):
    """S3 create bucket configuration location."""

    type: Literal["AvailabilityZone", "LocalZone"]
    name: str


class S3CreateBucketConfigurationTag(BaseModel):
    """S3 create bucket configuration tag."""

    key: str
    value: str


class S3CreateBucketConfigurationBucket(BaseModel):
    """S3 create bucket configuration bucket."""

    data_redundancy: Literal["SingleAvailabilityZone", "SingleLocalZone"]
    type: Literal["Directory"]


class S3CreateBucketConfiguration(BaseModel):
    """S3 create bucket configuration."""

    location_constraint: (
        Literal[
            "af-south-1",
            "ap-east-1",
            "ap-northeast-1",
            "ap-northeast-2",
            "ap-northeast-3",
            "ap-south-1",
            "ap-south-2",
            "ap-southeast-1",
            "ap-southeast-2",
            "ap-southeast-3",
            "ap-southeast-4",
            "ap-southeast-5",
            "ca-central-1",
            "cn-north-1",
            "cn-northwest-1",
            "EU",
            "eu-central-1",
            "eu-central-2",
            "eu-north-1",
            "eu-south-1",
            "eu-south-2",
            "eu-west-1",
            "eu-west-2",
            "eu-west-3",
            "il-central-1",
            "me-central-1",
            "me-south-1",
            "sa-east-1",
            "us-east-2",
            "us-gov-east-1",
            "us-gov-west-1",
            "us-west-1",
            "us-west-2",
        ]
        | None
    ) = None
    location: S3CreateBucketConfigurationLocation | None = None
    bucket: S3CreateBucketConfigurationBucket | None = None
    tags: Sequence[S3CreateBucketConfigurationTag] | None = None


class S3DeleteObjectsDeleteObject(BaseModel):
    """S3 delete objects delete object."""

    key: str
    version_id: str | None = None
    etag: str | None = None
    last_modified_time: datetime | None = None
    size: int | None = None


class S3DeleteObjectsDelete(BaseModel):
    """S3 delete objects delete."""

    objects: Sequence[S3DeleteObjectsDeleteObject]
    quiet: bool | None = None


class S3DeleteObjectsResponseDeletedItem(BaseModel):
    """S3 delete objects response deleted item."""

    key: str
    version_id: str
    delete_marker: bool
    delete_marker_version_id: str


class S3DeleteObjectsResponseErrorItem(BaseModel):
    """S3 delete objects response error item."""

    key: str
    version_id: str
    code: str
    message: str


class S3DeleteObjectsResponse(BaseModel):
    """S3 delete objects response."""

    deleted: Sequence[S3DeleteObjectsResponseDeletedItem]
    error: Sequence[S3DeleteObjectsResponseErrorItem]
    request_charged: Literal["requester"] | None = None


class S3Owner(BaseModel):
    """S3 owner."""

    display_name: str | None = None
    id: str | None = None


class S3Grantee(BaseModel):
    """S3 grantee."""

    display_name: str | None = None
    email_address: str | None = None
    id: str | None = None
    type: Literal["CanonicalUser", "AmazonCustomerByEmail", "Group"]
    uri: str | None = None


class S3Grant(BaseModel):
    """S3 grant."""

    grantee: S3Grantee | None = None
    permission: Literal["FULL_CONTROL", "WRITE", "WRITE_ACP", "READ", "READ_ACP"] | None = None


class S3AccessControlPolicy(BaseModel):
    """S3 access control policy."""

    grants: Sequence[S3Grant] | None = None
    owner: S3Owner | None = None


class S3GetBucketAclResponse(BaseModel):
    """S3 get bucket ACL response."""

    grants: Sequence[S3Grant] | None = None
    owner: S3Owner | None = None


class S3GetObjectAclResponse(BaseModel):
    """S3 get object ACL response."""

    grants: Sequence[S3Grant] | None = None
    owner: S3Owner | None = None
    request_charged: Literal["requester"] | None = None


class S3CORSRule(BaseModel):
    """S3 CORS rule."""

    allowed_headers: Sequence[str] | None = None
    allowed_methods: Sequence[str]
    allowed_origins: Sequence[str]
    expose_headers: Sequence[str] | None = None
    id: str | None = None
    max_age_seconds: int | None = None


class S3CORSConfiguration(BaseModel):
    """S3 CORS configuration."""

    cors_rules: Sequence[S3CORSRule]


class S3GetBucketCorsResponse(BaseModel):
    """S3 get bucket CORS response."""

    cors_rules: Sequence[S3CORSRule] | None = None


class S3LifecycleExpiration(BaseModel):
    """S3 lifecycle expiration."""

    date: datetime | None = None
    days: int | None = None
    expired_object_delete_marker: bool | None = None


class S3LifecycleTransition(BaseModel):
    """S3 lifecycle transition."""

    date: datetime | None = None
    days: int | None = None
    storage_class: (
        Literal[
            "GLACIER",
            "STANDARD_IA",
            "ONEZONE_IA",
            "INTELLIGENT_TIERING",
            "DEEP_ARCHIVE",
            "GLACIER_IR",
        ]
        | None
    ) = None


class S3NoncurrentVersionExpiration(BaseModel):
    """S3 noncurrent version expiration."""

    noncurrent_days: int | None = None
    newer_noncurrent_versions: int | None = None


class S3NoncurrentVersionTransition(BaseModel):
    """S3 noncurrent version transition."""

    noncurrent_days: int | None = None
    storage_class: (
        Literal[
            "GLACIER",
            "STANDARD_IA",
            "ONEZONE_IA",
            "INTELLIGENT_TIERING",
            "DEEP_ARCHIVE",
            "GLACIER_IR",
        ]
        | None
    ) = None
    newer_noncurrent_versions: int | None = None


class S3LifecycleRuleFilter(BaseModel):
    """S3 lifecycle rule filter."""

    and_: dict | None = None  # And
    prefix: str | None = None
    tag: dict | None = None
    object_size_greater_than: int | None = None
    object_size_less_than: int | None = None


class S3LifecycleRule(BaseModel):
    """S3 lifecycle rule."""

    id: str
    status: Literal["Enabled", "Disabled"]
    abort_incomplete_multipart_upload: dict | None = None
    expiration: S3LifecycleExpiration | None = None
    filter: S3LifecycleRuleFilter | None = None
    noncurrent_version_expiration: S3NoncurrentVersionExpiration | None = None
    noncurrent_version_transitions: Sequence[S3NoncurrentVersionTransition] | None = None
    transitions: Sequence[S3LifecycleTransition] | None = None


class S3LifecycleConfiguration(BaseModel):
    """S3 lifecycle configuration."""

    rules: Sequence[S3LifecycleRule]


class S3GetBucketLifecycleConfigurationResponse(BaseModel):
    """S3 get bucket lifecycle configuration response."""

    rules: Sequence[S3LifecycleRule] | None = None


class S3GetBucketPolicyResponse(BaseModel):
    """S3 get bucket policy response."""

    policy: str | None = None
    revision_id: str | None = None


class S3GetObjectResponse(BaseModel):
    """S3 get object response."""

    body: bytes
    delete_marker: bool | None = None
    accept_ranges: str | None = None
    expiration: str | None = None
    restore: str | None = None
    last_modified: datetime | None = None
    content_length: int | None = None
    etag: str | None = None
    checksum_crc32: str | None = None
    checksum_crc32c: str | None = None
    checksum_sha1: str | None = None
    checksum_sha256: str | None = None
    missing_meta: int | None = None
    version_id: str | None = None
    cache_control: str | None = None
    content_disposition: str | None = None
    content_encoding: str | None = None
    content_language: str | None = None
    content_range: str | None = None
    content_type: str | None = None
    expires: datetime | None = None
    website_redirect_location: str | None = None
    server_side_encryption: Literal["AES256", "aws:kms", "aws:kms:dsse"] | None = None
    metadata: dict[str, str] | None = None
    sse_customer_algorithm: str | None = None
    sse_customer_key_md5: str | None = None
    sse_kms_key_id: str | None = None
    bucket_key_enabled: bool | None = None
    storage_class: (
        Literal[
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
        ]
        | None
    ) = None
    request_charged: Literal["requester"] | None = None
    replication_status: Literal["COMPLETE", "PENDING", "FAILED", "REPLICA"] | None = None
    parts_count: int | None = None
    tag_count: int | None = None
    object_lock_mode: Literal["GOVERNANCE", "COMPLIANCE"] | None = None
    object_lock_retain_until_date: datetime | None = None
    object_lock_legal_hold_status: Literal["ON", "OFF"] | None = None


class S3Bucket(BaseModel):
    """S3 bucket."""

    name: str | None = None
    creation_date: datetime | None = None


class S3ListBucketsResponse(BaseModel):
    """S3 list buckets response."""

    buckets: Sequence[S3Bucket] | None = None
    owner: S3Owner | None = None


class S3Object(BaseModel):
    """S3 object."""

    key: str | None = None
    last_modified: datetime | None = None
    etag: str | None = None
    checksum_algorithm: Sequence[Literal["CRC32", "CRC32C", "SHA1", "SHA256"]] | None = None
    size: int | None = None
    storage_class: (
        Literal[
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
        ]
        | None
    ) = None
    owner: S3Owner | None = None


class S3CommonPrefix(BaseModel):
    """S3 common prefix."""

    prefix: str | None = None


class S3ListObjectsResponse(BaseModel):
    """S3 list objects response."""

    is_truncated: bool | None = None
    marker: str | None = None
    next_marker: str | None = None
    contents: Sequence[S3Object] | None = None
    name: str | None = None
    prefix: str | None = None
    delimiter: str | None = None
    max_keys: int | None = None
    common_prefixes: Sequence[S3CommonPrefix] | None = None
    encoding_type: Literal["url"] | None = None
    request_charged: Literal["requester"] | None = None


class S3ListObjectsV2Response(BaseModel):
    """S3 list objects v2 response."""

    is_truncated: bool | None = None
    contents: Sequence[S3Object] | None = None
    name: str | None = None
    prefix: str | None = None
    delimiter: str | None = None
    max_keys: int | None = None
    common_prefixes: Sequence[S3CommonPrefix] | None = None
    encoding_type: Literal["url"] | None = None
    key_count: int | None = None
    continuation_token: str | None = None
    next_continuation_token: str | None = None
    start_after: str | None = None
    request_charged: Literal["requester"] | None = None


class S3PutObjectResponse(BaseModel):
    """S3 put object response."""

    etag: str | None = None
    checksum_crc32: str | None = None
    checksum_crc32c: str | None = None
    checksum_sha1: str | None = None
    checksum_sha256: str | None = None
    expiration: str | None = None
    request_charged: Literal["requester"] | None = None
    sse_customer_algorithm: str | None = None
    sse_customer_key_md5: str | None = None
    sse_kms_key_id: str | None = None
    sse_kms_encryption_context: str | None = None
    bucket_key_enabled: bool | None = None
    server_side_encryption: Literal["AES256", "aws:kms", "aws:kms:dsse"] | None = None
    version_id: str | None = None


class S3PutObjectAclResponse(BaseModel):
    """S3 put object ACL response."""

    request_charged: Literal["requester"] | None = None


class S3PutBucketAclResponse(BaseModel):
    """S3 put bucket ACL response."""

    request_charged: Literal["requester"] | None = None


class S3PutBucketCorsResponse(BaseModel):
    """S3 put bucket CORS response."""

    request_charged: Literal["requester"] | None = None


class S3PutBucketLifecycleConfigurationResponse(BaseModel):
    """S3 put bucket lifecycle configuration response."""

    request_charged: Literal["requester"] | None = None


class S3PutBucketPolicyResponse(BaseModel):
    """S3 put bucket policy response."""

    request_charged: Literal["requester"] | None = None


class S3ObjectLockLegalHold(BaseModel):
    """S3 object lock legal hold."""

    status: Literal["ON", "OFF"] | None = None


class S3PutObjectLegalHoldResponse(BaseModel):
    """S3 put object legal hold response."""

    request_charged: Literal["requester"] | None = None


class S3DefaultRetention(BaseModel):
    """S3 default retention."""

    mode: Literal["GOVERNANCE", "COMPLIANCE"] | None = None
    days: int | None = None
    years: int | None = None


class S3ObjectLockRule(BaseModel):
    """S3 object lock rule."""

    default_retention: S3DefaultRetention | None = None


class S3ObjectLockConfiguration(BaseModel):
    """S3 object lock configuration."""

    object_lock_enabled: Literal["Enabled"] | None = None
    rule: S3ObjectLockRule | None = None


class S3PutObjectLockConfigurationResponse(BaseModel):
    """S3 put object lock configuration response."""

    request_charged: Literal["requester"] | None = None


class S3ObjectLockRetention(BaseModel):
    """S3 object lock retention."""

    mode: Literal["GOVERNANCE", "COMPLIANCE"] | None = None
    retain_until_date: datetime | None = None


class S3PutObjectRetentionResponse(BaseModel):
    """S3 put object retention response."""

    request_charged: Literal["requester"] | None = None


class S3CopyObjectResult(BaseModel):
    """S3 copy object result."""

    etag: str | None = None
    last_modified: datetime | None = None
    checksum_crc32: str | None = None
    checksum_crc32c: str | None = None
    checksum_sha1: str | None = None
    checksum_sha256: str | None = None


class S3CopyObjectResponse(BaseModel):
    """S3 copy object response."""

    copy_object_result: S3CopyObjectResult | None = None
    copy_source_version_id: str | None = None
    expiration: str | None = None
    request_charged: Literal["requester"] | None = None
    server_side_encryption: Literal["AES256", "aws:kms", "aws:kms:dsse"] | None = None
    sse_customer_algorithm: str | None = None
    sse_customer_key_md5: str | None = None
    sse_kms_key_id: str | None = None
    sse_kms_encryption_context: str | None = None
    bucket_key_enabled: bool | None = None
    version_id: str | None = None


class S3CreateBucketResponse(BaseModel):
    """S3 create bucket response."""

    location: str | None = None


class S3DeleteBucketResponse(BaseModel):
    """S3 delete bucket response."""

    request_charged: Literal["requester"] | None = None


class S3DeleteBucketCorsResponse(BaseModel):
    """S3 delete bucket CORS response."""

    request_charged: Literal["requester"] | None = None


class S3DeleteBucketLifecycleResponse(BaseModel):
    """S3 delete bucket lifecycle response."""

    request_charged: Literal["requester"] | None = None


class S3DeleteBucketPolicyResponse(BaseModel):
    """S3 delete bucket policy response."""

    request_charged: Literal["requester"] | None = None


class S3DeleteObjectResponse(BaseModel):
    """S3 delete object response."""

    delete_marker: bool | None = None
    version_id: str | None = None
    request_charged: Literal["requester"] | None = None
