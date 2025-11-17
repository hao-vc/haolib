"""Abstract S3 client.

IMPORTANT: THIS MODULE HAS BEEN VIBE CODED

"""

from datetime import datetime
from typing import Literal, Protocol

from haolib.database.files.s3.clients.pydantic import (
    S3AccessControlPolicy,
    S3CopyObjectResponse,
    S3CORSConfiguration,
    S3CreateBucketConfiguration,
    S3CreateBucketResponse,
    S3DeleteBucketCorsResponse,
    S3DeleteBucketLifecycleResponse,
    S3DeleteBucketPolicyResponse,
    S3DeleteBucketResponse,
    S3DeleteObjectResponse,
    S3DeleteObjectsDelete,
    S3DeleteObjectsResponse,
    S3GetBucketAclResponse,
    S3GetBucketCorsResponse,
    S3GetBucketLifecycleConfigurationResponse,
    S3GetBucketPolicyResponse,
    S3GetObjectAclResponse,
    S3GetObjectResponse,
    S3LifecycleConfiguration,
    S3ListBucketsResponse,
    S3ListObjectsResponse,
    S3ListObjectsV2Response,
    S3ObjectLockConfiguration,
    S3ObjectLockLegalHold,
    S3ObjectLockRetention,
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


class S3ClientException(Exception):
    """Base exception for S3 client errors."""


class S3BucketAlreadyExistsClientException(S3ClientException):
    """Raised when attempting to create a bucket that already exists."""


class S3BucketAlreadyOwnedByYouClientException(S3ClientException):
    """Raised when attempting to create a bucket that you already own."""


class S3NoSuchBucketClientException(S3ClientException):
    """Raised when the specified bucket does not exist."""


class S3NoSuchKeyClientException(S3ClientException):
    """Raised when the specified object key does not exist."""


class S3AccessDeniedClientException(S3ClientException):
    """Raised when access is denied to the specified resource."""


class S3InvalidRequestClientException(S3ClientException):
    """Raised when the request is invalid."""


class S3InvalidArgumentClientException(S3ClientException):
    """Raised when an invalid argument is provided."""


class S3NoSuchLifecycleConfigurationClientException(S3ClientException):
    """Raised when the lifecycle configuration does not exist."""


class S3NoSuchCORSConfigurationClientException(S3ClientException):
    """Raised when the CORS configuration does not exist."""


class S3NoSuchPolicyClientException(S3ClientException):
    """Raised when the bucket policy does not exist."""


class S3InvalidBucketNameClientException(S3ClientException):
    """Raised when the bucket name is invalid."""


class S3BucketNotEmptyClientException(S3ClientException):
    """Raised when attempting to delete a bucket that is not empty."""


class S3ObjectNotInActiveTierClientException(S3ClientException):
    """Raised when the object is not in the active tier."""


class S3InvalidObjectStateClientException(S3ClientException):
    """Raised when the object is in an invalid state."""


class S3PreconditionFailedClientException(S3ClientException):
    """Raised when a precondition check fails."""


class S3InvalidSecurityClientException(S3ClientException):
    """Raised when the security credentials are invalid."""


class S3InvalidTokenClientException(S3ClientException):
    """Raised when the token is invalid."""


class S3RequestTimeoutClientException(S3ClientException):
    """Raised when the request times out."""


class S3ServiceClientException(S3ClientException):
    """Raised when an S3 service error occurs."""


class AbstractS3Client(Protocol):
    """Abstract S3 client. Must implement all the methods of the S3 API.

    See https://docs.aws.amazon.com/code-library/latest/ug/python_3_s3_code_examples.html#actions
    """

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
        """Copy an object from one bucket to another.

        Creates a copy of an object that is already stored in Amazon S3. You can store individual
        objects of up to 5 TB in Amazon S3. You create a copy of your object up to 5 GB in size
        in a single atomic operation using this API. However, for copying an object greater than
        5 GB, you must use the multipart upload Upload Part - Copy API.

        Args:
            bucket: The name of the destination bucket.
            copy_source: The name of the source bucket and key name of the source object, separated
                by a slash (/). Must be URL-encoded. For example: bucket-name/object-name.
            key: The key of the destination object.
            acl: The canned ACL to apply to the object.
            cache_control: Specifies caching behavior along the request/reply chain.
            checksum_algorithm: Indicates the algorithm used to create the checksum.
            content_disposition: Specifies presentational information for the object.
            content_encoding: Specifies what content encodings have been applied to the object.
            content_language: The language the content is in.
            content_type: A standard MIME type describing the format of the object data.
            copy_source_if_match: Copies the object if its entity tag (ETag) matches the specified tag.
            copy_source_if_modified_since: Copies the object if it has been modified since the specified time.
            copy_source_if_none_match: Copies the object if its entity tag (ETag) is different from the specified tag.
            copy_source_if_unmodified_since: Copies the object if it hasn't been modified since the specified time.
            expires: The date and time at which the object is no longer cacheable.
            grant_full_control: Gives the grantee READ, READ_ACP, and WRITE_ACP permissions on the object.
            grant_read: Allows grantee to read the object data and its metadata.
            grant_read_acp: Allows grantee to read the object ACL.
            grant_write_acp: Allows grantee to write the ACL for the applicable object.
            if_match: Return the object only if its entity tag (ETag) is the same as the one specified.
            if_none_match: Return the object only if its entity tag (ETag) is different from the one specified.
            metadata: A map of metadata to store with the object in S3.
            metadata_directive: Specifies whether the metadata is copied from the source object or replaced.
            tagging_directive: Specifies whether the object tag-set are copied from the source object or replaced.
            server_side_encryption: The server-side encryption algorithm used when storing this object.
            storage_class: By default, Amazon S3 uses the STANDARD Storage Class to store newly created objects.
            website_redirect_location: If the bucket is configured as a website, redirects requests for this object.
            sse_customer_algorithm: Specifies the algorithm to use to when encrypting the object.
            sse_customer_key: Specifies the customer-provided encryption key.
            sse_kms_key_id: Specifies the ID of the customer managed KMS key.
            sse_kms_encryption_context: Specifies the Amazon S3 encryption context.
            bucket_key_enabled: Specifies whether Amazon S3 should use an S3 Bucket Key for object encryption.
            copy_source_sse_customer_algorithm: Specifies the algorithm to use when decrypting the source object.
            copy_source_sse_customer_key: Specifies the customer-provided encryption key for the source object.
            request_payer: Confirms that the requester knows that they will be charged for the request.
            tagging: The tag-set for the object.
            object_lock_mode: The Object Lock mode that you want to apply to this object.
            object_lock_retain_until_date: The date and time when you want this object's Object Lock to expire.
            object_lock_legal_hold_status: Specifies whether a legal hold will be applied to this object.
            expected_bucket_owner: The account ID of the expected destination bucket owner.
            expected_source_bucket_owner: The account ID of the expected source bucket owner.

        Returns:
            A response containing metadata about the copied object, including ETag, LastModified,
            version ID, and encryption information.

        Raises:
            S3NoSuchBucketClientException: If the source or destination bucket does not exist.
            S3NoSuchKeyClientException: If the source object key does not exist.
            S3AccessDeniedClientException: If access is denied to the source or destination bucket/object.
            S3PreconditionFailedClientException: If a precondition check fails.
            S3InvalidRequestClientException: If the request is invalid.
            S3ServiceClientException: For other S3 service errors.

        """
        ...

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
        """Create a new bucket.

        Creates a new S3 bucket. To create a bucket, you must register with Amazon S3 and have a
        valid AWS Access Key ID to authenticate requests. Anonymous requests are never allowed to
        create buckets. By creating the bucket, you become the bucket owner.

        Args:
            bucket: The name of the bucket to create. Bucket names must be unique and follow
                Amazon S3 naming rules.
            acl: The canned ACL to apply to the bucket.
            create_bucket_configuration: The configuration information for the bucket.
            grant_full_control: Allows grantee the read, write, read ACP, and write ACP permissions
                on the bucket.
            grant_read: Allows grantee to list the objects in the bucket.
            grant_read_acp: Allows grantee to read the bucket ACL.
            grant_write: Allows grantee to create, overwrite, and delete any object in the bucket.
            grant_write_acp: Allows grantee to write the ACL for the applicable bucket.
            object_lock_enabled_for_bucket: Specifies whether you want S3 Object Lock to be enabled
                for the new bucket.
            object_ownership: The container element for object ownership for a bucket's ownership controls.

        Returns:
            A response containing the location of the newly created bucket.

        Raises:
            S3BucketAlreadyExistsClientException: If the bucket already exists.
            S3BucketAlreadyOwnedByYouClientException: If the bucket is already owned by you.
            S3InvalidBucketNameClientException: If the bucket name is invalid.
            S3AccessDeniedClientException: If access is denied.
            S3InvalidRequestClientException: If the request is invalid.
            S3ServiceClientException: For other S3 service errors.

        """
        ...

    async def delete_bucket(
        self,
        bucket: str,
        expected_bucket_owner: str | None = None,
    ) -> S3DeleteBucketResponse:
        """Delete a bucket.

        Deletes the S3 bucket. All objects (including all object versions and delete markers)
        in the bucket must be deleted before the bucket itself can be deleted.

        Args:
            bucket: The name of the bucket to delete.
            expected_bucket_owner: The account ID of the expected bucket owner.

        Returns:
            A response containing request metadata.

        Raises:
            S3NoSuchBucketClientException: If the bucket does not exist.
            S3BucketNotEmptyClientException: If the bucket is not empty.
            S3AccessDeniedClientException: If access is denied.
            S3InvalidRequestClientException: If the request is invalid.
            S3ServiceClientException: For other S3 service errors.

        """
        ...

    async def delete_bucket_cors(
        self, bucket: str, expected_bucket_owner: str | None = None
    ) -> S3DeleteBucketCorsResponse:
        """Delete the CORS configuration for a bucket.

        Deletes the CORS configuration information set for the bucket.

        Args:
            bucket: The name of the bucket to delete the CORS configuration from.
            expected_bucket_owner: The account ID of the expected bucket owner.

        Returns:
            A response containing request metadata.

        Raises:
            S3NoSuchBucketClientException: If the bucket does not exist.
            S3NoSuchCORSConfigurationClientException: If the CORS configuration does not exist.
            S3AccessDeniedClientException: If access is denied.
            S3InvalidRequestClientException: If the request is invalid.
            S3ServiceClientException: For other S3 service errors.

        """
        ...

    async def delete_bucket_lifecycle(
        self, bucket: str, expected_bucket_owner: str | None = None
    ) -> S3DeleteBucketLifecycleResponse:
        """Delete the lifecycle configuration for a bucket.

        Deletes the lifecycle configuration from the specified bucket. Amazon S3 removes all
        the lifecycle configuration rules in the lifecycle subresource associated with the bucket.

        Args:
            bucket: The name of the bucket to delete the lifecycle configuration from.
            expected_bucket_owner: The account ID of the expected bucket owner.

        Returns:
            A response containing request metadata.

        Raises:
            S3NoSuchBucketClientException: If the bucket does not exist.
            S3NoSuchLifecycleConfigurationClientException: If the lifecycle configuration does not exist.
            S3AccessDeniedClientException: If access is denied.
            S3InvalidRequestClientException: If the request is invalid.
            S3ServiceClientException: For other S3 service errors.

        """
        ...

    async def delete_bucket_policy(
        self, bucket: str, expected_bucket_owner: str | None = None
    ) -> S3DeleteBucketPolicyResponse:
        """Delete the policy for a bucket.

        Deletes the policy from the specified bucket. This action removes all policies from
        the bucket. There must be at least one policy attached to the bucket.

        Args:
            bucket: The name of the bucket to delete the policy from.
            expected_bucket_owner: The account ID of the expected bucket owner.

        Returns:
            A response containing request metadata.

        Raises:
            S3NoSuchBucketClientException: If the bucket does not exist.
            S3NoSuchPolicyClientException: If the bucket policy does not exist.
            S3AccessDeniedClientException: If access is denied.
            S3InvalidRequestClientException: If the request is invalid.
            S3ServiceClientException: For other S3 service errors.

        """
        ...

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
        """Delete an object from a bucket.

        Removes the null version (if there is one) of an object and inserts a delete marker,
        which becomes the current version of the object. If there isn't a null version, Amazon S3
        does not remove any objects but will still respond that the command was successful.

        Args:
            bucket: The name of the bucket containing the object to delete.
            key: The key name of the object to delete.
            mfa: The concatenation of the authentication device's serial number, a space, and
                the value that is displayed on your authentication device.
            version_id: Version ID used to reference a specific version of the object.
            request_payer: Confirms that the requester knows that they will be charged for the request.
            bypass_governance_retention: Indicates whether S3 Object Lock should bypass
                Governance-mode restrictions to process this operation.
            expected_bucket_owner: The account ID of the expected bucket owner.
            if_match: Return the object only if its entity tag (ETag) is the same as the one specified.
            if_match_last_modified_time: Return the object only if it has been modified since the specified time.
            if_match_size: Return the object only if its size matches the specified size.

        Returns:
            A response containing information about the deleted object, including whether a delete
            marker was created, the version ID, and request metadata.

        Raises:
            S3NoSuchBucketClientException: If the bucket does not exist.
            S3NoSuchKeyClientException: If the object key does not exist.
            S3AccessDeniedClientException: If access is denied.
            S3PreconditionFailedClientException: If a precondition check fails.
            S3InvalidObjectStateClientException: If the object is in an invalid state.
            S3InvalidRequestClientException: If the request is invalid.
            S3ServiceClientException: For other S3 service errors.

        """
        ...

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
        """Delete objects from a bucket.

        Args:
            bucket: The name of the bucket.
            delete: The objects to delete.
            mfa: The concatenation of the authentication device's serial number, a space, and
                the value that is displayed on your authentication device.
            request_payer: Confirms that the requester knows that they will be charged for the request.
            bypass_governance_retention: Indicates whether S3 Object Lock should bypass
                Governance-mode restrictions to process this operation.
            expected_bucket_owner: The account ID of the expected bucket owner.
            checksum_algorithm: Indicates the algorithm used to create the checksum.

        Returns:
            A response containing information about the deleted objects,
                including the objects that were deleted and the objects that were not deleted.

        Raises:
            S3NoSuchBucketClientException: If the bucket does not exist.
            S3AccessDeniedClientException: If access is denied.
            S3InvalidRequestClientException: If the request is invalid.
            S3ServiceClientException: For other S3 service errors.

        """
        ...

    async def get_bucket_acl(
        self,
        bucket: str,
        expected_bucket_owner: str | None = None,
    ) -> S3GetBucketAclResponse:
        """Get the ACL for a bucket.

        Args:
            bucket: The name of the bucket.
            expected_bucket_owner: The account ID of the expected bucket owner.

        Returns:
            A dictionary containing the ACL information.

        Raises:
            S3NoSuchBucketClientException: If the bucket does not exist.
            S3AccessDeniedClientException: If access is denied.
            S3InvalidRequestClientException: If the request is invalid.
            S3ServiceClientException: For other S3 service errors.

        """
        ...

    async def get_bucket_cors(
        self,
        bucket: str,
        expected_bucket_owner: str | None = None,
    ) -> S3GetBucketCorsResponse:
        """Get the CORS configuration for a bucket.

        Args:
            bucket: The name of the bucket.
            expected_bucket_owner: The account ID of the expected bucket owner.

        Returns:
            A dictionary containing the CORS configuration.

        Raises:
            S3NoSuchBucketClientException: If the bucket does not exist.
            S3NoSuchCORSConfigurationClientException: If the CORS configuration does not exist.
            S3AccessDeniedClientException: If access is denied.
            S3InvalidRequestClientException: If the request is invalid.
            S3ServiceClientException: For other S3 service errors.

        """
        ...

    async def get_bucket_lifecycle_configuration(
        self,
        bucket: str,
        expected_bucket_owner: str | None = None,
    ) -> S3GetBucketLifecycleConfigurationResponse:
        """Get the lifecycle configuration for a bucket.

        Args:
            bucket: The name of the bucket.
            expected_bucket_owner: The account ID of the expected bucket owner.

        Returns:
            A dictionary containing the lifecycle configuration.

        Raises:
            S3NoSuchBucketClientException: If the bucket does not exist.
            S3NoSuchLifecycleConfigurationClientException: If the lifecycle configuration does not exist.
            S3AccessDeniedClientException: If access is denied.
            S3InvalidRequestClientException: If the request is invalid.
            S3ServiceClientException: For other S3 service errors.

        """
        ...

    async def get_bucket_policy(
        self,
        bucket: str,
        expected_bucket_owner: str | None = None,
    ) -> S3GetBucketPolicyResponse:
        """Get the policy for a bucket.

        Args:
            bucket: The name of the bucket.
            expected_bucket_owner: The account ID of the expected bucket owner.

        Returns:
            A dictionary containing the bucket policy.

        Raises:
            S3NoSuchBucketClientException: If the bucket does not exist.
            S3NoSuchPolicyClientException: If the bucket policy does not exist.
            S3AccessDeniedClientException: If access is denied.
            S3InvalidRequestClientException: If the request is invalid.
            S3ServiceClientException: For other S3 service errors.

        """
        ...

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
        """Get an object from a bucket.

        Args:
            bucket: The name of the bucket.
            key: The object key.
            if_match: Return the object only if its entity tag (ETag) is the same as the one specified.
            if_modified_since: Return the object only if it has been modified since the specified time.
            if_none_match: Return the object only if its entity tag (ETag) is different from the one specified.
            if_unmodified_since: Return the object only if it has not been modified since the specified time.
            range_header: Downloads the specified range bytes of an object.
            response_cache_control: Sets the Cache-Control header of the response.
            response_content_disposition: Sets the Content-Disposition header of the response.
            response_content_encoding: Sets the Content-Encoding header of the response.
            response_content_language: Sets the Content-Language header of the response.
            response_content_type: Sets the Content-Type header of the response.
            response_expires: Sets the Expires header of the response.
            version_id: Version ID used to reference a specific version of the object.
            sse_customer_algorithm: Specifies the algorithm to use to when decrypting the object.
            sse_customer_key: Specifies the customer-provided encryption key.
            request_payer: Confirms that the requester knows that they will be charged for the request.
            expected_bucket_owner: The account ID of the expected bucket owner.
            checksum_mode: To retrieve the checksum, this parameter must be enabled.

        Returns:
            A dictionary containing the object data and metadata.

        Raises:
            S3NoSuchBucketClientException: If the bucket does not exist.
            S3NoSuchKeyClientException: If the object key does not exist.
            S3AccessDeniedClientException: If access is denied.
            S3PreconditionFailedClientException: If a precondition check fails.
            S3InvalidObjectStateClientException: If the object is in an invalid state.
            S3InvalidRequestClientException: If the request is invalid.
            S3ServiceClientException: For other S3 service errors.

        """
        ...

    async def get_object_acl(
        self,
        bucket: str,
        key: str,
        version_id: str | None = None,
        request_payer: Literal["requester", "requester_payer"] | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3GetObjectAclResponse:
        """Get the ACL for an object.

        Args:
            bucket: The name of the bucket.
            key: The object key.
            version_id: Version ID used to reference a specific version of the object.
            request_payer: Confirms that the requester knows that they will be charged for the request.
            expected_bucket_owner: The account ID of the expected bucket owner.

        Returns:
            A dictionary containing the ACL information.

        """
        ...

    async def list_buckets(
        self,
    ) -> S3ListBucketsResponse:
        """List all buckets owned by the authenticated sender of the request.

        Returns:
            A dictionary containing the list of buckets.

        """
        ...

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
        """List some or all (up to 1,000) of the objects in a bucket.

        Args:
            bucket: The name of the bucket.
            delimiter: A delimiter is a character you use to group keys.
            encoding_type: Encoding type used by Amazon S3 to encode object keys in the response.
            marker: Specifies the key to start with when listing objects in a bucket.
            max_keys: Sets the maximum number of keys returned in the response.
            prefix: Limits the response to keys that begin with the specified prefix.
            request_payer: Confirms that the requester knows that they will be charged for the request.
            expected_bucket_owner: The account ID of the expected bucket owner.

        Returns:
            A dictionary containing the list of objects.

        """
        ...

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
        """List some or all (up to 1,000) of the objects in a bucket using version 2 API.

        Args:
            bucket: The name of the bucket.
            delimiter: A delimiter is a character you use to group keys.
            encoding_type: Encoding type used by Amazon S3 to encode object keys in the response.
            max_keys: Sets the maximum number of keys returned in the response.
            prefix: Limits the response to keys that begin with the specified prefix.
            continuation_token: ContinuationToken indicates Amazon S3 that
                the list is being continued on this bucket with a token.
            fetch_owner: The owner field is not present in listV2 by default.
            start_after: StartAfter is where you want Amazon S3 to start listing from.
            request_payer: Confirms that the requester knows that they will be charged for the request.
            expected_bucket_owner: The account ID of the expected bucket owner.

        Returns:
            A dictionary containing the list of objects.

        """
        ...

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
        """Put the ACL for a bucket.

        Args:
            bucket: The name of the bucket.
            acl: The canned ACL to apply to the bucket.
            access_control_policy: Contains the elements that set the ACL permissions.
            content_md5: The base64-encoded 128-bit MD5 digest of the data.
            checksum_algorithm: Indicates the algorithm used to create the checksum.
            grant_full_control: Allows grantee the read, write, read ACP, and write ACP permissions on the bucket.
            grant_read: Allows grantee to list the objects in the bucket.
            grant_read_acp: Allows grantee to read the bucket ACL.
            grant_write: Allows grantee to create, overwrite, and delete any object in the bucket.
            grant_write_acp: Allows grantee to write the ACL for the applicable bucket.
            expected_bucket_owner: The account ID of the expected bucket owner.

        """
        ...

    async def put_bucket_cors(
        self,
        bucket: str,
        cors_configuration: S3CORSConfiguration,
        content_md5: str | None = None,
        checksum_algorithm: Literal["CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME"] | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3PutBucketCorsResponse:
        """Put the CORS configuration for a bucket.

        Args:
            bucket: The name of the bucket.
            cors_configuration: The CORS configuration.
            content_md5: The base64-encoded 128-bit MD5 digest of the data.
            checksum_algorithm: Indicates the algorithm used to create the checksum.
            expected_bucket_owner: The account ID of the expected bucket owner.

        """
        ...

    async def put_bucket_lifecycle_configuration(
        self,
        bucket: str,
        lifecycle_configuration: S3LifecycleConfiguration | None = None,
        checksum_algorithm: Literal["CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME"] | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3PutBucketLifecycleConfigurationResponse:
        """Put the lifecycle configuration for a bucket.

        Args:
            bucket: The name of the bucket.
            lifecycle_configuration: Container for lifecycle rules.
            checksum_algorithm: Indicates the algorithm used to create the checksum.
            expected_bucket_owner: The account ID of the expected bucket owner.

        """
        ...

    async def put_bucket_policy(
        self,
        bucket: str,
        policy: str,
        content_md5: str | None = None,
        checksum_algorithm: Literal["CRC32", "CRC32C", "SHA1", "SHA256", "CRC64NVME"] | None = None,
        confirm_remove_self_bucket_access: bool | None = None,
        expected_bucket_owner: str | None = None,
    ) -> S3PutBucketPolicyResponse:
        """Put the policy for a bucket.

        Args:
            bucket: The name of the bucket.
            policy: The bucket policy as a JSON document.
            content_md5: The base64-encoded 128-bit MD5 digest of the data.
            checksum_algorithm: Indicates the algorithm used to create the checksum.
            confirm_remove_self_bucket_access: Set this parameter to true to confirm that you want to remove your
                permissions to change this bucket policy.
            expected_bucket_owner: The account ID of the expected bucket owner.

        """
        ...

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
        """Put an object into a bucket.

        Args:
            bucket: The name of the bucket.
            key: The object key.
            body: Object data.
            acl: The canned ACL to apply to the object.
            cache_control: Can be used to specify caching behavior along the request/reply chain.
            content_disposition: Specifies presentational information for the object.
            content_encoding: Specifies what content encodings have been applied to the object.
            content_language: The language the content is in.
            content_length: Size of the body in bytes.
            content_md5: The base64-encoded 128-bit MD5 digest of the data.
            content_type: A standard MIME type describing the format of the object data.
            checksum_algorithm: Indicates the algorithm used to create the checksum.
            checksum_crc32: This header can be used as a message integrity check.
            checksum_crc32c: This header can be used as a message integrity check.
            checksum_sha1: This header can be used as a message integrity check.
            checksum_sha256: This header can be used as a message integrity check.
            expires: The date and time at which the object is no longer cacheable.
            grant_full_control: Allows grantee the read, write, read ACP, and write ACP permissions on the object.
            grant_read: Allows grantee to read the object data and its metadata.
            grant_read_acp: Allows grantee to read the object ACL.
            grant_write_acp: Allows grantee to write the ACL for the applicable object.
            metadata: A map of metadata to store with the object in S3.
            server_side_encryption: The server-side encryption algorithm used when storing this object.
            storage_class: By default, Amazon S3 uses the STANDARD Storage Class to store newly created objects.
            website_redirect_location: If the bucket is configured as a website, redirects requests
                for this object to another object.
            sse_customer_algorithm: Specifies the algorithm to use to when encrypting the object.
            sse_customer_key: Specifies the customer-provided encryption key.
            sse_kms_key_id: Specifies the ID of the customer managed KMS key.
            sse_kms_encryption_context: Specifies the Amazon S3 encryption context.
            bucket_key_enabled: Specifies whether Amazon S3 should use an S3 Bucket Key for object encryption.
            request_payer: Confirms that the requester knows that they will be charged for the request.
            tagging: The tag-set for the object.
            object_lock_mode: The Object Lock mode that you want to apply to this object.
            object_lock_retain_until_date: The date and time when you want this object's Object Lock to expire.
            object_lock_legal_hold_status: Specifies whether a legal hold will be applied to this object.
            expected_bucket_owner: The account ID of the expected bucket owner.
            metadata_directive: Specifies whether the metadata is copied from the source object or
                replaced with metadata provided in the request.

        Returns:
            A dictionary containing the response metadata.

        """
        ...

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
        """Put the ACL for an object.

        Args:
            bucket: The name of the bucket.
            key: The object key.
            acl: The canned ACL to apply to the object.
            access_control_policy: Contains the elements that set the ACL permissions.
            content_md5: The base64-encoded 128-bit MD5 digest of the data.
            checksum_algorithm: Indicates the algorithm used to create the checksum.
            grant_full_control: Allows grantee the read, write, read ACP, and write ACP permissions on the object.
            grant_read: Allows grantee to read the object data and its metadata.
            grant_read_acp: Allows grantee to read the object ACL.
            grant_write: Allows grantee to write the ACL for the applicable object.
            grant_write_acp: Allows grantee to write the ACL for the applicable object.
            request_payer: Confirms that the requester knows that they will be charged for the request.
            version_id: Version ID used to reference a specific version of the object.
            expected_bucket_owner: The account ID of the expected bucket owner.

        Returns:
            A dictionary containing the response metadata.

        """
        ...

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
        """Put the legal hold for an object.

        Args:
            bucket: The name of the bucket.
            key: The object key.
            legal_hold: Container element for the Object Lock legal hold status.
            request_payer: Confirms that the requester knows that they will be charged for the request.
            version_id: Version ID used to reference a specific version of the object.
            expected_bucket_owner: The account ID of the expected bucket owner.
            content_md5: The base64-encoded 128-bit MD5 digest of the data.
            checksum_algorithm: Indicates the algorithm used to create the checksum.

        Returns:
            A dictionary containing the response metadata.

        """
        ...

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
        """Put the lock configuration for a bucket.

        Args:
            bucket: The name of the bucket.
            object_lock_configuration: The Object Lock configuration that you want to apply to the specified bucket.
            request_payer: Confirms that the requester knows that they will be charged for the request.
            token: A token to allow Object Lock to be enabled for an existing bucket.
            content_md5: The base64-encoded 128-bit MD5 digest of the data.
            checksum_algorithm: Indicates the algorithm used to create the checksum.
            expected_bucket_owner: The account ID of the expected bucket owner.

        Returns:
            A dictionary containing the response metadata.

        """
        ...

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
        """Put the retention for an object.

        Args:
            bucket: The name of the bucket.
            key: The object key.
            retention: The container element for the Object Lock retention configuration.
            request_payer: Confirms that the requester knows that they will be charged for the request.
            version_id: Version ID used to reference a specific version of the object.
            bypass_governance_retention: Indicates whether this action should bypass Governance-mode restrictions.
            expected_bucket_owner: The account ID of the expected bucket owner.
            content_md5: The base64-encoded 128-bit MD5 digest of the data.
            checksum_algorithm: Indicates the algorithm used to create the checksum.

        Returns:
            A dictionary containing the response metadata.

        """
        ...

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
        """Generate a presigned URL for an S3 object.

        A presigned URL gives time-limited permission to download or upload objects.
        The URL can be entered in a browser or used by a program to access the object.
        The credentials used by the presigned URL are those of the AWS user who generated the URL.

        Args:
            bucket: The name of the bucket containing the object.
            key: The key (path and filename) of the object in the S3 bucket.
            client_method: The S3 client method that the URL performs.
                - "get_object": Generate a URL for downloading an object (default).
                - "put_object": Generate a URL for uploading an object.
            expires_in: The number of seconds the presigned URL is valid for. Default: 3600 (1 hour).
                Maximum: 604800 (7 days) when using AWS CLI, 43200 (12 hours) when using console.
            version_id: Version ID used to reference a specific version of the object.
            response_content_type: Sets the Content-Type header of the response (for get_object).
            response_content_disposition: Sets the Content-Disposition header of the response (for get_object).
            response_content_encoding: Sets the Content-Encoding header of the response (for get_object).
            response_content_language: Sets the Content-Language header of the response (for get_object).
            response_cache_control: Sets the Cache-Control header of the response (for get_object).
            response_expires: Sets the Expires header of the response (for get_object).
            content_type: A standard MIME type describing the format of the object data (for put_object).
            content_md5: The base64-encoded 128-bit MD5 digest of the data (for put_object).
            metadata: A map of metadata to store with the object in S3 (for put_object).
            server_side_encryption: The server-side encryption algorithm used when storing this object (for put_object).
            sse_customer_algorithm: Specifies the algorithm to use when encrypting/decrypting the object.
            sse_customer_key: Specifies the customer-provided encryption key.
            sse_kms_key_id: Specifies the ID of the customer managed KMS key (for put_object).
            acl: The canned ACL to apply to the object (for put_object).

        Returns:
            A presigned URL string that can be used to access the object.

        Raises:
            S3NoSuchBucketClientException: If the bucket does not exist.
            S3NoSuchKeyClientException: If the object key does not exist (for get_object).
            S3AccessDeniedClientException: If access is denied.
            S3InvalidRequestClientException: If the request is invalid.
            S3ServiceClientException: For other S3 service errors.

        Example:
            ```python
            # Generate a presigned URL for downloading an object
            url = await s3_client.generate_presigned_url(
                bucket="my-bucket",
                key="path/to/file.txt",
                client_method="get_object",
                expires_in=3600
            )
            print(f"Download URL: {url}")

            # Generate a presigned URL for uploading an object
            url = await s3_client.generate_presigned_url(
                bucket="my-bucket",
                key="path/to/upload.txt",
                client_method="put_object",
                expires_in=1800,
                content_type="text/plain"
            )
            print(f"Upload URL: {url}")
            ```

        """
        ...
