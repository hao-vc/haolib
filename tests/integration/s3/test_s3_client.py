"""Test the S3 AIOboto3 client."""

import json
from datetime import UTC, datetime

import pytest

from haolib.database.files.s3.clients.abstract import (
    AbstractS3Client,
    S3BucketAlreadyExistsClientException,
    S3BucketAlreadyOwnedByYouClientException,
    S3BucketNotEmptyClientException,
    S3InvalidRequestClientException,
    S3NoSuchBucketClientException,
    S3NoSuchCORSConfigurationClientException,
    S3NoSuchKeyClientException,
    S3NoSuchLifecycleConfigurationClientException,
    S3NoSuchPolicyClientException,
)
from haolib.database.files.s3.clients.pydantic import (
    S3CORSConfiguration,
    S3CORSRule,
    S3DefaultRetention,
    S3DeleteObjectsDelete,
    S3DeleteObjectsDeleteObject,
    S3LifecycleConfiguration,
    S3LifecycleExpiration,
    S3LifecycleRule,
    S3ObjectLockConfiguration,
    S3ObjectLockLegalHold,
    S3ObjectLockRetention,
    S3ObjectLockRule,
)


@pytest.mark.asyncio
async def test_create_bucket(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test creating a bucket."""
    bucket_name = "test-bucket"
    response = await s3_client.create_bucket(bucket_name)
    assert response.location is not None
    assert bucket_name in response.location


@pytest.mark.asyncio
async def test_create_bucket_already_exists(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test creating a bucket that already exists."""
    bucket_name = "test-bucket-exists"
    await s3_client.create_bucket(bucket_name)
    # MinIO returns BucketAlreadyOwnedByYou, AWS returns BucketAlreadyExists
    with pytest.raises((S3BucketAlreadyExistsClientException, S3BucketAlreadyOwnedByYouClientException)):
        await s3_client.create_bucket(bucket_name)


@pytest.mark.asyncio
async def test_delete_bucket(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test deleting a bucket."""
    bucket_name = "test-bucket-delete"
    await s3_client.create_bucket(bucket_name)
    response = await s3_client.delete_bucket(bucket_name)
    assert response is not None


@pytest.mark.asyncio
async def test_delete_bucket_not_exists(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test deleting a non-existent bucket."""
    with pytest.raises(S3NoSuchBucketClientException):
        await s3_client.delete_bucket("non-existent-bucket")


@pytest.mark.asyncio
async def test_delete_bucket_not_empty(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test deleting a bucket that is not empty."""
    bucket_name = "test-bucket-not-empty"
    await s3_client.create_bucket(bucket_name)
    await s3_client.put_object(bucket_name, "test-key", body=b"test-data")
    with pytest.raises(S3BucketNotEmptyClientException):
        await s3_client.delete_bucket(bucket_name)


@pytest.mark.asyncio
async def test_list_buckets(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test listing buckets."""
    bucket1 = "test-bucket-1"
    bucket2 = "test-bucket-2"
    await s3_client.create_bucket(bucket1)
    await s3_client.create_bucket(bucket2)
    response = await s3_client.list_buckets()
    assert response.buckets is not None
    bucket_names = [b.name for b in response.buckets if b.name]
    assert bucket1 in bucket_names
    assert bucket2 in bucket_names


@pytest.mark.asyncio
async def test_list_buckets_empty(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test listing buckets when none exist."""
    response = await s3_client.list_buckets()
    # MinIO may return None or empty list when no buckets exist
    if response.buckets is not None:
        assert isinstance(response.buckets, list)
        assert len(response.buckets) == 0


@pytest.mark.asyncio
async def test_put_object(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test putting an object."""
    bucket_name = "test-bucket-put"
    key = "test-key"
    body = b"test-data"
    await s3_client.create_bucket(bucket_name)
    response = await s3_client.put_object(bucket_name, key, body=body)
    assert response.etag is not None
    # version_id may be None if versioning is not enabled (MinIO default)


@pytest.mark.asyncio
async def test_put_object_bucket_not_exists(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test putting an object to a non-existent bucket."""
    with pytest.raises(S3NoSuchBucketClientException):
        await s3_client.put_object("non-existent-bucket", "test-key", body=b"test-data")


@pytest.mark.asyncio
async def test_get_object(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test getting an object."""
    bucket_name = "test-bucket-get"
    key = "test-key"
    body = b"test-data"
    await s3_client.create_bucket(bucket_name)
    await s3_client.put_object(bucket_name, key, body=body)
    response = await s3_client.get_object(bucket_name, key)
    assert response.body == body
    assert response.etag is not None
    assert response.content_length is not None


@pytest.mark.asyncio
async def test_get_object_not_exists(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test getting a non-existent object."""
    bucket_name = "test-bucket-get-not-exists"
    await s3_client.create_bucket(bucket_name)
    with pytest.raises(S3NoSuchKeyClientException):
        await s3_client.get_object(bucket_name, "non-existent-key")


@pytest.mark.asyncio
async def test_get_object_bucket_not_exists(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test getting an object from a non-existent bucket."""
    with pytest.raises(S3NoSuchBucketClientException):
        await s3_client.get_object("non-existent-bucket", "test-key")


@pytest.mark.asyncio
async def test_delete_object(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test deleting an object."""
    bucket_name = "test-bucket-delete-obj"
    key = "test-key"
    await s3_client.create_bucket(bucket_name)
    await s3_client.put_object(bucket_name, key, body=b"test-data")
    await s3_client.delete_object(bucket_name, key)
    # delete_marker may be None if versioning is not enabled (MinIO default)
    # Verify object is deleted
    with pytest.raises(S3NoSuchKeyClientException):
        await s3_client.get_object(bucket_name, key)


@pytest.mark.asyncio
async def test_delete_object_not_exists(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test deleting a non-existent object."""
    bucket_name = "test-bucket-delete-not-exists"
    await s3_client.create_bucket(bucket_name)
    # S3/MinIO doesn't raise an exception when deleting a non-existent object
    # It just succeeds (this is correct S3 behavior)
    response = await s3_client.delete_object(bucket_name, "non-existent-key")
    assert response is not None


@pytest.mark.asyncio
async def test_delete_objects(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test deleting multiple objects."""
    bucket_name = "test-bucket-delete-multiple"
    await s3_client.create_bucket(bucket_name)
    await s3_client.put_object(bucket_name, "key1", body=b"data1")
    await s3_client.put_object(bucket_name, "key2", body=b"data2")
    await s3_client.put_object(bucket_name, "key3", body=b"data3")
    delete_objects = S3DeleteObjectsDelete(
        objects=[
            S3DeleteObjectsDeleteObject(key="key1"),
            S3DeleteObjectsDeleteObject(key="key2"),
            S3DeleteObjectsDeleteObject(key="key3"),
        ]
    )
    response = await s3_client.delete_objects(bucket_name, delete_objects)

    assert len(response.deleted) == len(delete_objects.objects)
    assert len(response.error) == 0


@pytest.mark.asyncio
async def test_delete_objects_partial(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test deleting objects where some don't exist."""
    bucket_name = "test-bucket-delete-partial"
    await s3_client.create_bucket(bucket_name)
    await s3_client.put_object(bucket_name, "key1", body=b"data1")
    delete_objects = S3DeleteObjectsDelete(
        objects=[
            S3DeleteObjectsDeleteObject(key="key1"),
            S3DeleteObjectsDeleteObject(key="non-existent-key"),
        ]
    )
    response = await s3_client.delete_objects(bucket_name, delete_objects)
    # MinIO/S3 may return non-existent objects as deleted (no error)
    # This is correct S3 behavior - delete_objects doesn't fail for non-existent keys
    assert len(response.deleted) >= 1  # At least key1 should be deleted
    # key1 should definitely be in deleted
    deleted_keys = [item.key for item in response.deleted]
    assert "key1" in deleted_keys


@pytest.mark.asyncio
async def test_copy_object(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test copying an object."""
    source_bucket = "test-bucket-source"
    dest_bucket = "test-bucket-dest"
    source_key = "source-key"
    dest_key = "dest-key"
    body = b"test-data"
    await s3_client.create_bucket(source_bucket)
    await s3_client.create_bucket(dest_bucket)
    await s3_client.put_object(source_bucket, source_key, body=body)
    response = await s3_client.copy_object(dest_bucket, f"{source_bucket}/{source_key}", dest_key)
    assert response.copy_object_result is not None
    # Verify copied object exists
    copied_obj = await s3_client.get_object(dest_bucket, dest_key)
    assert copied_obj.body == body


@pytest.mark.asyncio
async def test_copy_object_source_not_exists(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test copying a non-existent object."""
    dest_bucket = "test-bucket-dest"
    await s3_client.create_bucket(dest_bucket)
    # Source bucket doesn't exist, so we get NoSuchBucket, not NoSuchKey
    with pytest.raises((S3NoSuchKeyClientException, S3NoSuchBucketClientException)):
        await s3_client.copy_object(dest_bucket, "non-existent-bucket/non-existent-key", "dest-key")


@pytest.mark.asyncio
async def test_list_objects(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test listing objects."""
    bucket_name = "test-bucket-list"
    await s3_client.create_bucket(bucket_name)
    await s3_client.put_object(bucket_name, "key1", body=b"data1")
    await s3_client.put_object(bucket_name, "key2", body=b"data2")
    await s3_client.put_object(bucket_name, "key3", body=b"data3")
    response = await s3_client.list_objects(bucket_name)
    assert response.contents is not None
    total_objects = 3
    assert len(response.contents) == total_objects
    keys = [obj.key for obj in response.contents if obj.key]
    assert "key1" in keys
    assert "key2" in keys
    assert "key3" in keys


@pytest.mark.asyncio
async def test_list_objects_with_prefix(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test listing objects with prefix."""
    bucket_name = "test-bucket-list-prefix"
    await s3_client.create_bucket(bucket_name)
    await s3_client.put_object(bucket_name, "prefix1/key1", body=b"data1")
    await s3_client.put_object(bucket_name, "prefix1/key2", body=b"data2")
    await s3_client.put_object(bucket_name, "prefix2/key3", body=b"data3")
    response = await s3_client.list_objects(bucket_name, prefix="prefix1/")
    assert response.contents is not None
    total_objects = 2
    assert len(response.contents) == total_objects
    keys = [obj.key for obj in response.contents if obj.key]
    assert "prefix1/key1" in keys
    assert "prefix1/key2" in keys
    assert "prefix2/key3" not in keys


@pytest.mark.asyncio
async def test_list_objects_v2(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test listing objects v2."""
    bucket_name = "test-bucket-list-v2"
    await s3_client.create_bucket(bucket_name)
    await s3_client.put_object(bucket_name, "key1", body=b"data1")
    await s3_client.put_object(bucket_name, "key2", body=b"data2")
    response = await s3_client.list_objects_v2(bucket_name)
    assert response.contents is not None
    total_objects = 2
    assert response.key_count == total_objects


@pytest.mark.asyncio
async def test_list_objects_bucket_not_exists(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test listing objects from a non-existent bucket."""
    with pytest.raises(S3NoSuchBucketClientException):
        await s3_client.list_objects("non-existent-bucket")


@pytest.mark.asyncio
async def test_put_bucket_cors(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test putting bucket CORS configuration."""
    bucket_name = "test-bucket-cors"
    await s3_client.create_bucket(bucket_name)
    cors_config = S3CORSConfiguration(
        cors_rules=[
            S3CORSRule(
                allowed_methods=["GET", "POST"],
                allowed_origins=["*"],
                allowed_headers=["*"],
                max_age_seconds=3600,
            )
        ]
    )
    # MinIO may not support CORS, so handle InvalidRequestException
    try:
        response = await s3_client.put_bucket_cors(bucket_name, cors_config)
        assert response is not None
    except S3InvalidRequestClientException:
        # MinIO doesn't support CORS, skip this test
        pytest.skip("CORS not supported by this S3 backend")


@pytest.mark.asyncio
async def test_get_bucket_cors(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test getting bucket CORS configuration."""
    bucket_name = "test-bucket-get-cors"
    await s3_client.create_bucket(bucket_name)
    cors_config = S3CORSConfiguration(
        cors_rules=[
            S3CORSRule(
                allowed_methods=["GET", "POST"],
                allowed_origins=["*"],
            )
        ]
    )
    # MinIO may not support CORS, so handle InvalidRequestException
    try:
        await s3_client.put_bucket_cors(bucket_name, cors_config)
        response = await s3_client.get_bucket_cors(bucket_name)
        assert response.cors_rules is not None
        assert len(response.cors_rules) == 1
    except S3InvalidRequestClientException:
        # MinIO doesn't support CORS, skip this test
        pytest.skip("CORS not supported by this S3 backend")


@pytest.mark.asyncio
async def test_get_bucket_cors_not_exists(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test getting CORS configuration that doesn't exist."""
    bucket_name = "test-bucket-get-cors-not-exists"
    await s3_client.create_bucket(bucket_name)
    with pytest.raises(S3NoSuchCORSConfigurationClientException):
        await s3_client.get_bucket_cors(bucket_name)


@pytest.mark.asyncio
async def test_delete_bucket_cors(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test deleting bucket CORS configuration."""
    bucket_name = "test-bucket-delete-cors"
    await s3_client.create_bucket(bucket_name)
    cors_config = S3CORSConfiguration(
        cors_rules=[
            S3CORSRule(
                allowed_methods=["GET"],
                allowed_origins=["*"],
            )
        ]
    )
    # MinIO may not support CORS, so handle InvalidRequestException
    try:
        await s3_client.put_bucket_cors(bucket_name, cors_config)
        response = await s3_client.delete_bucket_cors(bucket_name)
        assert response is not None
        # Verify CORS is deleted
        with pytest.raises(S3NoSuchCORSConfigurationClientException):
            await s3_client.get_bucket_cors(bucket_name)
    except S3InvalidRequestClientException:
        # MinIO doesn't support CORS, skip this test
        pytest.skip("CORS not supported by this S3 backend")


@pytest.mark.asyncio
async def test_put_bucket_lifecycle_configuration(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test putting bucket lifecycle configuration."""
    bucket_name = "test-bucket-lifecycle"
    await s3_client.create_bucket(bucket_name)
    lifecycle_config = S3LifecycleConfiguration(
        rules=[
            S3LifecycleRule(
                id="rule1",
                status="Enabled",
                expiration=S3LifecycleExpiration(days=30),
            )
        ]
    )
    # MinIO may not support lifecycle, so handle InvalidRequestException
    try:
        response = await s3_client.put_bucket_lifecycle_configuration(bucket_name, lifecycle_config)
        assert response is not None
    except S3InvalidRequestClientException:
        # MinIO doesn't support lifecycle, skip this test
        pytest.skip("Lifecycle configuration not supported by this S3 backend")


@pytest.mark.asyncio
async def test_get_bucket_lifecycle_configuration(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test getting bucket lifecycle configuration."""
    bucket_name = "test-bucket-get-lifecycle"
    await s3_client.create_bucket(bucket_name)
    # S3 requires at least one action (Expiration, Transition, etc.) in lifecycle rules
    lifecycle_config = S3LifecycleConfiguration(
        rules=[
            S3LifecycleRule(
                id="rule1",
                status="Enabled",
                expiration=S3LifecycleExpiration(days=30),
            )
        ]
    )
    # MinIO may not support lifecycle, so handle InvalidRequestException
    try:
        await s3_client.put_bucket_lifecycle_configuration(bucket_name, lifecycle_config)
        response = await s3_client.get_bucket_lifecycle_configuration(bucket_name)
        assert response.rules is not None
        assert len(response.rules) == 1
    except S3InvalidRequestClientException:
        # MinIO doesn't support lifecycle, skip this test
        pytest.skip("Lifecycle configuration not supported by this S3 backend")


@pytest.mark.asyncio
async def test_get_bucket_lifecycle_configuration_not_exists(
    s3_client: AbstractS3Client, clean_all_buckets: None
) -> None:
    """Test getting lifecycle configuration that doesn't exist."""
    bucket_name = "test-bucket-get-lifecycle-not-exists"
    await s3_client.create_bucket(bucket_name)
    response = await s3_client.get_bucket_lifecycle_configuration(bucket_name)
    assert response.rules is not None
    assert response.rules == []


@pytest.mark.asyncio
async def test_delete_bucket_lifecycle_configuration(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test deleting bucket lifecycle configuration."""
    bucket_name = "test-bucket-delete-lifecycle"
    await s3_client.create_bucket(bucket_name)
    # S3 requires at least one action (Expiration, Transition, etc.) in lifecycle rules
    lifecycle_config = S3LifecycleConfiguration(
        rules=[
            S3LifecycleRule(
                id="rule1",
                status="Enabled",
                expiration=S3LifecycleExpiration(days=30),
            )
        ]
    )
    # MinIO may not support lifecycle, so handle InvalidRequestException
    try:
        await s3_client.put_bucket_lifecycle_configuration(bucket_name, lifecycle_config)
        response = await s3_client.delete_bucket_lifecycle(bucket_name)
        assert response is not None
        # Verify lifecycle is deleted
        configuration_response = await s3_client.get_bucket_lifecycle_configuration(bucket_name)
        assert configuration_response.rules == []
    except S3InvalidRequestClientException:
        # MinIO doesn't support lifecycle, skip this test
        pytest.skip("Lifecycle configuration not supported by this S3 backend")


@pytest.mark.asyncio
async def test_delete_bucket_lifecycle_configuration_not_exists(
    s3_client: AbstractS3Client, clean_all_buckets: None
) -> None:
    """Test deleting lifecycle configuration that doesn't exist."""
    bucket_name = "test-bucket-delete-lifecycle-not-exists"
    await s3_client.create_bucket(bucket_name)
    # MinIO may not support lifecycle, so handle InvalidRequestException
    try:
        # MinIO/S3 may not raise an exception when deleting non-existent lifecycle config
        # It may just succeed (similar to delete_object behavior)
        try:
            response = await s3_client.delete_bucket_lifecycle(bucket_name)
            # If it succeeds, that's also valid behavior
            assert response is not None
        except S3NoSuchLifecycleConfigurationClientException:
            # Some backends raise this exception, which is also valid
            pass
    except S3InvalidRequestClientException:
        # MinIO doesn't support lifecycle, skip this test
        pytest.skip("Lifecycle configuration not supported by this S3 backend")


@pytest.mark.asyncio
async def test_put_bucket_policy(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test putting bucket policy."""
    bucket_name = "test-bucket-policy"
    await s3_client.create_bucket(bucket_name)
    policy = '{"Version": "2012-10-17", "Statement": []}'
    response = await s3_client.put_bucket_policy(bucket_name, policy)
    assert response is not None


@pytest.mark.asyncio
async def test_get_bucket_policy(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test getting bucket policy."""
    bucket_name = "test-bucket-get-policy"
    await s3_client.create_bucket(bucket_name)
    policy = '{"Version": "2012-10-17", "Statement": []}'
    # MinIO may not support policy, so handle InvalidRequestException
    try:
        await s3_client.put_bucket_policy(bucket_name, policy)
        response = await s3_client.get_bucket_policy(bucket_name)
        # MinIO/S3 may normalize JSON (remove whitespace), so parse and compare
        assert response.policy is not None
        assert json.loads(response.policy) == json.loads(policy)
    except S3InvalidRequestClientException:
        # MinIO doesn't support policy, skip this test
        pytest.skip("Bucket policy not supported by this S3 backend")


@pytest.mark.asyncio
async def test_get_bucket_policy_not_exists(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test getting policy that doesn't exist."""
    bucket_name = "test-bucket-get-policy-not-exists"
    await s3_client.create_bucket(bucket_name)
    with pytest.raises(S3NoSuchPolicyClientException):
        await s3_client.get_bucket_policy(bucket_name)


@pytest.mark.asyncio
async def test_delete_bucket_policy(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test deleting bucket policy."""
    bucket_name = "test-bucket-delete-policy"
    await s3_client.create_bucket(bucket_name)
    policy = '{"Version": "2012-10-17", "Statement": []}'
    await s3_client.put_bucket_policy(bucket_name, policy)
    response = await s3_client.delete_bucket_policy(bucket_name)
    assert response is not None
    # Verify policy is deleted
    with pytest.raises(S3NoSuchPolicyClientException):
        await s3_client.get_bucket_policy(bucket_name)


@pytest.mark.asyncio
async def test_put_bucket_acl(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test putting bucket ACL."""
    bucket_name = "test-bucket-acl"
    await s3_client.create_bucket(bucket_name)
    response = await s3_client.put_bucket_acl(bucket_name, acl="private")
    assert response is not None


@pytest.mark.asyncio
async def test_get_bucket_acl(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test getting bucket ACL."""
    bucket_name = "test-bucket-get-acl"
    await s3_client.create_bucket(bucket_name)
    await s3_client.put_bucket_acl(bucket_name, acl="private")
    response = await s3_client.get_bucket_acl(bucket_name)
    assert response.grants is not None
    assert response.owner is not None


@pytest.mark.asyncio
async def test_put_object_acl(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test putting object ACL."""
    bucket_name = "test-bucket-obj-acl"
    key = "test-key"
    await s3_client.create_bucket(bucket_name)
    await s3_client.put_object(bucket_name, key, body=b"test-data")
    response = await s3_client.put_object_acl(bucket_name, key, acl="private")
    assert response is not None


@pytest.mark.asyncio
async def test_get_object_acl(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test getting object ACL."""
    bucket_name = "test-bucket-get-obj-acl"
    key = "test-key"
    await s3_client.create_bucket(bucket_name)
    await s3_client.put_object(bucket_name, key, body=b"test-data")
    await s3_client.put_object_acl(bucket_name, key, acl="private")
    response = await s3_client.get_object_acl(bucket_name, key)
    assert response.grants is not None
    assert response.owner is not None


@pytest.mark.asyncio
async def test_put_object_legal_hold(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test putting object legal hold."""
    bucket_name = "test-bucket-legal-hold"
    key = "test-key"
    await s3_client.create_bucket(bucket_name)
    await s3_client.put_object(bucket_name, key, body=b"test-data")
    legal_hold = S3ObjectLockLegalHold(status="ON")
    # MinIO may not support object lock, so handle InvalidRequestException
    try:
        response = await s3_client.put_object_legal_hold(bucket_name, key, legal_hold=legal_hold)
        assert response is not None
    except S3InvalidRequestClientException:
        # MinIO doesn't support object lock, skip this test
        pytest.skip("Object lock not supported by this S3 backend")


@pytest.mark.asyncio
async def test_put_object_retention(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test putting object retention."""
    bucket_name = "test-bucket-retention"
    key = "test-key"
    await s3_client.create_bucket(bucket_name)
    await s3_client.put_object(bucket_name, key, body=b"test-data")
    retention = S3ObjectLockRetention(mode="GOVERNANCE", retain_until_date=datetime.now(UTC))
    # MinIO may not support object lock, so handle InvalidRequestException
    try:
        response = await s3_client.put_object_retention(bucket_name, key, retention=retention)
        assert response is not None
    except S3InvalidRequestClientException:
        # MinIO doesn't support object lock, skip this test
        pytest.skip("Object lock not supported by this S3 backend")


@pytest.mark.asyncio
async def test_put_object_lock_configuration(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test putting object lock configuration."""
    bucket_name = "test-bucket-lock-config"
    await s3_client.create_bucket(bucket_name)
    lock_config = S3ObjectLockConfiguration(
        object_lock_enabled="Enabled",
        rule=S3ObjectLockRule(default_retention=S3DefaultRetention(mode="GOVERNANCE", days=30)),
    )
    # MinIO may not support object lock on existing buckets, so handle InvalidRequestException
    try:
        response = await s3_client.put_object_lock_configuration(bucket_name, object_lock_configuration=lock_config)
        assert response is not None
    except S3InvalidRequestClientException:
        # MinIO doesn't support object lock on existing buckets, skip this test
        pytest.skip("Object lock configuration not supported by this S3 backend")


@pytest.mark.asyncio
async def test_empty_object(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test putting and getting an empty object."""
    bucket_name = "test-bucket-empty"
    key = "empty-key"
    await s3_client.create_bucket(bucket_name)
    await s3_client.put_object(bucket_name, key, body=b"")
    response = await s3_client.get_object(bucket_name, key)
    assert response.body == b""
    assert response.content_length == 0


@pytest.mark.asyncio
async def test_large_object(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test putting and getting a large object."""
    bucket_name = "test-bucket-large"
    key = "large-key"
    large_body = b"x" * 10000
    await s3_client.create_bucket(bucket_name)
    await s3_client.put_object(bucket_name, key, body=large_body)
    response = await s3_client.get_object(bucket_name, key)
    assert response.body == large_body
    large_body_length = 10000
    assert response.content_length == large_body_length


@pytest.mark.asyncio
async def test_special_characters_in_key(s3_client: AbstractS3Client, clean_all_buckets: None) -> None:
    """Test putting and getting objects with special characters in key."""
    bucket_name = "test-bucket-special"
    key = "test/key with spaces & special-chars.txt"
    body = b"test-data"
    await s3_client.create_bucket(bucket_name)
    await s3_client.put_object(bucket_name, key, body=body)
    response = await s3_client.get_object(bucket_name, key)
    assert response.body == body
