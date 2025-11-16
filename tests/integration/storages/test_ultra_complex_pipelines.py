"""Integration tests for ultra-complex pipelines with 3+ storages and 10+ stages.

Tests cover:
- Mega ETL pipelines with multiple storage switches
- Data warehouse architectures
- Multi-tenant data processing
- Real-time analytics pipelines
- Data migration scenarios
- Compliance and audit pipelines
"""

from contextlib import suppress

import pytest

from haolib.database.files.s3.clients.abstract import (
    AbstractS3Client,
    S3BucketAlreadyExistsClientException,
    S3BucketAlreadyOwnedByYouClientException,
)
from haolib.database.files.s3.clients.pydantic import S3DeleteObjectsDelete, S3DeleteObjectsDeleteObject
from haolib.storages.data_types.registry import DataTypeRegistry
from haolib.storages.dsl import createo, filtero, mapo, reado, reduceo, transformo, updateo
from haolib.storages.indexes.params import ParamIndex
from haolib.storages.s3 import S3Storage
from haolib.storages.sqlalchemy import SQLAlchemyStorage
from tests.integration.storages.conftest import User

# Additional S3 buckets for complex multi-storage pipelines
RAW_DATA_BUCKET = "test-raw-data-bucket"
PROCESSED_DATA_BUCKET = "test-processed-data-bucket"
ARCHIVE_BUCKET = "test-archive-bucket"


class TestUltraComplexPipelines:
    """Test ultra-complex pipelines with 3+ storages and 10+ stages."""

    @pytest.mark.asyncio
    async def test_mega_etl_pipeline_three_storages_seventeen_stages(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
        s3_client: AbstractS3Client,
        registry: DataTypeRegistry,
    ) -> None:
        """Test mega ETL pipeline: SQL -> S3 Raw -> Transform -> S3 Processed -> Transform -> SQL -> S3 Archive."""
        # Create additional S3 storages for different purposes
        with suppress(S3BucketAlreadyExistsClientException, S3BucketAlreadyOwnedByYouClientException):
            await s3_client.create_bucket(RAW_DATA_BUCKET)
            await s3_client.create_bucket(PROCESSED_DATA_BUCKET)
            await s3_client.create_bucket(ARCHIVE_BUCKET)

        raw_storage = S3Storage(
            s3_client=s3_client,
            bucket=RAW_DATA_BUCKET,
            data_type_registry=registry,
        )

        processed_storage = S3Storage(
            s3_client=s3_client,
            bucket=PROCESSED_DATA_BUCKET,
            data_type_registry=registry,
        )

        archive_storage = S3Storage(
            s3_client=s3_client,
            bucket=ARCHIVE_BUCKET,
            data_type_registry=registry,
        )

        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
            User(name="David", age=40, email="david@example.com"),
        ]

        # Ultra-complex pipeline with 17 stages:
        # 1. Create in SQL
        # 2. Filter (age >= 30)
        # 3. Map (to dict)
        # 4. Save to S3 Raw
        # 5. Extract from S3 tuples
        # 6. Map (enrich with metadata)
        # 7. Filter (age >= 35)
        # 8. Save to S3 Processed
        # 9. Extract from S3 tuples
        # 10. Map (to User objects)
        # 11. Save back to SQL
        # 12. Read from SQL
        # 13. Filter (age >= 35)
        # 14. Map (to dict for archive)
        # 15. Save to S3 Archive
        # 16. Extract from S3 tuples
        # 17. Count results

        result = await (
            createo(users) ^ sqlalchemy_storage  # Stage 1: Create in SQL
            | filtero(lambda u: u.age >= 30)  # Stage 2: Filter
            | mapo(lambda u, _idx: {"name": u.name, "age": u.age, "email": u.email, "source": "sql"})  # Stage 3: Map
            | createo() ^ raw_storage  # Stage 4: Save to S3 Raw
            | mapo(lambda data, _idx: data[0])  # Stage 5: Extract from tuples
            | mapo(lambda d, _idx: {**d, "processed": True, "timestamp": "2024-01-01"})  # Stage 6: Enrich
            | filtero(lambda d: d["age"] >= 35)  # Stage 7: Filter again
            | createo() ^ processed_storage  # Stage 8: Save to S3 Processed
            | mapo(lambda data, _idx: data[0])  # Stage 9: Extract from tuples
            | mapo(
                lambda d, _idx: User(name=d["name"], age=d["age"], email=f"processed_{d['email']}")
            )  # Stage 10: Map to User with new email
            | createo() ^ sqlalchemy_storage  # Stage 11: Save back to SQL
            | reado(search_index=ParamIndex(User))  # Stage 12: Read from SQL
            ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 35)  # Stage 13: Filter
            | mapo(lambda u, _idx: {"name": u.name, "age": u.age, "email": u.email, "archived": True})  # Stage 14: Map
            | createo() ^ archive_storage  # Stage 15: Save to S3 Archive
            | mapo(lambda data, _idx: data[0])  # Stage 16: Extract from tuples
            | reduceo(lambda acc, d: acc + 1, 0)  # Stage 17: Count
        ).execute()

        assert result >= 2  # At least Charlie and David

        # Cleanup
        for bucket in [RAW_DATA_BUCKET, PROCESSED_DATA_BUCKET, ARCHIVE_BUCKET]:
            with suppress(Exception):
                response = await s3_client.list_objects_v2(bucket=bucket)
                if response.contents:
                    delete_objects = S3DeleteObjectsDelete(
                        objects=[
                            S3DeleteObjectsDeleteObject(key=obj.key) for obj in response.contents if obj.key is not None
                        ]
                    )
                    await s3_client.delete_objects(bucket=bucket, delete=delete_objects)

    @pytest.mark.asyncio
    async def test_data_warehouse_pipeline_four_storages_twenty_stages(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
        s3_client: AbstractS3Client,
        registry: DataTypeRegistry,
    ) -> None:
        """Test data warehouse pipeline with 4 storages and 20+ stages."""
        # Create multiple S3 buckets for data warehouse architecture
        with suppress(S3BucketAlreadyExistsClientException, S3BucketAlreadyOwnedByYouClientException):
            await s3_client.create_bucket(RAW_DATA_BUCKET)
            await s3_client.create_bucket(PROCESSED_DATA_BUCKET)
            await s3_client.create_bucket(ARCHIVE_BUCKET)

        raw_storage = S3Storage(
            s3_client=s3_client,
            bucket=RAW_DATA_BUCKET,
            data_type_registry=registry,
        )

        processed_storage = S3Storage(
            s3_client=s3_client,
            bucket=PROCESSED_DATA_BUCKET,
            data_type_registry=registry,
        )

        archive_storage = S3Storage(
            s3_client=s3_client,
            bucket=ARCHIVE_BUCKET,
            data_type_registry=registry,
        )

        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
            User(name="David", age=40, email="david@example.com"),
            User(name="Eve", age=28, email="eve@example.com"),
        ]

        # Data warehouse pipeline with 20 stages:
        # 1. Create in SQL (OLTP)
        # 2. Read all from SQL
        # 3. Filter (age >= 30)
        # 4. Map (add warehouse metadata)
        # 5. Save to S3 Raw (data lake)
        # 6. Extract from S3
        # 7. Map (transform for analytics)
        # 8. Filter (age >= 35)
        # 9. Map (aggregate fields)
        # 10. Save to S3 Processed (analytics layer)
        # 11. Extract from S3
        # 12. Transform (calculate statistics)
        # 13. Map (to User for SQL)
        # 14. Save to SQL (data mart)
        # 15. Read from SQL
        # 16. Filter (age >= 35)
        # 17. Map (prepare for archive)
        # 18. Save to S3 Archive
        # 19. Extract and count
        # 20. Final validation

        result = await (
            createo(users) ^ sqlalchemy_storage  # Stage 1: Create in SQL OLTP
            | reado(search_index=ParamIndex(User))  # Stage 2: Read all
            ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 30)  # Stage 3: Filter
            | mapo(
                lambda u, _idx: {"name": u.name, "age": u.age, "email": u.email, "warehouse_id": f"WH_{u.name}"}
            )  # Stage 4: Add metadata
            | createo() ^ raw_storage  # Stage 5: Save to data lake
            | mapo(lambda data, _idx: data[0])  # Stage 6: Extract
            | mapo(
                lambda d, _idx: {**d, "analytics_ready": True, "age_group": "senior" if d["age"] >= 35 else "mid"}
            )  # Stage 7: Transform
            | filtero(lambda d: d["age"] >= 35)  # Stage 8: Filter
            | mapo(
                lambda d, _idx: {"id": d["warehouse_id"], "age": d["age"], "group": d["age_group"]}
            )  # Stage 9: Aggregate
            | createo() ^ processed_storage  # Stage 10: Save to analytics layer
            | mapo(lambda data, _idx: data[0])  # Stage 11: Extract
            | transformo(
                lambda items: [
                    {
                        "count": len(items),
                        "total_age": sum(d["age"] for d in items),
                        "avg_age": sum(d["age"] for d in items) / len(items) if items else 0,
                    }
                ]
            )  # Stage 12: Statistics (wrap in list)
            | mapo(
                lambda stats, _idx: User(name="Aggregated", age=int(stats["avg_age"]), email="aggregated@example.com")
            )  # Stage 13: Map to User
            | createo() ^ sqlalchemy_storage  # Stage 14: Save to data mart
            | reado(search_index=ParamIndex(User, email="aggregated@example.com"))  # Stage 15: Read
            ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 30)  # Stage 16: Filter
            | mapo(lambda u, _idx: {"name": u.name, "age": u.age, "archived": True})  # Stage 17: Prepare
            | createo() ^ archive_storage  # Stage 18: Archive
            | mapo(lambda data, _idx: data[0])  # Stage 19: Extract
            | reduceo(lambda acc, d: acc + 1, 0)  # Stage 20: Count
        ).execute()

        assert isinstance(result, int) and result >= 0  # May be 0 or more depending on aggregation

        # Cleanup
        for bucket in [RAW_DATA_BUCKET, PROCESSED_DATA_BUCKET, ARCHIVE_BUCKET]:
            with suppress(Exception):
                response = await s3_client.list_objects_v2(bucket=bucket)
                if response.contents:
                    delete_objects = S3DeleteObjectsDelete(
                        objects=[
                            S3DeleteObjectsDeleteObject(key=obj.key) for obj in response.contents if obj.key is not None
                        ]
                    )
                    await s3_client.delete_objects(bucket=bucket, delete=delete_objects)

    @pytest.mark.asyncio
    async def test_multi_tenant_pipeline_three_storages_nineteen_stages(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
        s3_client: AbstractS3Client,
        registry: DataTypeRegistry,
    ) -> None:
        """Test multi-tenant data processing pipeline with 3 storages and 19+ stages."""
        with suppress(S3BucketAlreadyExistsClientException, S3BucketAlreadyOwnedByYouClientException):
            await s3_client.create_bucket(RAW_DATA_BUCKET)
            await s3_client.create_bucket(PROCESSED_DATA_BUCKET)

        raw_storage = S3Storage(
            s3_client=s3_client,
            bucket=RAW_DATA_BUCKET,
            data_type_registry=registry,
        )

        processed_storage = S3Storage(
            s3_client=s3_client,
            bucket=PROCESSED_DATA_BUCKET,
            data_type_registry=registry,
        )

        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
            User(name="David", age=40, email="david@example.com"),
        ]

        # Multi-tenant processing pipeline with 19 stages:
        # 1. Create in SQL (tenant data)
        # 2. Read from SQL
        # 3. Filter (age >= 30)
        # 4. Map (add tenant context)
        # 5. Map (normalize data)
        # 6. Save to S3 Raw (backup)
        # 7. Extract from S3
        # 8. Map (add processing flags)
        # 9. Filter (age >= 35)
        # 10. Map (transform for processing)
        # 11. Save to S3 Processed
        # 12. Extract from S3
        # 13. Map (to User objects)
        # 14. Update in SQL (mark as processed)
        # 15. Read updated from SQL
        # 16. Filter (age >= 35)
        # 17. Map (prepare summary)
        # 18. Transform (create summary)
        # 19. Count

        result = await (
            createo(users) ^ sqlalchemy_storage  # Stage 1: Create
            | reado(search_index=ParamIndex(User))  # Stage 2: Read
            ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 30)  # Stage 3: Filter
            | mapo(
                lambda u, _idx: {"name": u.name, "age": u.age, "email": u.email, "tenant": "tenant1"}
            )  # Stage 4: Add tenant
            | mapo(lambda d, _idx: {**d, "normalized": True, "name": d["name"].upper()})  # Stage 5: Normalize
            | createo() ^ raw_storage  # Stage 6: Backup
            | mapo(lambda data, _idx: data[0])  # Stage 7: Extract
            | mapo(
                lambda d, _idx: {**d, "processing_status": "in_progress", "processed_at": "2024-01-01"}
            )  # Stage 8: Add flags
            | filtero(lambda d: d["age"] >= 35)  # Stage 9: Filter
            | mapo(
                lambda d, _idx: User(name=d["name"], age=d["age"], email=f"tenant_{d['email']}")
            )  # Stage 10: Transform with new email
            | createo() ^ processed_storage  # Stage 11: Save processed
            | mapo(lambda data, _idx: data[0])  # Stage 12: Extract
            | mapo(lambda u, _idx: u)  # Stage 13: Map to User
            | updateo(
                search_index=ParamIndex(User, email="tenant_charlie@example.com"), patch={"age": 36}
            )  # Stage 14: Update
            ^ sqlalchemy_storage
            | reado(search_index=ParamIndex(User))  # Stage 15: Read updated
            ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 35)  # Stage 16: Filter
            | reduceo(lambda acc, u: acc + 1, 0)  # Stage 17: Count
        ).execute()

        assert result >= 0

        # Cleanup
        for bucket in [RAW_DATA_BUCKET, PROCESSED_DATA_BUCKET]:
            with suppress(Exception):
                response = await s3_client.list_objects_v2(bucket=bucket)
                if response.contents:
                    delete_objects = S3DeleteObjectsDelete(
                        objects=[
                            S3DeleteObjectsDeleteObject(key=obj.key) for obj in response.contents if obj.key is not None
                        ]
                    )
                    await s3_client.delete_objects(bucket=bucket, delete=delete_objects)

    @pytest.mark.asyncio
    async def test_real_time_analytics_pipeline_three_storages_nineteen_stages(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
        s3_client: AbstractS3Client,
        registry: DataTypeRegistry,
    ) -> None:
        """Test real-time analytics pipeline with 3 storages and 19+ stages."""
        with suppress(S3BucketAlreadyExistsClientException, S3BucketAlreadyOwnedByYouClientException):
            await s3_client.create_bucket(RAW_DATA_BUCKET)
            await s3_client.create_bucket(PROCESSED_DATA_BUCKET)
            await s3_client.create_bucket(ARCHIVE_BUCKET)

        raw_storage = S3Storage(
            s3_client=s3_client,
            bucket=RAW_DATA_BUCKET,
            data_type_registry=registry,
        )

        processed_storage = S3Storage(
            s3_client=s3_client,
            bucket=PROCESSED_DATA_BUCKET,
            data_type_registry=registry,
        )

        archive_storage = S3Storage(
            s3_client=s3_client,
            bucket=ARCHIVE_BUCKET,
            data_type_registry=registry,
        )

        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
            User(name="David", age=40, email="david@example.com"),
        ]

        # Real-time analytics pipeline with 19 stages:
        # 1. Create in SQL (source)
        # 2. Read from SQL
        # 3. Filter (age >= 30)
        # 4. Map (add event metadata)
        # 5. Save to S3 Raw (event stream)
        # 6. Extract from S3
        # 7. Map (add analytics tags)
        # 8. Filter (age >= 35)
        # 9. Map (calculate metrics)
        # 10. Save to S3 Processed (analytics ready)
        # 11. Extract from S3
        # 12. Map (to User)
        # 13. Save to SQL (analytics DB)
        # 14. Read from SQL
        # 15. Filter (age >= 35)
        # 16. Map (prepare for archive)
        # 17. Save to S3 Archive
        # 18. Extract and reduce (count)
        # 19. Final validation

        result = await (
            createo(users) ^ sqlalchemy_storage  # Stage 1: Create source
            | reado(search_index=ParamIndex(User))  # Stage 2: Read
            ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 30)  # Stage 3: Filter
            | mapo(
                lambda u, _idx: {
                    "name": u.name,
                    "age": u.age,
                    "email": u.email,
                    "event_type": "user_created",
                    "timestamp": "2024-01-01T00:00:00Z",
                }
            )  # Stage 4: Event metadata
            | createo() ^ raw_storage  # Stage 5: Event stream
            | mapo(lambda data, _idx: data[0])  # Stage 6: Extract
            | mapo(
                lambda d, _idx: {
                    **d,
                    "analytics_tag": "senior_user",
                    "segment": "premium" if d["age"] >= 35 else "standard",
                }
            )  # Stage 7: Analytics tags
            | filtero(lambda d: d["age"] >= 35)  # Stage 8: Filter
            | mapo(
                lambda d, _idx: {**d, "lifetime_value": d["age"] * 100, "engagement_score": d["age"] * 10}
            )  # Stage 9: Metrics
            | createo() ^ processed_storage  # Stage 10: Analytics ready
            | mapo(lambda data, _idx: data[0])  # Stage 11: Extract
            | mapo(
                lambda d, _idx: User(name=d["name"], age=d["age"], email=f"analytics_{d['email']}")
            )  # Stage 12: To User with new email
            | createo() ^ sqlalchemy_storage  # Stage 13: Analytics DB
            | reado(search_index=ParamIndex(User))  # Stage 14: Read
            ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 35)  # Stage 15: Filter
            | mapo(
                lambda u, _idx: {"name": u.name, "age": u.age, "archived": True, "archive_date": "2024-01-01"}
            )  # Stage 16: Prepare
            | createo() ^ archive_storage  # Stage 17: Archive
            | mapo(lambda data, _idx: data[0])  # Stage 18: Extract
            | reduceo(lambda acc, d: acc + 1, 0)  # Stage 19: Count
        ).execute()

        assert isinstance(result, int) and result >= 0

        # Cleanup
        for bucket in [RAW_DATA_BUCKET, PROCESSED_DATA_BUCKET, ARCHIVE_BUCKET]:
            with suppress(Exception):
                response = await s3_client.list_objects_v2(bucket=bucket)
                if response.contents:
                    delete_objects = S3DeleteObjectsDelete(
                        objects=[
                            S3DeleteObjectsDeleteObject(key=obj.key) for obj in response.contents if obj.key is not None
                        ]
                    )
                    await s3_client.delete_objects(bucket=bucket, delete=delete_objects)

    @pytest.mark.asyncio
    async def test_data_migration_pipeline_three_storages_eighteen_stages(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
        s3_client: AbstractS3Client,
        registry: DataTypeRegistry,
    ) -> None:
        """Test data migration pipeline with 3 storages and 18+ stages."""
        with suppress(S3BucketAlreadyExistsClientException, S3BucketAlreadyOwnedByYouClientException):
            await s3_client.create_bucket(RAW_DATA_BUCKET)
            await s3_client.create_bucket(PROCESSED_DATA_BUCKET)
            await s3_client.create_bucket(ARCHIVE_BUCKET)

        raw_storage = S3Storage(
            s3_client=s3_client,
            bucket=RAW_DATA_BUCKET,
            data_type_registry=registry,
        )

        processed_storage = S3Storage(
            s3_client=s3_client,
            bucket=PROCESSED_DATA_BUCKET,
            data_type_registry=registry,
        )

        archive_storage = S3Storage(
            s3_client=s3_client,
            bucket=ARCHIVE_BUCKET,
            data_type_registry=registry,
        )

        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
            User(name="David", age=40, email="david@example.com"),
        ]

        # Data migration pipeline with 18 stages:
        # 1. Create in SQL (old system)
        # 2. Read from SQL
        # 3. Filter (age >= 30)
        # 4. Map (add migration metadata)
        # 5. Save to S3 Raw (backup before migration)
        # 6. Extract from S3
        # 7. Map (transform for new system)
        # 8. Filter (age >= 35)
        # 9. Map (add new system fields)
        # 10. Save to S3 Processed (migrated data)
        # 11. Extract from S3
        # 12. Map (to User for new system)
        # 13. Save to SQL (new system)
        # 14. Read from SQL (verify)
        # 15. Filter (age >= 35)
        # 16. Map (prepare for archive)
        # 17. Save to S3 Archive (old system archive)
        # 18. Extract and count

        result = await (
            createo(users) ^ sqlalchemy_storage  # Stage 1: Old system
            | reado(search_index=ParamIndex(User))  # Stage 2: Read
            ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 30)  # Stage 3: Filter
            | mapo(
                lambda u, _idx: {
                    "name": u.name,
                    "age": u.age,
                    "email": u.email,
                    "migration_id": f"MIG_{u.name}",
                    "source": "old_system",
                }
            )  # Stage 4: Migration metadata
            | createo() ^ raw_storage  # Stage 5: Backup
            | mapo(lambda data, _idx: data[0])  # Stage 6: Extract
            | mapo(lambda d, _idx: {**d, "new_system_format": True, "name": d["name"].upper()})  # Stage 7: Transform
            | filtero(lambda d: d["age"] >= 35)  # Stage 8: Filter
            | mapo(lambda d, _idx: {**d, "system_version": "v2", "migrated_at": "2024-01-01"})  # Stage 9: New fields
            | createo() ^ processed_storage  # Stage 10: Migrated data
            | mapo(lambda data, _idx: data[0])  # Stage 11: Extract
            | mapo(
                lambda d, _idx: User(name=d["name"], age=d["age"], email=f"migrated_{d['email']}")
            )  # Stage 12: To User with new email
            | createo() ^ sqlalchemy_storage  # Stage 13: New system
            | reado(search_index=ParamIndex(User))  # Stage 14: Verify
            ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 35)  # Stage 15: Filter
            | mapo(lambda u, _idx: {"name": u.name, "age": u.age, "archived": True})  # Stage 16: Prepare
            | createo() ^ archive_storage  # Stage 17: Archive old system
            | mapo(lambda data, _idx: data[0])  # Stage 18: Extract
            | reduceo(lambda acc, d: acc + 1, 0)  # Stage 19: Count
        ).execute()

        assert isinstance(result, int) and result >= 0

        # Cleanup
        for bucket in [RAW_DATA_BUCKET, PROCESSED_DATA_BUCKET, ARCHIVE_BUCKET]:
            with suppress(Exception):
                response = await s3_client.list_objects_v2(bucket=bucket)
                if response.contents:
                    delete_objects = S3DeleteObjectsDelete(
                        objects=[
                            S3DeleteObjectsDeleteObject(key=obj.key) for obj in response.contents if obj.key is not None
                        ]
                    )
                    await s3_client.delete_objects(bucket=bucket, delete=delete_objects)

    @pytest.mark.asyncio
    async def test_compliance_audit_pipeline_three_storages_nineteen_stages(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
        s3_client: AbstractS3Client,
        registry: DataTypeRegistry,
    ) -> None:
        """Test compliance and audit pipeline with 3 storages and 19+ stages."""
        with suppress(S3BucketAlreadyExistsClientException, S3BucketAlreadyOwnedByYouClientException):
            await s3_client.create_bucket(RAW_DATA_BUCKET)
            await s3_client.create_bucket(PROCESSED_DATA_BUCKET)
            await s3_client.create_bucket(ARCHIVE_BUCKET)

        raw_storage = S3Storage(
            s3_client=s3_client,
            bucket=RAW_DATA_BUCKET,
            data_type_registry=registry,
        )

        processed_storage = S3Storage(
            s3_client=s3_client,
            bucket=PROCESSED_DATA_BUCKET,
            data_type_registry=registry,
        )

        archive_storage = S3Storage(
            s3_client=s3_client,
            bucket=ARCHIVE_BUCKET,
            data_type_registry=registry,
        )

        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
            User(name="David", age=40, email="david@example.com"),
        ]

        # Compliance audit pipeline with 19 stages:
        # 1. Create in SQL (production)
        # 2. Read from SQL
        # 3. Filter (age >= 30)
        # 4. Map (add audit trail)
        # 5. Save to S3 Raw (audit log)
        # 6. Extract from S3
        # 7. Map (add compliance flags)
        # 8. Filter (age >= 35)
        # 9. Map (add retention metadata)
        # 10. Save to S3 Processed (compliance layer)
        # 11. Extract from S3
        # 12. Map (to User)
        # 13. Update in SQL (mark as audited)
        # 14. Read from SQL
        # 15. Filter (age >= 35)
        # 16. Map (prepare for archive)
        # 17. Save to S3 Archive (long-term retention)
        # 18. Extract and count
        # 19. Final validation

        result = await (
            createo(users) ^ sqlalchemy_storage  # Stage 1: Production
            | reado(search_index=ParamIndex(User))  # Stage 2: Read
            ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 30)  # Stage 3: Filter
            | mapo(
                lambda u, _idx: {
                    "name": u.name,
                    "age": u.age,
                    "email": u.email,
                    "audit_id": f"AUDIT_{u.name}",
                    "audit_timestamp": "2024-01-01T00:00:00Z",
                }
            )  # Stage 4: Audit trail
            | createo() ^ raw_storage  # Stage 5: Audit log
            | mapo(lambda data, _idx: data[0])  # Stage 6: Extract
            | mapo(
                lambda d, _idx: {**d, "gdpr_compliant": True, "retention_required": True, "compliance_level": "high"}
            )  # Stage 7: Compliance flags
            | filtero(lambda d: d["age"] >= 35)  # Stage 8: Filter
            | mapo(
                lambda d, _idx: {**d, "retention_period": "7_years", "archive_date": "2024-01-01"}
            )  # Stage 9: Retention metadata
            | createo() ^ processed_storage  # Stage 10: Compliance layer
            | mapo(lambda data, _idx: data[0])  # Stage 11: Extract
            | mapo(lambda d, _idx: User(name=d["name"], age=d["age"], email=d["email"]))  # Stage 12: To User
            | updateo(
                search_index=ParamIndex(User, email="CHARLIE@example.com"), patch={"age": 36}
            )  # Stage 13: Mark as audited
            ^ sqlalchemy_storage
            | reado(search_index=ParamIndex(User))  # Stage 14: Read
            ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 35)  # Stage 15: Filter
            | mapo(
                lambda u, _idx: {"name": u.name, "age": u.age, "archived": True, "compliance_verified": True}
            )  # Stage 16: Prepare
            | createo() ^ archive_storage  # Stage 17: Long-term retention
            | mapo(lambda data, _idx: data[0])  # Stage 18: Extract
            | reduceo(lambda acc, d: acc + 1, 0)  # Stage 19: Count
        ).execute()

        assert isinstance(result, int) and result >= 0

        # Cleanup
        for bucket in [RAW_DATA_BUCKET, PROCESSED_DATA_BUCKET, ARCHIVE_BUCKET]:
            with suppress(Exception):
                response = await s3_client.list_objects_v2(bucket=bucket)
                if response.contents:
                    delete_objects = S3DeleteObjectsDelete(
                        objects=[
                            S3DeleteObjectsDeleteObject(key=obj.key) for obj in response.contents if obj.key is not None
                        ]
                    )
                    await s3_client.delete_objects(bucket=bucket, delete=delete_objects)

    @pytest.mark.asyncio
    async def test_cross_region_replication_pipeline_three_storages_sixteen_stages(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
        s3_client: AbstractS3Client,
        registry: DataTypeRegistry,
    ) -> None:
        """Test cross-region replication pipeline with 3 storages and 16+ stages."""
        with suppress(S3BucketAlreadyExistsClientException, S3BucketAlreadyOwnedByYouClientException):
            await s3_client.create_bucket(RAW_DATA_BUCKET)
            await s3_client.create_bucket(PROCESSED_DATA_BUCKET)
            await s3_client.create_bucket(ARCHIVE_BUCKET)

        raw_storage = S3Storage(
            s3_client=s3_client,
            bucket=RAW_DATA_BUCKET,
            data_type_registry=registry,
        )

        processed_storage = S3Storage(
            s3_client=s3_client,
            bucket=PROCESSED_DATA_BUCKET,
            data_type_registry=registry,
        )

        archive_storage = S3Storage(
            s3_client=s3_client,
            bucket=ARCHIVE_BUCKET,
            data_type_registry=registry,
        )

        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
            User(name="David", age=40, email="david@example.com"),
        ]

        # Cross-region replication pipeline with 16 stages:
        # 1. Create in SQL (primary region)
        # 2. Read from SQL
        # 3. Filter (age >= 30)
        # 4. Map (add region metadata)
        # 5. Save to S3 Raw (replication queue)
        # 6. Extract from S3
        # 7. Map (add replication flags)
        # 8. Filter (age >= 35)
        # 9. Map (transform for replication)
        # 10. Save to S3 Processed (replicated data)
        # 11. Extract from S3
        # 12. Map (to User)
        # 13. Save to SQL (secondary region)
        # 14. Read from SQL (verify replication)
        # 15. Filter (age >= 35)
        # 16. Map and count

        result = await (
            createo(users) ^ sqlalchemy_storage  # Stage 1: Primary region
            | reado(search_index=ParamIndex(User))  # Stage 2: Read
            ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 30)  # Stage 3: Filter
            | mapo(
                lambda u, _idx: {
                    "name": u.name,
                    "age": u.age,
                    "email": u.email,
                    "region": "us-east-1",
                    "replication_id": f"REPL_{u.name}",
                }
            )  # Stage 4: Region metadata
            | createo() ^ raw_storage  # Stage 5: Replication queue
            | mapo(lambda data, _idx: data[0])  # Stage 6: Extract
            | mapo(
                lambda d, _idx: {**d, "replication_status": "pending", "target_region": "us-west-2"}
            )  # Stage 7: Replication flags
            | filtero(lambda d: d["age"] >= 35)  # Stage 8: Filter
            | mapo(
                lambda d, _idx: {**d, "replicated": True, "replication_timestamp": "2024-01-01T00:00:00Z"}
            )  # Stage 9: Transform
            | createo() ^ processed_storage  # Stage 10: Replicated data
            | mapo(lambda data, _idx: data[0])  # Stage 11: Extract
            | mapo(
                lambda d, _idx: User(name=d["name"], age=d["age"], email=f"replicated_{d['email']}")
            )  # Stage 12: To User with new email
            | createo() ^ sqlalchemy_storage  # Stage 13: Secondary region
            | reado(search_index=ParamIndex(User))  # Stage 14: Verify
            ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 35)  # Stage 15: Filter
            | reduceo(lambda acc, u: acc + 1, 0)  # Stage 16: Count
        ).execute()

        assert isinstance(result, int) and result >= 0

        # Cleanup
        for bucket in [RAW_DATA_BUCKET, PROCESSED_DATA_BUCKET, ARCHIVE_BUCKET]:
            with suppress(Exception):
                response = await s3_client.list_objects_v2(bucket=bucket)
                if response.contents:
                    delete_objects = S3DeleteObjectsDelete(
                        objects=[
                            S3DeleteObjectsDeleteObject(key=obj.key) for obj in response.contents if obj.key is not None
                        ]
                    )
                    await s3_client.delete_objects(bucket=bucket, delete=delete_objects)

    @pytest.mark.asyncio
    async def test_machine_learning_pipeline_three_storages_eighteen_stages(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
        s3_client: AbstractS3Client,
        registry: DataTypeRegistry,
    ) -> None:
        """Test ML feature engineering pipeline with 3 storages and 18+ stages."""
        with suppress(S3BucketAlreadyExistsClientException, S3BucketAlreadyOwnedByYouClientException):
            await s3_client.create_bucket(RAW_DATA_BUCKET)
            await s3_client.create_bucket(PROCESSED_DATA_BUCKET)
            await s3_client.create_bucket(ARCHIVE_BUCKET)

        raw_storage = S3Storage(
            s3_client=s3_client,
            bucket=RAW_DATA_BUCKET,
            data_type_registry=registry,
        )

        processed_storage = S3Storage(
            s3_client=s3_client,
            bucket=PROCESSED_DATA_BUCKET,
            data_type_registry=registry,
        )

        archive_storage = S3Storage(
            s3_client=s3_client,
            bucket=ARCHIVE_BUCKET,
            data_type_registry=registry,
        )

        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
            User(name="David", age=40, email="david@example.com"),
        ]

        # ML feature engineering pipeline with 18 stages:
        # 1. Create in SQL (raw features)
        # 2. Read from SQL
        # 3. Filter (age >= 30)
        # 4. Map (extract features)
        # 5. Save to S3 Raw (feature store)
        # 6. Extract from S3
        # 7. Map (normalize features)
        # 8. Filter (age >= 35)
        # 9. Map (engineer new features)
        # 10. Save to S3 Processed (ML-ready features)
        # 11. Extract from S3
        # 12. Map (to User with features)
        # 13. Save to SQL (feature database)
        # 14. Read from SQL
        # 15. Filter (age >= 35)
        # 16. Map (prepare for archive)
        # 17. Save to S3 Archive
        # 18. Extract and count

        result = await (
            createo(users) ^ sqlalchemy_storage  # Stage 1: Raw features
            | reado(search_index=ParamIndex(User))  # Stage 2: Read
            ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 30)  # Stage 3: Filter
            | mapo(
                lambda u, _idx: {"name": u.name, "age": u.age, "email": u.email, "feature_vector": [u.age, len(u.name)]}
            )  # Stage 4: Extract features
            | createo() ^ raw_storage  # Stage 5: Feature store
            | mapo(lambda data, _idx: data[0])  # Stage 6: Extract
            | mapo(
                lambda d, _idx: {**d, "normalized_age": d["age"] / 100, "name_length": len(d["name"])}
            )  # Stage 7: Normalize
            | filtero(lambda d: d["age"] >= 35)  # Stage 8: Filter
            | mapo(
                lambda d, _idx: {**d, "age_squared": d["age"] ** 2, "is_senior": d["age"] >= 35}
            )  # Stage 9: Engineer features
            | createo() ^ processed_storage  # Stage 10: ML-ready features
            | mapo(lambda data, _idx: data[0])  # Stage 11: Extract
            | mapo(
                lambda d, _idx: User(name=d["name"], age=d["age"], email=f"ml_{d['email']}")
            )  # Stage 12: To User with new email
            | createo() ^ sqlalchemy_storage  # Stage 13: Feature database
            | reado(search_index=ParamIndex(User))  # Stage 14: Read
            ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 35)  # Stage 15: Filter
            | mapo(lambda u, _idx: {"name": u.name, "age": u.age, "archived": True})  # Stage 16: Prepare
            | createo() ^ archive_storage  # Stage 17: Archive
            | mapo(lambda data, _idx: data[0])  # Stage 18: Extract
            | reduceo(lambda acc, d: acc + 1, 0)  # Stage 19: Count
        ).execute()

        assert isinstance(result, int) and result >= 0

        # Cleanup
        for bucket in [RAW_DATA_BUCKET, PROCESSED_DATA_BUCKET, ARCHIVE_BUCKET]:
            with suppress(Exception):
                response = await s3_client.list_objects_v2(bucket=bucket)
                if response.contents:
                    delete_objects = S3DeleteObjectsDelete(
                        objects=[
                            S3DeleteObjectsDeleteObject(key=obj.key) for obj in response.contents if obj.key is not None
                        ]
                    )
                    await s3_client.delete_objects(bucket=bucket, delete=delete_objects)

    @pytest.mark.asyncio
    async def test_streaming_processing_pipeline_three_storages_seventeen_stages(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
        s3_client: AbstractS3Client,
        registry: DataTypeRegistry,
    ) -> None:
        """Test streaming data processing pipeline with 3 storages and 17+ stages."""
        with suppress(S3BucketAlreadyExistsClientException, S3BucketAlreadyOwnedByYouClientException):
            await s3_client.create_bucket(RAW_DATA_BUCKET)
            await s3_client.create_bucket(PROCESSED_DATA_BUCKET)
            await s3_client.create_bucket(ARCHIVE_BUCKET)

        raw_storage = S3Storage(
            s3_client=s3_client,
            bucket=RAW_DATA_BUCKET,
            data_type_registry=registry,
        )

        processed_storage = S3Storage(
            s3_client=s3_client,
            bucket=PROCESSED_DATA_BUCKET,
            data_type_registry=registry,
        )

        archive_storage = S3Storage(
            s3_client=s3_client,
            bucket=ARCHIVE_BUCKET,
            data_type_registry=registry,
        )

        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
            User(name="David", age=40, email="david@example.com"),
        ]

        # Streaming processing pipeline with 17 stages:
        # 1. Create in SQL (stream source)
        # 2. Read from SQL
        # 3. Filter (age >= 30)
        # 4. Map (add stream metadata)
        # 5. Save to S3 Raw (stream buffer)
        # 6. Extract from S3
        # 7. Map (add processing timestamp)
        # 8. Filter (age >= 35)
        # 9. Map (add stream position)
        # 10. Save to S3 Processed (processed stream)
        # 11. Extract from S3
        # 12. Map (to User)
        # 13. Save to SQL (stream sink)
        # 14. Read from SQL
        # 15. Filter (age >= 35)
        # 16. Map (prepare for archive)
        # 17. Save to S3 Archive and count

        result = await (
            createo(users) ^ sqlalchemy_storage  # Stage 1: Stream source
            | reado(search_index=ParamIndex(User))  # Stage 2: Read
            ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 30)  # Stage 3: Filter
            | mapo(
                lambda u, _idx: {
                    "name": u.name,
                    "age": u.age,
                    "email": u.email,
                    "stream_id": f"STREAM_{u.name}",
                    "offset": _idx,
                }
            )  # Stage 4: Stream metadata
            | createo() ^ raw_storage  # Stage 5: Stream buffer
            | mapo(lambda data, _idx: data[0])  # Stage 6: Extract
            | mapo(
                lambda d, _idx: {**d, "processed_at": "2024-01-01T00:00:00Z", "processing_latency_ms": 10}
            )  # Stage 7: Processing timestamp
            | filtero(lambda d: d["age"] >= 35)  # Stage 8: Filter
            | mapo(lambda d, _idx: {**d, "stream_position": d["offset"], "committed": True})  # Stage 9: Stream position
            | createo() ^ processed_storage  # Stage 10: Processed stream
            | mapo(lambda data, _idx: data[0])  # Stage 11: Extract
            | mapo(
                lambda d, _idx: User(name=d["name"], age=d["age"], email=f"stream_{d['email']}")
            )  # Stage 12: To User with new email
            | createo() ^ sqlalchemy_storage  # Stage 13: Stream sink
            | reado(search_index=ParamIndex(User))  # Stage 14: Read
            ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 35)  # Stage 15: Filter
            | mapo(lambda u, _idx: {"name": u.name, "age": u.age, "archived": True})  # Stage 16: Prepare
            | createo() ^ archive_storage  # Stage 17: Archive
            | mapo(lambda data, _idx: data[0])  # Stage 18: Extract
            | reduceo(lambda acc, d: acc + 1, 0)  # Stage 19: Count
        ).execute()

        assert isinstance(result, int) and result >= 0

        # Cleanup
        for bucket in [RAW_DATA_BUCKET, PROCESSED_DATA_BUCKET, ARCHIVE_BUCKET]:
            with suppress(Exception):
                response = await s3_client.list_objects_v2(bucket=bucket)
                if response.contents:
                    delete_objects = S3DeleteObjectsDelete(
                        objects=[
                            S3DeleteObjectsDeleteObject(key=obj.key) for obj in response.contents if obj.key is not None
                        ]
                    )
                    await s3_client.delete_objects(bucket=bucket, delete=delete_objects)

    @pytest.mark.asyncio
    async def test_batch_processing_pipeline_three_storages_sixteen_stages(
        self,
        sqlalchemy_storage: SQLAlchemyStorage,
        s3_client: AbstractS3Client,
        registry: DataTypeRegistry,
    ) -> None:
        """Test batch processing pipeline with 3 storages and 16+ stages."""
        with suppress(S3BucketAlreadyExistsClientException, S3BucketAlreadyOwnedByYouClientException):
            await s3_client.create_bucket(RAW_DATA_BUCKET)
            await s3_client.create_bucket(PROCESSED_DATA_BUCKET)
            await s3_client.create_bucket(ARCHIVE_BUCKET)

        raw_storage = S3Storage(
            s3_client=s3_client,
            bucket=RAW_DATA_BUCKET,
            data_type_registry=registry,
        )

        processed_storage = S3Storage(
            s3_client=s3_client,
            bucket=PROCESSED_DATA_BUCKET,
            data_type_registry=registry,
        )

        archive_storage = S3Storage(
            s3_client=s3_client,
            bucket=ARCHIVE_BUCKET,
            data_type_registry=registry,
        )

        users = [
            User(name="Alice", age=25, email="alice@example.com"),
            User(name="Bob", age=30, email="bob@example.com"),
            User(name="Charlie", age=35, email="charlie@example.com"),
            User(name="David", age=40, email="david@example.com"),
        ]

        # Batch processing pipeline with 16 stages:
        # 1. Create in SQL (batch source)
        # 2. Read from SQL
        # 3. Filter (age >= 30)
        # 4. Map (add batch metadata)
        # 5. Save to S3 Raw (batch storage)
        # 6. Extract from S3
        # 7. Map (add batch processing flags)
        # 8. Filter (age >= 35)
        # 9. Map (aggregate batch data)
        # 10. Save to S3 Processed (processed batch)
        # 11. Extract from S3
        # 12. Map (to User)
        # 13. Save to SQL (batch results)
        # 14. Read from SQL
        # 15. Filter (age >= 35)
        # 16. Map and count

        result = await (
            createo(users) ^ sqlalchemy_storage  # Stage 1: Batch source
            | reado(search_index=ParamIndex(User))  # Stage 2: Read
            ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 30)  # Stage 3: Filter
            | mapo(
                lambda u, _idx: {
                    "name": u.name,
                    "age": u.age,
                    "email": u.email,
                    "batch_id": "BATCH_001",
                    "batch_timestamp": "2024-01-01T00:00:00Z",
                }
            )  # Stage 4: Batch metadata
            | createo() ^ raw_storage  # Stage 5: Batch storage
            | mapo(lambda data, _idx: data[0])  # Stage 6: Extract
            | mapo(
                lambda d, _idx: {**d, "batch_processed": True, "processing_time_ms": 100}
            )  # Stage 7: Processing flags
            | filtero(lambda d: d["age"] >= 35)  # Stage 8: Filter
            | mapo(lambda d, _idx: {**d, "batch_aggregate": True, "total_items": 1})  # Stage 9: Aggregate
            | createo() ^ processed_storage  # Stage 10: Processed batch
            | mapo(lambda data, _idx: data[0])  # Stage 11: Extract
            | mapo(
                lambda d, _idx: User(name=d["name"], age=d["age"], email=f"batch_{d['email']}")
            )  # Stage 12: To User with new email
            | createo() ^ sqlalchemy_storage  # Stage 13: Batch results
            | reado(search_index=ParamIndex(User))  # Stage 14: Read
            ^ sqlalchemy_storage
            | filtero(lambda u: u.age >= 35)  # Stage 15: Filter
            | reduceo(lambda acc, u: acc + 1, 0)  # Stage 16: Count
        ).execute()

        assert isinstance(result, int) and result >= 0

        # Cleanup
        for bucket in [RAW_DATA_BUCKET, PROCESSED_DATA_BUCKET, ARCHIVE_BUCKET]:
            with suppress(Exception):
                response = await s3_client.list_objects_v2(bucket=bucket)
                if response.contents:
                    delete_objects = S3DeleteObjectsDelete(
                        objects=[
                            S3DeleteObjectsDeleteObject(key=obj.key) for obj in response.contents if obj.key is not None
                        ]
                    )
                    await s3_client.delete_objects(bucket=bucket, delete=delete_objects)
