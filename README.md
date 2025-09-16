# HAOlib

[![Python](https://img.shields.io/badge/python-3.13+-blue.svg)](https://www.python.org/downloads/)

A comprehensive Python utility library designed for building robust backend applications in the HAO ecosystem. HAOlib provides a rich set of tools and patterns for FastAPI applications, including dependency injection, configuration management, observability, exception handling, and more.

## Features

- 🚀 **Application Builder** - Fluent API for setting up FastAPI applications with all necessary components
- ⚙️ **Configuration Management** - Pydantic-based configuration with environment variable support
- 🔒 **Authentication & Security** - JWT services, password hashing, and encryption utilities
- 🚨 **Exception Handling** - Rich exception hierarchy with logging and structured error responses
- 📊 **Observability** - OpenTelemetry integration for distributed tracing and monitoring
- 🔄 **Idempotency** - Built-in idempotency middleware with Redis backing
- 🧩 **Specification Pattern** - Flexible filtering and querying with specification objects
- 🗄️ **Database Models** - Enhanced SQLAlchemy base models with serialization capabilities
- 🛠️ **Utilities** - UUID generation, password utilities, HTTP scheme detection, and more

## Installation

### Using uv

```bash
uv add git+https://github.com/hao-vc/haolib
```

## Quick Start

Here's a minimal example of setting up a FastAPI application with HAOlib:

```python
from dishka import AsyncContainer
from fastapi import FastAPI
from haolib.app import AppBuilder
from haolib.configs.server import ServerConfig
from haolib.configs.observability import ObservabilityConfig

# Create FastAPI app and dependency container
app = FastAPI(title="My HAO Application")
container = AsyncContainer()

# Build the application with HAOlib
builder = AppBuilder(container, app)

async def setup_app():
    """Setup the application with all necessary components."""
    final_app = await (
        builder
        .setup_observability(ObservabilityConfig.from_env())
        .setup_exception_handlers()
        .setup_cors_middleware()
        .setup_idempotency_middleware()
        .get_app()
    )
    
    # Get configured server
    server_config = ServerConfig.from_env()
    server = await builder.get_server(server_config)
    
    return server

# Run the application
if __name__ == "__main__":
    import asyncio
    
    server = asyncio.run(setup_app())
    server.run()
```

## Core Components

### 1. Application Builder

The `AppBuilder` provides a fluent interface for configuring FastAPI applications:

```python
from haolib.app import AppBuilder
from haolib.configs.base import BaseConfig
from dishka import AsyncContainer
from fastapi import FastAPI, APIRouter

class MyAppConfig(BaseConfig):
    observability: ObservabilityConfig
    
config = MyAppConfig.from_env()

app = FastAPI()
container = AsyncContainer()
router = APIRouter()

builder = AppBuilder(container, app)

# Chain configuration methods
configured_app = await (
    builder
    .setup_observability(config.observability)
    .setup_exception_handlers()
    .setup_cors_middleware()
    .setup_idempotency_middleware()
    .setup_router(router)
    .get_app()
)
```

### 2. Configuration Management

HAOlib uses Pydantic for configuration with automatic environment variable binding:

```python
from haolib.configs.base import BaseConfig
from haolib.configs.sqlalchemy import SQLAlchemyConfig

class MyAppConfig(BaseConfig):
    database: SQLAlchemyConfig
    api_key: str

# Automatically loads from environment variables
config = MyAppConfig.from_env()
```

Environment variables are mapped using double underscores for nested values:

- `DATABASE__URL` → `database.url`
- `API_KEY` → `api_key`

### 3. Exception Handling

Rich exception hierarchy with automatic logging and structured responses:

```python
from haolib.exceptions.base import NotFoundException, BadRequestException

# Custom exceptions
class UserNotFound(NotFoundException):
    detail = "User with ID {user_id} not found"
    additional_info = {"additional_info": "here it goes"}


# Usage with automatic formatting
raise UserNotFound(user_id=123)
```

If the exception handler is set in the `AppBuilder` and the exception raised from any FastAPI route, the exception will be handled by the exception handler.

Error scheme that will be returned to the client:

```json
{
    "error_code": "USER_NOT_FOUND",
    "detail": "User with ID 123 not found",
    "additional_info": {"additional_info": "here it goes"}
}
```

### 4. JWT Service

Secure JWT token handling with configurable algorithms:

```python
from haolib.services.jwt import JWTService
from haolib.configs.jwt import JWTConfig
from pydantic import BaseModel

class UserData(BaseModel):
    user_id: int
    username: str

jwt_service = JWTService(JWTConfig(secret_key="your-secret", algorithm="HS256"))

# Encode token
user_data = UserData(user_id=1, username="john")
token = jwt_service.encode(context_data=user_data, expires_in=60)  # 60 minutes

# Decode token
decoded_data = jwt_service.decode(token, UserData)
```

### 5. Password Security

Secure password hashing and verification using bcrypt:

```python
from haolib.utils.hash_password import hash_password, verify_password

# Hash a password
hashed = hash_password("user_password")

# Verify password
is_valid = verify_password("user_password", hashed)  # True
```

### 6. Specification Pattern

Flexible object filtering with composable specifications:

```python
from haolib.specification.base import EqualsSpecification, AndSpecification

# Create specifications
name_spec = EqualsSpecification("name", "John")
age_spec = EqualsSpecification("age", 25)

# Combine specifications
combined_spec = AndSpecification(name_spec, age_spec)

# Apply to objects
user = {"name": "John", "age": 25}
matches = combined_spec.is_satisfied_by(user)  # True
```

Also you can use the `SQLSpecification` to filter SQLAlchemy models:

```python
from haolib.specification.sqlalchemy import add_specifications_to_query
from haolib.models.base import AbstractModel
from haolib.specification.base import EqualsSpecification

from sqlalchemy import Integer, String, select 
from sqlalchemy.orm import Mapped, mapped_column

class UserModel(AbstractModel):
    __tablename__ = "users"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    email: Mapped[str] = mapped_column(String)

spec = EqualsSpecification("name", "John")
query = select(UserModel)
query = add_specifications_to_query(query, UserModel, [spec])
```

### 7. Database Models

Enhanced SQLAlchemy models with automatic serialization:

```python
from haolib.models.base import AbstractModel

from pydantic import BaseModel
from sqlalchemy import Integer, String
from sqlalchemy.orm import Mapped, mapped_column


class UserModel(AbstractModel):
    __tablename__ = "users"
    
    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String)
    email: Mapped[str] = mapped_column(String)

class UserSchema(BaseModel):
    name: str
    email: str

# Create from Pydantic model
user_schema = UserSchema(name="John", email="john@example.com")
user_model = UserModel.from_schema(user_schema)

# Convert to dictionary
user_dict = user_model.to_dict()
```

### 8. Idempotency Middleware

Prevent duplicate operations with Redis-backed idempotency:

```python
from haolib.middlewares.idempotency import IdempotencyKeysStorage
from redis.asyncio import Redis

# Setup idempotency storage
redis = Redis.from_url("redis://localhost:6379")
storage = IdempotencyKeysStorage(redis, ttl=300000)  # 5 minutes TTL

# Middleware is automatically configured with AppBuilder
builder.setup_idempotency_middleware()
```

Clients can include `Idempotency-Key` header to ensure operations are performed only once:

```bash
curl -X POST /api/orders \
  -H "Idempotency-Key: unique-operation-id" \
  -H "Content-Type: application/json" \
  -d '{"product_id": 123, "quantity": 1}'
```

## Advanced Usage

## API Reference

### Core Modules

- **`haolib.app`** - Application builder and setup utilities
- **`haolib.configs`** - Configuration management classes
- **`haolib.exceptions`** - Exception hierarchy and handlers
- **`haolib.models`** - Database model base classes and mixins
- **`haolib.services`** - JWT, encryption, and other services
- **`haolib.middlewares`** - Request/response middlewares
- **`haolib.specification`** - Specification pattern implementation
- **`haolib.enums`** - Utility enums
- **`haolib.observability`** - OpenTelemetry and monitoring setup
- **`haolib.utils`** - Utility functions

### Configuration Classes

| Class | Purpose | Environment Prefix |
|-------|---------|-------------------|
| `BaseConfig` | Base configuration with .env support | - |
| `ServerConfig` | Server host/port configuration | `SERVER__` |
| `ObservabilityConfig` | OpenTelemetry configuration | `OBSERVABILITY__` |
| `RedisConfig` | Redis connection configuration | `REDIS__` |
| `SQLAlchemyConfig` | Database configuration | `SQLALCHEMY__` |
| `IdempotencyConfig` | Idempotency configuration | `IDEMPOTENCY__` |
| `JWTConfig` | JWT configuration | `JWT__` |

### Exception Classes

| Class | HTTP Status | Use Case |
|-------|-------------|----------|
| `BadRequestException` | 400 | Invalid request data |
| `UnauthorizedException` | 401 | Authentication required |
| `ForbiddenException` | 403 | Access denied |
| `NotFoundException` | 404 | Resource not found |
| `MethodNotAllowedException` | 405 | Method not allowed |
| `ConflictException` | 409 | Resource conflict |
| `IdempotentRequest` | 409 | Idempotent request |
| `UnprocessableEntityException` | 422 | Validation errors |
| `TooManyRequestsException` | 429 | Rate limiting |
| `GoneException` | 410 | Resource gone |
| `InternalServerErrorException` | 500 | Server errors |
| `NotImplementedException` | 501 | Not implemented |
| `ServiceUnavailableException` | 503 | Service unavailable |

## Contributing

We welcome contributions! Please see [CONTRIBUTING.md](CONTRIBUTING.md) for guidelines.

### Development Setup

1. Clone the repository:

   ```bash
   git clone https://github.com/hao-vc/haolib.git
   cd haolib
   ```

2. Install dependencies with uv:

   ```bash
   uv sync
   ```

3. Run linting and formatting:

   ```bash
   uv run ruff check .
   uv run ruff format .
   ```

## Changelog

See [CHANGELOG.md](CHANGELOG.md) for release notes and migration guides.

## Support

For questions, issues, or contributions, please visit our [GitHub repository](https://github.com/hao-vc/haolib) or contact the HAO team.
