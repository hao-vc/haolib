<div align="center">

# **haolib**

### *Build better. Build faster. Build SOTA.*

A Python meta-framework for building observable, durable, and flexible applications.

[**Documentation**](https://lib.hao.vc) · [**Installation**](https://lib.hao.vc/essentials/installation) · [**Philosophy**](https://lib.hao.vc/essentials/philosophy)

---

</div>

## Quick Start

```bash
uv add haolib
```

```python
from haolib.entrypoints import HAOrchestrator
from haolib.entrypoints.fastapi import FastAPIEntrypoint
from fastapi import FastAPI

app = FastAPI()
entrypoint = FastAPIEntrypoint(app=app)

hao = HAOrchestrator().add_entrypoint(entrypoint)
await hao.run_entrypoints()
```

## Principles

- **User Experience** — Extremely convenient to use
- **Customizability** — Fully extensible and declarative
- **Meta-framework** — Works with any framework
- **SOTA** — State-of-the-art toolkit
- **Implementation-agnostic** — Replace any component
- **All-in-one** — Single package, optional dependencies
- **9/10** — Almost perfect. Almost.

## Features

**Entrypoints** · FastAPI · FastStream · TaskIQ · FastMCP
**Storages** · SQLAlchemy · S3 · Redis
**Pipelines** · Composable data operations
**Security** · JWT · Encryption · StackAuth
**Observability** · OpenTelemetry · Logging · Metrics
**Background** · Fair task queues · Scheduling

---

<div align="center">

**[Full Documentation →](https://lib.hao.vc)**

*MIT OR Apache-2.0*

</div>
