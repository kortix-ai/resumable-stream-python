# Resumable Stream Python

A Python implementation of resumable streams using Redis as the backend.

Heavily inspired by [vercel/resumable-stream](https://github.com/vercel/resumable-stream).

## Installation

### requirements.txt

Add the following line to your requirements.txt

```txt
resumable-stream @ git+https://github.com/kortix-ai/resumable-stream-python@v0.0.1
```

### Poetry

Add the following line to your pyproject.toml

```toml
resumable-stream = {git = "https://github.com/kortix-ai/resumable-stream-python", tag = "v0.0.1"}
```

## Usage

```python
import asyncio
from redis.asyncio import Redis
from resumable_stream.runtime import create_resumable_stream_context

async def main():
    redis = Redis(host='localhost', port=6379, db=0)
    context = create_resumable_stream_context(redis, key_prefix="my-stream")

    # Create a new stream
    stream = await context.create_new_resumable_stream("my-stream-id", lambda: my_stream_generator())

    # Or resume an existing stream
    stream = await context.resume_existing_stream("my-stream-id")

    # Read from the stream
    async for chunk in stream:
        print(chunk)

if __name__ == "__main__":
    asyncio.run(main())
```

## Development

### Requirements

- [uv](https://docs.astral.sh/uv/)
- Redis server running locally (you can do `docker compose up -d` in this repo)

### Setup

```bash
docker compose up -d # start redis server
uv sync # install dependencies
source .venv/bin/activate # activate virtual environment
python -m pytest # run tests
```
