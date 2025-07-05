import asyncio
import pytest
import pytest_asyncio
from redis.asyncio import Redis
from resumable_stream import (
    create_resumable_stream_context,
    ResumableStreamContext,
)
from typing import AsyncGenerator, List, Any


@pytest_asyncio.fixture
async def redis() -> AsyncGenerator[Redis, None]:
    redis = Redis(host="localhost", port=6379, db=0, decode_responses=True)
    yield redis
    keys = redis.scan_iter("test-resumable-stream:*")
    async for key in keys:
        await redis.delete(key)
    await redis.aclose()


@pytest_asyncio.fixture
async def stream_context(redis: Redis) -> ResumableStreamContext:
    return create_resumable_stream_context(redis, "test-resumable-stream")


async def async_generator(items: List[str]) -> AsyncGenerator[str, None]:
    for item in items:
        await asyncio.sleep(0.1)
        yield item


@pytest.mark.asyncio
@pytest.mark.timeout(1)
async def test_create_new_stream(stream_context: ResumableStreamContext) -> None:
    stream_id = "test-stream-1"
    test_data = ["chunk1", "chunk2", "chunk3"]

    # Create a new stream
    stream = await stream_context.create_new_resumable_stream(
        stream_id, lambda: async_generator(test_data)
    )

    # Collect all chunks
    received_chunks = []
    async for chunk in stream:
        received_chunks.append(chunk)

    assert "".join(received_chunks) == "".join(test_data)


@pytest.mark.asyncio
@pytest.mark.timeout(1)
async def test_resume_existing_stream(stream_context: ResumableStreamContext) -> None:
    stream_id = "test-stream-2"
    test_data = ["chunk1", "chunk2", "chunk3"]

    # Create initial stream
    stream = await stream_context.create_new_resumable_stream(
        stream_id, lambda: async_generator(test_data)
    )

    # Consume the stream in background
    async def consume_stream():
        async for _ in stream:
            pass

    asyncio.create_task(consume_stream())

    # Resume the stream
    resumed_stream = await stream_context.resume_existing_stream(stream_id)
    assert resumed_stream is not None

    # Collect remaining chunks
    received_chunks = []
    async for chunk in resumed_stream:
        received_chunks.append(chunk)

    assert "".join(received_chunks) == "".join(test_data)


@pytest.mark.asyncio
@pytest.mark.timeout(1)
async def test_resume_done_stream(stream_context: ResumableStreamContext) -> None:
    stream_id = "test-stream-3"
    test_data = ["chunk1\n", "chunk2\n"]

    # Create and complete first stream
    stream1 = await stream_context.create_new_resumable_stream(
        stream_id, lambda: async_generator(test_data)
    )
    received_chunks1 = []
    async for chunk in stream1:
        received_chunks1.append(chunk)

    # Create second stream
    stream2 = await stream_context.create_new_resumable_stream(
        stream_id, lambda: async_generator(test_data)
    )
    received_chunks2 = []
    async for chunk in stream2:
        received_chunks2.append(chunk)

    assert "".join(received_chunks1) == "".join(test_data)
    assert "".join(received_chunks2) == "".join(test_data)


@pytest.mark.asyncio
@pytest.mark.timeout(1)
async def test_resume_in_progress_stream(
    stream_context: ResumableStreamContext,
) -> None:
    stream_id = "test-stream-4"
    test_data = ["chunk1\n", "chunk2\n"]

    # Create first stream
    stream1 = await stream_context.create_new_resumable_stream(
        stream_id, lambda: async_generator(test_data)
    )

    # Start consuming first chunk
    received_chunks1 = []
    async for chunk in stream1:
        received_chunks1.append(chunk)
        if len(received_chunks1) == 1:
            break

    # Create second stream while first is still in progress
    stream2 = await stream_context.create_new_resumable_stream(
        stream_id, lambda: async_generator(test_data)
    )
    received_chunks2 = []
    async for chunk in stream2:
        received_chunks2.append(chunk)

    # Complete first stream
    async for chunk in stream1:
        received_chunks1.append(chunk)

    assert "".join(received_chunks1) == "".join(test_data)
    assert "".join(received_chunks2) == "".join(test_data)


@pytest.mark.asyncio
@pytest.mark.timeout(1)
async def test_multiple_streams(stream_context: ResumableStreamContext) -> None:
    stream_id1 = "test-stream-5"
    stream_id2 = "test-stream-6"
    test_data1 = ["chunk1\n", "chunk2\n"]
    test_data2 = ["chunk3\n", "chunk4\n"]

    # Create streams for different IDs
    stream1 = await stream_context.create_new_resumable_stream(
        stream_id1, lambda: async_generator(test_data1)
    )
    stream2 = await stream_context.create_new_resumable_stream(
        stream_id2, lambda: async_generator(test_data2)
    )

    # Resume both streams
    resumed_stream1 = await stream_context.resume_existing_stream(stream_id1)
    resumed_stream2 = await stream_context.resume_existing_stream(stream_id2)

    # Collect chunks from all streams
    received_chunks1 = []
    received_chunks2 = []
    received_chunks_resumed1 = []
    received_chunks_resumed2 = []

    async for chunk in stream1:
        received_chunks1.append(chunk)
    async for chunk in stream2:
        received_chunks2.append(chunk)

    if resumed_stream1 is not None:
        async for chunk in resumed_stream1:
            received_chunks_resumed1.append(chunk)
    if resumed_stream2 is not None:
        async for chunk in resumed_stream2:
            received_chunks_resumed2.append(chunk)

    assert "".join(received_chunks1) == "".join(test_data1)
    assert "".join(received_chunks2) == "".join(test_data2)
    assert "".join(received_chunks_resumed1) == "".join(test_data1)
    assert "".join(received_chunks_resumed2) == "".join(test_data2)


@pytest.mark.asyncio
@pytest.mark.timeout(1)
async def test_done_stream_returns_none(stream_context: ResumableStreamContext) -> None:
    stream_id = "test-stream-7"
    test_data = ["chunk1\n", "chunk2\n"]

    # Create and complete stream
    stream = await stream_context.create_new_resumable_stream(
        stream_id, lambda: async_generator(test_data)
    )
    received_chunks = []
    async for chunk in stream:
        received_chunks.append(chunk)

    # Try to resume completed stream
    resumed_stream = await stream_context.resume_existing_stream(stream_id)

    if resumed_stream:
        async for chunk in resumed_stream:
            print("resumed chunk", chunk)

    assert resumed_stream is None
    assert "".join(received_chunks) == "".join(test_data)
