import asyncio
import pytest
import pytest_asyncio
from redis.asyncio import Redis
from resumable_stream import (
    create_resumable_stream_context,
    ResumableStreamContext,
)
from resumable_stream.runtime import incr_or_done, DONE_VALUE
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
async def test_incr_or_done_new_key(redis: Redis) -> None:
    """Test incr_or_done with a new key that doesn't exist."""
    key = "test-incr-new"
    
    # Ensure key doesn't exist
    await redis.delete(key)
    
    result = await incr_or_done(redis, key)
    assert result == 1
    
    # Verify the key was actually set
    value = await redis.get(key)
    assert value == "1"
    
    # Clean up
    await redis.delete(key)


@pytest.mark.asyncio
async def test_incr_or_done_existing_integer(redis: Redis) -> None:
    """Test incr_or_done with an existing integer key."""
    key = "test-incr-existing"
    
    # Set initial value
    await redis.set(key, "5")
    
    result = await incr_or_done(redis, key)
    assert result == 6
    
    # Verify the key was incremented
    value = await redis.get(key)
    assert value == "6"
    
    # Test incrementing again
    result = await incr_or_done(redis, key)
    assert result == 7
    
    # Clean up
    await redis.delete(key)


@pytest.mark.asyncio
async def test_incr_or_done_with_done_value(redis: Redis) -> None:
    """Test incr_or_done with a key containing DONE_VALUE."""
    key = "test-incr-done"
    
    # Set key to DONE_VALUE
    await redis.set(key, DONE_VALUE)
    
    result = await incr_or_done(redis, key)
    assert result == DONE_VALUE
    
    # Verify the key value is unchanged
    value = await redis.get(key)
    assert value == DONE_VALUE
    
    # Clean up
    await redis.delete(key)


@pytest.mark.asyncio
async def test_incr_or_done_with_non_integer_string(redis: Redis) -> None:
    """Test incr_or_done with a key containing a non-integer string."""
    key = "test-incr-string"
    
    # Set key to a non-integer string
    await redis.set(key, "not-a-number")
    
    result = await incr_or_done(redis, key)
    assert result == DONE_VALUE
    
    # Verify the key value is unchanged
    value = await redis.get(key)
    assert value == "not-a-number"
    
    # Clean up
    await redis.delete(key)


@pytest.mark.asyncio
async def test_incr_or_done_multiple_increments(redis: Redis) -> None:
    """Test multiple increments to verify the function works consistently."""
    key = "test-incr-multiple"
    
    # Clean start
    await redis.delete(key)
    
    # First increment (key doesn't exist)
    result1 = await incr_or_done(redis, key)
    assert result1 == 1
    
    # Second increment 
    result2 = await incr_or_done(redis, key)
    assert result2 == 2
    
    # Third increment
    result3 = await incr_or_done(redis, key)
    assert result3 == 3
    
    # Now set it to DONE and verify behavior changes
    await redis.set(key, DONE_VALUE)
    result4 = await incr_or_done(redis, key)
    assert result4 == DONE_VALUE
    
    # Clean up
    await redis.delete(key)


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


@pytest.mark.asyncio
@pytest.mark.timeout(1)
async def test_create_new_stream_with_start(
    stream_context: ResumableStreamContext,
) -> None:
    stream_id = "test-stream-9"
    test_data = ["chunk1", "chunk2", "chunk3"]

    # Create a new stream
    stream = await stream_context.resumable_stream(
        stream_id, lambda: async_generator(test_data), start=True
    )

    if stream is None:
        raise Exception("Stream is None")

    # Collect all chunks
    received_chunks = []
    async for chunk in stream:
        received_chunks.append(chunk)

    assert "".join(received_chunks) == "".join(test_data)


@pytest.mark.asyncio
@pytest.mark.timeout(3)
async def test_resume_existing_stream_with_start(
    stream_context: ResumableStreamContext,
) -> None:
    stream_id = "test-stream-10"
    test_data = ["chunk1", "chunk2", "chunk3"]

    # Create initial stream
    _ = await stream_context.resumable_stream(
        stream_id, lambda: async_generator(test_data), start=True
    )

    # Resume the stream
    resumed_stream = await stream_context.resume_existing_stream(stream_id)
    assert resumed_stream is not None

    # Collect remaining chunks
    received_chunks = []
    async for chunk in resumed_stream:
        received_chunks.append(chunk)

    assert "".join(received_chunks) == "".join(test_data)


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_timeout_and_connection_closure(stream_context: ResumableStreamContext, redis: Redis) -> None:
    """Test that pubsub connections are properly cleaned up when timeout occurs during stream resumption."""
    
    stream_id = "test-timeout-stream"
    
    # Set up a stream state that exists but has no active publisher
    # This simulates a scenario where a stream was created but the publisher died
    await redis.set(f"test-resumable-stream:rs:sentinel:{stream_id}", "2", ex=24*60*60)
    
    # Try to resume the stream - this should timeout because no publisher is responding
    # The internal timeout in resume_stream is 1 second
    with pytest.raises(TimeoutError, match="Timeout waiting for ack"):
        resumed_stream = await stream_context.resume_existing_stream(stream_id)
        if resumed_stream:
            # Try to consume the stream - this should trigger the timeout
            chunks = []
            async for chunk in resumed_stream:
                chunks.append(chunk)
    
    # After the timeout, verify that the Redis state is still intact
    # (timeout shouldn't corrupt the stream state)
    state = await redis.get(f"test-resumable-stream:rs:sentinel:{stream_id}")
    assert state == "2"  # Should still be "2", not "DONE"
    
    # Verify that no pubsub channels are leaked by checking active channels
    # This tests the resource cleanup aspect
    pubsub_channels = await redis.execute_command("PUBSUB", "CHANNELS", "test-resumable-stream:rs:*")
    
    # There should be no active channels for our test stream after timeout cleanup
    if pubsub_channels:
        timeout_related_channels = [ch for ch in pubsub_channels if stream_id in str(ch)]
        assert len(timeout_related_channels) == 0, f"Found leaked channels: {timeout_related_channels}"
    
    # Clean up
    await redis.delete(f"test-resumable-stream:rs:sentinel:{stream_id}")
