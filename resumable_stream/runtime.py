"""
Resumable Stream Runtime

This module provides a Redis-backed resumable streaming system that allows clients to 
create, pause, and resume async streams with persistence. It supports multiple consumers
and handles connection failures gracefully.

The system uses Redis pub/sub for real-time communication and Redis keys for persistence.
Streams can be resumed from any point by specifying the number of characters to skip.

Key Features:
- Resumable async streams with Redis persistence
- Multiple consumer support via broadcasting
- Automatic cleanup on stream completion
- Timeout handling for connection issues
- Character-level resumption granularity

Example:
    import asyncio
    from redis.asyncio import Redis
    from resumable_stream.runtime import create_resumable_stream_context
    
    async def main():
        redis = Redis()
        ctx = create_resumable_stream_context(redis)
        
        async def sample_stream():
            for i in range(10):
                yield f"chunk {i}\n"
        
        stream = await ctx.resumable_stream("test-stream", sample_stream)
        if stream:
            async for chunk in stream:
                print(chunk, end='')
"""
import asyncio
import json
import uuid
from typing import (
    Protocol,
    Optional,
    Callable,
    Any,
    Awaitable,
    AsyncIterator,
    List,
    Union,
    cast,
)
from redis.asyncio import Redis

# Sentinel message sent to indicate stream completion
DONE_MESSAGE = "\n\n\nDONE_SENTINEL_hasdfasudfyge374%$%^$EDSATRTYFtydryrte\n"
# Value stored in Redis to indicate a stream is complete
DONE_VALUE = "DONE"


class StreamBroadcaster:
    """
    Broadcasts a single async stream to multiple consumers using queues.
    
    This class allows one source stream to be consumed by multiple clients
    simultaneously. Each consumer gets its own queue and receives all chunks
    from the source stream.
    
    Attributes:
        source: The source async iterator to broadcast from
        queues: List of queues for each consumer
    """
    
    def __init__(self, source: AsyncIterator[str]):
        """
        Initialize the broadcaster with a source stream.
        
        Args:
            source: The async iterator to broadcast from
        """
        self.source = source
        self.queues: List[asyncio.Queue[str | None]] = []

    def add_consumer(self) -> asyncio.Queue[str]:
        """
        Add a new consumer queue to receive broadcasted chunks.
        
        Returns:
            A new queue that will receive all chunks from the source stream
        """
        q: asyncio.Queue = asyncio.Queue()
        self.queues.append(q)
        return q

    async def start(self) -> None:
        """
        Start broadcasting the source stream to all consumer queues.
        
        This method consumes the source stream and puts each chunk into all
        consumer queues. When the source is exhausted, it sends None to all
        queues to signal completion.
        """
        async for chunk in self.source:
            for q in self.queues:
                await q.put(chunk)
        for q in self.queues:
            await q.put(None)  # Sentinel to close consumers

    @staticmethod
    async def queue_to_stream(queue: asyncio.Queue[str]) -> AsyncIterator[str]:
        """
        Convert a queue back into an async iterator stream.
        
        Args:
            queue: The queue to convert to a stream
            
        Yields:
            str: Chunks from the queue until None is received
        """
        while True:
            chunk = await queue.get()
            if chunk is None:
                break
            yield chunk

    @staticmethod
    async def iterate_bg(queue: asyncio.Queue[str]) -> None:
        """
        Background task to iterate through a queue for debugging purposes.
        
        Args:
            queue: The queue to iterate through
        """
        debug_log("Broadcaster started")
        while True:
            chunk = await queue.get()
            if chunk is None:
                break
            debug_log("Broadcaster received chunk", chunk)


class CreateResumableStreamContext:
    """
    Context object containing Redis connection and configuration for resumable streams.
    
    This class holds the shared state needed for all resumable stream operations,
    including the Redis connection and key prefix for namespacing.
    
    Attributes:
        key_prefix: Redis key prefix for this context
        redis: Redis connection instance
    """
    
    def __init__(
        self,
        key_prefix: str,
        redis: Redis,
    ):
        """
        Initialize the resumable stream context.
        
        Args:
            key_prefix: Prefix for Redis keys to namespace this context
            redis: Redis connection instance
        """
        self.key_prefix = key_prefix
        self.redis = redis


class ResumableStreamContext(Protocol):
    """
    Protocol interface for resumable stream context implementations.
    
    This protocol defines the public interface for creating and managing
    resumable streams. Implementations should provide methods for creating
    new streams, resuming existing ones, and handling both scenarios.
    """

    async def resumable_stream(
        self,
        stream_id: str,
        make_stream: Callable[[], AsyncIterator[str]],
        skip_characters: Optional[int] = None,
    ) -> AsyncIterator[str] | None:
        """
        Create a new resumable stream or resume an existing one.
        
        Args:
            stream_id: Unique identifier for the stream
            make_stream: Function that creates the source stream
            skip_characters: Number of characters to skip when resuming
            
        Returns:
            AsyncIterator[str] | None: The stream iterator or None if stream is done
        """
        ...

    async def resume_existing_stream(
        self,
        stream_id: str,
        skip_characters: Optional[int] = None,
    ) -> AsyncIterator[str] | None:
        """
        Resume an existing stream from where it left off.
        
        Args:
            stream_id: Unique identifier for the stream
            skip_characters: Number of characters to skip when resuming
            
        Returns:
            AsyncIterator[str] | None: The stream iterator or None if not found/done
        """
        ...

    async def create_new_resumable_stream(
        self,
        stream_id: str,
        make_stream: Callable[[], AsyncIterator[str]],
        skip_characters: Optional[int] = None,
        start: bool = False,
    ) -> AsyncIterator[str]:
        """
        Create a new resumable stream.
        
        Args:
            stream_id: Unique identifier for the stream
            make_stream: Function that creates the source stream
            skip_characters: Number of characters to skip (for consistency)
            start: Whether to immediately start the stream with broadcasting
            
        Returns:
            AsyncIterator[str]: The new stream iterator
        """
        ...


def create_resumable_stream_context(
    redis: Redis,
    key_prefix: str = "resumable-stream",
) -> ResumableStreamContext:
    """
    Factory function to create a resumable stream context.
    
    This function creates a context that can be used to create and manage
    resumable streams. The context handles Redis connections and provides
    a clean interface for stream operations.
    
    Args:
        redis: Redis connection instance
        key_prefix: Prefix for Redis keys (default: "resumable-stream")
        
    Returns:
        ResumableStreamContext: A context object for managing resumable streams
    """
    ctx = CreateResumableStreamContext(
        key_prefix=f"{key_prefix or 'resumable-stream'}:rs",
        redis=redis,
    )

    class ResumableStreamContextImpl:
        """Implementation of the ResumableStreamContext protocol."""
        
        async def resume_existing_stream(
            self,
            stream_id: str,
            skip_characters: Optional[int] = None,
        ) -> AsyncIterator[str] | None:
            """
            Resume an existing stream from where it left off.
            
            Args:
                stream_id: Unique identifier for the stream
                skip_characters: Number of characters to skip when resuming
                
            Returns:
                AsyncIterator[str] | None: The stream iterator or None if not found/done
            """
            state = await ctx.redis.get(f"{ctx.key_prefix}:sentinel:{stream_id}")
            if not state:
                return None
            if state == DONE_VALUE:
                return None
            return await resume_stream(ctx, stream_id, skip_characters)

        async def create_new_resumable_stream(
            self,
            stream_id: str,
            make_stream: Callable[[], AsyncIterator[str]],
            skip_characters: Optional[int] = None,
            start: bool = True,
        ) -> AsyncIterator[str]:
            """
            Create a new resumable stream.
            
            Args:
                stream_id: Unique identifier for the stream
                make_stream: Function that creates the source stream
                skip_characters: Number of characters to skip (for consistency)
                start: Whether to immediately start the stream with broadcasting. This starts the stream in the background.
                
            Returns:
                AsyncIterator[str]: The new stream iterator
            """
            await ctx.redis.set(
                f"{ctx.key_prefix}:sentinel:{stream_id}",
                "1",
                ex=24 * 60 * 60,
            )
            stream = await create_new_resumable_stream(
                ctx,
                stream_id,
                make_stream,
            )
            if not start:
                return stream

            debug_log("Starting broadcaster")
            broadcaster = StreamBroadcaster(stream)
            queue = broadcaster.add_consumer()
            asyncio.create_task(broadcaster.start())
            debug_log("Broadcaster started")
            return broadcaster.queue_to_stream(queue)

        async def resumable_stream(
            self,
            stream_id: str,
            make_stream: Callable[[], AsyncIterator[str]],
            skip_characters: Optional[int] = None,
        ) -> AsyncIterator[str] | None:
            """
            Create a new resumable stream or resume an existing one.
            
            Args:
                stream_id: Unique identifier for the stream
                make_stream: Function that creates the source stream
                skip_characters: Number of characters to skip when resuming
                
            Returns:
                AsyncIterator[str] | None: The stream iterator or None if stream is done
            """
            return await create_resumable_stream(
                ctx,
                stream_id,
                make_stream,
                skip_characters,
            )

    return ResumableStreamContextImpl()


async def resume_existing_stream(
    init_promise: Awaitable[Any],
    ctx: CreateResumableStreamContext,
    stream_id: str,
    skip_characters: Optional[int] = None,
) -> AsyncIterator[str] | None:
    """
    Resume an existing stream after waiting for initialization.
    
    This function waits for an initialization promise to complete before
    attempting to resume a stream. It checks if the stream exists and
    isn't already complete.
    
    Args:
        init_promise: Promise to wait for before resuming
        ctx: The resumable stream context
        stream_id: Unique identifier for the stream
        skip_characters: Number of characters to skip when resuming
        
    Returns:
        AsyncIterator[str] | None: The stream iterator or None if not found/done
    """
    await init_promise
    state = await ctx.redis.get(f"{ctx.key_prefix}:sentinel:{stream_id}")
    if not state:
        return None
    if state == DONE_VALUE:
        return None
    return await resume_stream(ctx, stream_id, skip_characters)


async def create_new_resumable_stream(
    ctx: CreateResumableStreamContext,
    stream_id: str,
    make_stream: Callable[[], AsyncIterator[str]],
) -> AsyncIterator[str]:
    """
    Create a new resumable stream that can be resumed later.
    
    This function creates a new stream that automatically stores chunks in memory
    and publishes them to Redis for resumption. It handles multiple listeners
    and manages cleanup when the stream completes.
    
    Args:
        ctx: The resumable stream context
        stream_id: Unique identifier for the stream
        make_stream: Function that creates the source stream
        
    Returns:
        AsyncIterator[str]: The new resumable stream iterator
        
    Raises:
        Exception: If Redis operations fail or stream creation errors occur
    """
    chunks: List[str] = []
    listener_channels: List[str] = []
    is_done = False
    pubsub = ctx.redis.pubsub()
    message_handler_task = None

    try:
        await pubsub.subscribe(f"{ctx.key_prefix}:request:{stream_id}")

        async def handle_message(message: str):
            """
            Handle incoming listener connection requests.
            
            Args:
                message: JSON message with listener ID and skip characters
            """
            parsed_message = json.loads(message)
            debug_log("Connected to listener", parsed_message["listenerId"])
            listener_channels.append(parsed_message["listenerId"])
            debug_log(
                "parsedMessage", len(chunks), parsed_message.get("skipCharacters")
            )
            chunks_to_send = "".join(chunks)[parsed_message.get("skipCharacters", 0) :]
            debug_log("sending chunks", len(chunks_to_send))
            promises = [
                ctx.redis.publish(
                    f"{ctx.key_prefix}:chunk:{parsed_message['listenerId']}",
                    chunks_to_send,
                )
            ]
            if is_done:
                promises.append(
                    ctx.redis.publish(
                        f"{ctx.key_prefix}:chunk:{parsed_message['listenerId']}",
                        DONE_MESSAGE,
                    )
                )
            await asyncio.gather(*promises)

        async def message_handler():
            """Background task to handle incoming messages."""
            try:
                async for message in pubsub.listen():
                    if message["type"] == "message":
                        await handle_message(message["data"])
            except asyncio.CancelledError:
                return

        message_handler_task = asyncio.create_task(message_handler())

        async def stream_generator():
            """
            Generate the stream while handling persistence and broadcasting.
            
            Yields:
                str: Chunks from the source stream
            """
            nonlocal is_done
            try:
                async for value in make_stream():
                    chunks.append(value)
                    debug_log("Enqueuing line", value)
                    yield value

                    promises = []
                    for listener_id in listener_channels:
                        debug_log("sending line to", listener_id)
                        promises.append(
                            ctx.redis.publish(
                                f"{ctx.key_prefix}:chunk:{listener_id}", value
                            )
                        )
                    await asyncio.gather(*promises)

                is_done = True
                debug_log("Stream done")
                promises = []
                debug_log("setting sentinel to done")
                promises.append(
                    ctx.redis.set(
                        f"{ctx.key_prefix}:sentinel:{stream_id}",
                        DONE_VALUE,
                        ex=24 * 60 * 60,
                    )
                )
                promises.append(
                    pubsub.unsubscribe(f"{ctx.key_prefix}:request:{stream_id}")
                )
                for listener_id in listener_channels:
                    debug_log("sending done message to", listener_id)
                    promises.append(
                        ctx.redis.publish(
                            f"{ctx.key_prefix}:chunk:{listener_id}", DONE_MESSAGE
                        )
                    )
                await asyncio.gather(*promises)
                debug_log("Cleanup done")
            except asyncio.CancelledError:
                # Clean up on cancellation
                if message_handler_task:
                    message_handler_task.cancel()
                await pubsub.unsubscribe(f"{ctx.key_prefix}:request:{stream_id}")
                raise

        return stream_generator()
    except Exception:
        # Clean up on any error
        if message_handler_task:
            message_handler_task.cancel()
        await pubsub.unsubscribe(f"{ctx.key_prefix}:request:{stream_id}")
        raise


async def resume_stream(
    ctx: CreateResumableStreamContext,
    stream_id: str,
    skip_characters: Optional[int] = None,
) -> AsyncIterator[str] | None:
    """
    Resume a stream from a specific point by connecting as a listener.
    
    This function connects to an existing stream by subscribing to its chunks
    and requesting to start from a specific character offset. It handles
    timeouts and connection issues gracefully.
    
    Args:
        ctx: The resumable stream context
        stream_id: Unique identifier for the stream
        skip_characters: Number of characters to skip when resuming
        
    Returns:
        AsyncIterator[str] | None: The resumed stream iterator
        
    Raises:
        TimeoutError: If the stream doesn't respond within the timeout period
        Exception: If Redis operations fail
    """
    listener_id = str(uuid.uuid4())
    pubsub = ctx.redis.pubsub()

    try:
        await pubsub.subscribe(f"{ctx.key_prefix}:chunk:{listener_id}")

        async def stream_generator():
            """
            Generate the resumed stream from Redis pub/sub messages.
            
            Yields:
                str: Chunks received from the stream
                
            Raises:
                TimeoutError: If timeout exceeded waiting for stream response
            """
            try:
                debug_log("STARTING STREAM", stream_id, listener_id)
                start = asyncio.get_event_loop().time()
                timeout_task = asyncio.create_task(asyncio.sleep(1.0))

                try:
                    async for message in pubsub.listen():
                        if message["type"] == "message":
                            debug_log("Received message", message["data"])
                            timeout_task.cancel()

                            if message["data"] == DONE_MESSAGE:
                                await pubsub.unsubscribe(
                                    f"{ctx.key_prefix}:chunk:{listener_id}"
                                )
                                return

                            yield message["data"]
                except asyncio.CancelledError:
                    val = await ctx.redis.get(f"{ctx.key_prefix}:sentinel:{stream_id}")
                    if val == DONE_VALUE:
                        return
                    if asyncio.get_event_loop().time() - start > 1.0:
                        raise TimeoutError("Timeout waiting for ack")
                finally:
                    await pubsub.unsubscribe(f"{ctx.key_prefix}:chunk:{listener_id}")
            except Exception as e:
                debug_log("Error in resume_stream", e)
                raise

        # Start the stream and send the request
        stream = stream_generator()
        await ctx.redis.publish(
            f"{ctx.key_prefix}:request:{stream_id}",
            json.dumps(
                {
                    "listenerId": listener_id,
                    "skipCharacters": skip_characters,
                }
            ),
        )

        return stream
    except Exception:
        await pubsub.unsubscribe(f"{ctx.key_prefix}:chunk:{listener_id}")
        raise


async def create_resumable_stream(
    ctx: CreateResumableStreamContext,
    stream_id: str,
    make_stream: Callable[[], AsyncIterator[str]],
    skip_characters: Optional[int] = None,
) -> AsyncIterator[str] | None:
    """
    Create a resumable stream or resume an existing one based on current state.
    
    This is the main entry point for stream operations. It checks if a stream
    already exists and decides whether to create a new one or resume an existing
    one based on the current listener count.
    
    Args:
        ctx: The resumable stream context
        stream_id: Unique identifier for the stream
        make_stream: Function that creates the source stream
        skip_characters: Number of characters to skip when resuming
        
    Returns:
        AsyncIterator[str] | None: The stream iterator or None if stream is done
    """
    current_listener_count = await incr_or_done(
        ctx.redis,
        f"{ctx.key_prefix}:sentinel:{stream_id}",
    )
    debug_log("currentListenerCount", current_listener_count)
    if current_listener_count == DONE_VALUE:
        return None
    if isinstance(current_listener_count, int) and current_listener_count > 1:
        return await resume_stream(ctx, stream_id, skip_characters)
    return await create_new_resumable_stream(ctx, stream_id, make_stream)


async def incr_or_done(publisher: Redis, key: str) -> Union[str, int]:
    """
    Increment a Redis key or return DONE_VALUE if the key contains a non-integer.
    
    This function attempts to increment a Redis key atomically. If the key
    contains a non-integer value (like DONE_VALUE), it returns that value
    instead of raising an exception.
    
    Args:
        publisher: Redis connection instance
        key: The Redis key to increment
        
    Returns:
        Union[str, int]: The incremented value or DONE_VALUE if not an integer
        
    Raises:
        Exception: If Redis operation fails for reasons other than type mismatch
    """
    try:
        return await publisher.incr(key)
    except Exception as reason:
        error_string = str(reason)
        if "ERR value is not an integer or out of range" in error_string:
            return DONE_VALUE
        raise


def debug_log(*messages: Any) -> None:
    """
    Log debug messages if DEBUG environment variable is set.
    
    This function provides conditional debug logging that can be enabled
    by setting the DEBUG environment variable. It prints all provided
    messages to stdout.
    
    Args:
        *messages: Variable number of messages to log
    """
    import os

    if os.getenv("DEBUG"):
        print(*messages)
