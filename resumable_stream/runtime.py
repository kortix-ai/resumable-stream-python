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

DONE_MESSAGE = "\n\n\nDONE_SENTINEL_hasdfasudfyge374%$%^$EDSATRTYFtydryrte\n"
DONE_VALUE = "DONE"


class CreateResumableStreamContext:
    def __init__(
        self,
        key_prefix: str,
        redis: Redis,
    ):
        self.key_prefix = key_prefix
        self.redis = redis


class ResumableStreamContext(Protocol):
    """Interface for resumable stream context."""

    async def resumable_stream(
        self,
        stream_id: str,
        make_stream: Callable[[], AsyncIterator[str]],
        skip_characters: Optional[int] = None,
    ) -> AsyncIterator[str] | None: ...

    async def resume_existing_stream(
        self,
        stream_id: str,
        skip_characters: Optional[int] = None,
    ) -> AsyncIterator[str] | None: ...

    async def create_new_resumable_stream(
        self,
        stream_id: str,
        make_stream: Callable[[], AsyncIterator[str]],
        skip_characters: Optional[int] = None,
    ) -> AsyncIterator[str]: ...


def create_resumable_stream_context(
    redis: Redis,
    key_prefix: str = "resumable-stream",
) -> ResumableStreamContext:
    ctx = CreateResumableStreamContext(
        key_prefix=f"{key_prefix or 'resumable-stream'}:rs",
        redis=redis,
    )

    class ResumableStreamContextImpl:
        async def resume_existing_stream(
            self,
            stream_id: str,
            skip_characters: Optional[int] = None,
        ) -> AsyncIterator[str] | None:
            state = await ctx.redis.get(f"{ctx.key_prefix}:sentinel:{stream_id}")
            if not state:
                return None
            if state.decode() == DONE_VALUE:
                return None
            return await resume_stream(ctx, stream_id, skip_characters)

        async def create_new_resumable_stream(
            self,
            stream_id: str,
            make_stream: Callable[[], AsyncIterator[str]],
            skip_characters: Optional[int] = None,
        ) -> AsyncIterator[str]:
            await ctx.redis.set(
                f"{ctx.key_prefix}:sentinel:{stream_id}",
                "1",
                ex=24 * 60 * 60,
            )
            return await create_new_resumable_stream(
                ctx,
                stream_id,
                make_stream,
            )

        async def resumable_stream(
            self,
            stream_id: str,
            make_stream: Callable[[], AsyncIterator[str]],
            skip_characters: Optional[int] = None,
        ) -> AsyncIterator[str] | None:
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
    chunks: List[str] = []
    listener_channels: List[str] = []
    is_done = False
    pubsub = ctx.redis.pubsub()
    message_handler_task = None

    try:
        await pubsub.subscribe(f"{ctx.key_prefix}:request:{stream_id}")

        async def handle_message(message: str):
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
            try:
                async for message in pubsub.listen():
                    if message["type"] == "message":
                        await handle_message(message["data"].decode())
            except asyncio.CancelledError:
                return

        message_handler_task = asyncio.create_task(message_handler())

        async def stream_generator():
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
    listener_id = str(uuid.uuid4())
    pubsub = ctx.redis.pubsub()

    try:
        await pubsub.subscribe(f"{ctx.key_prefix}:chunk:{listener_id}")

        async def stream_generator():
            try:
                debug_log("STARTING STREAM", stream_id, listener_id)
                start = asyncio.get_event_loop().time()
                timeout_task = asyncio.create_task(asyncio.sleep(1.0))

                try:
                    async for message in pubsub.listen():
                        if message["type"] == "message":
                            debug_log("Received message", message["data"].decode())
                            timeout_task.cancel()

                            if message["data"].decode() == DONE_MESSAGE:
                                await pubsub.unsubscribe(
                                    f"{ctx.key_prefix}:chunk:{listener_id}"
                                )
                                return

                            yield message["data"].decode()
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
    try:
        return await publisher.incr(key)
    except Exception as reason:
        error_string = str(reason)
        if "ERR value is not an integer or out of range" in error_string:
            return DONE_VALUE
        raise


def debug_log(*messages: Any) -> None:
    import os

    if os.getenv("DEBUG"):
        print(*messages)
