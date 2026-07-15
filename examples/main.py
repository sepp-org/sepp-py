"""End-to-end roundtrip: enqueue one job, then run a worker that processes it.

Requires a running sepp server. Point at it with ``SEPP_ADDR``
(default ``127.0.0.1:50051``)::

    uv run python examples/main.py
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import timedelta

from sepp import EnqueueRequest, JobCtx, Payload, SeppClient, Worker

QUEUE = "example"
JOB_TYPE = "greeting"


async def main() -> None:
    logging.basicConfig(level=logging.INFO)

    addr = os.environ.get("SEPP_ADDR", "127.0.0.1:50051")
    client = await SeppClient.connect(addr)

    # 1. Enqueue a single job.
    ack = await client.enqueue(
        EnqueueRequest(QUEUE, JOB_TYPE, payload=Payload(b"hello, sepp", "text/plain"))
    )
    print(f"enqueued job {ack.job_id}")

    # 2. Launch a worker. The handler signals the job id over a future so the
    #    example can finish instead of looping in `run` forever.
    done: asyncio.Future[str] = asyncio.get_running_loop().create_future()
    worker = Worker(client, [QUEUE], timedelta(seconds=30))

    @worker.handler(JOB_TYPE)
    async def greeting(payload: Payload | None, ctx: JobCtx) -> None:
        body = payload.data.decode() if payload else ""
        print(f"worker: processing job {ctx.id} — payload {body!r}")
        if not done.done():
            done.set_result(ctx.id)

    worker_task = asyncio.create_task(worker.run())

    # 3. Wait for the job to be processed, with a timeout, then shut down.
    #    shutdown() stops new reservations; awaiting `run` then drains what is
    #    in flight (including this job's ack) before returning.
    try:
        job_id = await asyncio.wait_for(asyncio.shield(done), timeout=15)
        print(f"roundtrip OK — job {job_id} was enqueued and processed")
    except asyncio.TimeoutError:
        print("timed out waiting for the job to be processed")
        raise SystemExit(1) from None
    finally:
        worker.shutdown_handle().shutdown()
        await worker_task
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
