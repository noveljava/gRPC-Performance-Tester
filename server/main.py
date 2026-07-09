import argparse
import asyncio
import logging
import random

import grpc

from proto import performance_pb2, performance_pb2_grpc


MIN_DELAY_MS = 40
MAX_DELAY_MS = 200


class PerformanceTestService(performance_pb2_grpc.PerformanceTestServicer):
    def __init__(self, concurrency: int):
        self._semaphore = asyncio.Semaphore(concurrency)

    async def Process(self, request_iterator, context):
        responses = asyncio.Queue()
        tasks = set()

        async def process(request):
            async with self._semaphore:
                delay_ms = random.randint(MIN_DELAY_MS, MAX_DELAY_MS)
                await asyncio.sleep(delay_ms / 1_000)
                logging.info(
                    "processed id=%d correlation_id=%d delay=%dms",
                    request.id,
                    request.correlation_id,
                    delay_ms,
                )
                await responses.put(
                    performance_pb2.TestResponse(
                        id=request.id,
                        text=request.text,
                        correlation_id=request.correlation_id,
                    )
                )

        async def receive_requests():
            try:
                async for request in request_iterator:
                    task = asyncio.create_task(process(request))
                    tasks.add(task)
                    task.add_done_callback(tasks.discard)
            finally:
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                await responses.put(None)

        receiver = asyncio.create_task(receive_requests())
        try:
            while True:
                response = await responses.get()
                if response is None:
                    break
                yield response
        finally:
            if not receiver.done():
                receiver.cancel()
            await asyncio.gather(receiver, return_exceptions=True)


async def serve(port: int, concurrency: int) -> None:
    server = grpc.aio.server()
    performance_pb2_grpc.add_PerformanceTestServicer_to_server(
        PerformanceTestService(concurrency),
        server,
    )

    address = f"[::]:{port}"
    server.add_insecure_port(address)
    await server.start()
    logging.info(
        "async streaming gRPC server started on %s "
        "(concurrency=%d, delay=%d-%dms)",
        address,
        concurrency,
        MIN_DELAY_MS,
        MAX_DELAY_MS,
    )

    try:
        await server.wait_for_termination()
    except asyncio.CancelledError:
        logging.info("stopping gRPC server")
        await server.stop(grace=5)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="async bidirectional streaming gRPC performance test server"
    )
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--concurrency", type=int, default=100)
    return parser.parse_args()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    args = parse_args()
    try:
        asyncio.run(serve(port=args.port, concurrency=args.concurrency))
    except KeyboardInterrupt:
        logging.info("stopping gRPC server")
