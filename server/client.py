import argparse
import asyncio

import grpc

from proto import performance_pb2, performance_pb2_grpc


async def run(host: str, port: int, count: int) -> None:
    async with grpc.aio.insecure_channel(f"{host}:{port}") as channel:
        stub = performance_pb2_grpc.PerformanceTestStub(channel)

        async def requests():
            for index in range(1, count + 1):
                yield performance_pb2.TestRequest(
                    id=index,
                    text=f"performance test {index}",
                    correlation_id=index,
                )

        received = 0
        async for response in stub.Process(requests()):
            received += 1
            print(
                f"id={response.id} text={response.text!r} "
                f"correlation_id={response.correlation_id}"
            )

        print(f"received={received}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="async streaming gRPC sample client")
    parser.add_argument("--host", default="localhost")
    parser.add_argument("--port", type=int, default=50051)
    parser.add_argument("--count", type=int, default=10)
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run(args.host, args.port, args.count))
