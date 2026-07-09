# gRPC Performance Test Server

비동기 양방향 streaming으로 요청을 받아 제한된 동시성으로 처리하는 gRPC
서버입니다. `id`, `text`, `correlation_id`를 받아 그대로 반환하며 각 요청마다
40~200ms 동안 비동기 대기합니다.

## 실행

```bash
uv sync
uv run python main.py
```

기본 포트는 `50051`, 기본 동시 처리 수는 `100`입니다.

```bash
uv run python main.py --port 50052 --concurrency 200
```

샘플 streaming 요청:

```bash
uv run python client.py --count 100
```

## proto 코드 다시 생성

```bash
uv run python -m grpc_tools.protoc \
  -I. \
  --python_out=. \
  --grpc_python_out=. \
  proto/performance.proto
```
