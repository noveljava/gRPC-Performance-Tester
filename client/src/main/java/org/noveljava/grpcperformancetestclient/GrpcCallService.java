package org.noveljava.grpcperformancetestclient;

import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;
import jakarta.annotation.PreDestroy;
import org.noveljava.grpcperformancetestclient.grpc.PerformanceTestGrpc;
import org.noveljava.grpcperformancetestclient.grpc.TestRequest;
import org.noveljava.grpcperformancetestclient.grpc.TestResponse;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

@Service
public class GrpcCallService {

    private final PerformanceTestGrpc.PerformanceTestStub stub;
    private final Duration timeout;
    private final AtomicLong correlationSequence = new AtomicLong();
    private final ConcurrentHashMap<Long, CompletableFuture<GrpcCallResult>> pending =
            new ConcurrentHashMap<>();
    private final Object streamLock = new Object();

    private volatile StreamObserver<TestRequest> requestStream;

    public GrpcCallService(
            ManagedChannel performanceTestChannel,
            @Value("${grpc.client.timeout}") Duration timeout) {
        this.stub = PerformanceTestGrpc.newStub(performanceTestChannel);
        this.timeout = timeout;
    }

    public GrpcCallResult call(long id) {
        long correlationId = correlationSequence.incrementAndGet();
        CompletableFuture<GrpcCallResult> future = new CompletableFuture<>();
        pending.put(correlationId, future);

        try {
            send(TestRequest.newBuilder()
                    .setId(id)
                    .setText("")
                    .setCorrelationId(correlationId)
                    .build());
            return future.get(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw Status.CANCELLED
                    .withDescription("Interrupted while waiting for gRPC response")
                    .withCause(exception)
                    .asRuntimeException();
        } catch (TimeoutException exception) {
            throw Status.DEADLINE_EXCEEDED
                    .withDescription("Timed out waiting for gRPC response")
                    .withCause(exception)
                    .asRuntimeException();
        } catch (ExecutionException exception) {
            Throwable cause = exception.getCause();
            if (cause instanceof StatusRuntimeException statusException) {
                throw statusException;
            }
            throw Status.UNKNOWN.withCause(cause).asRuntimeException();
        } finally {
            pending.remove(correlationId);
        }
    }

    private void send(TestRequest request) {
        synchronized (streamLock) {
            if (requestStream == null) {
                requestStream = stub.process(responseObserver());
            }

            try {
                requestStream.onNext(request);
            } catch (RuntimeException exception) {
                requestStream = null;
                failAll(exception);
                throw exception;
            }
        }
    }

    private StreamObserver<TestResponse> responseObserver() {
        return new StreamObserver<>() {
            @Override
            public void onNext(TestResponse response) {
                CompletableFuture<GrpcCallResult> future =
                        pending.remove(response.getCorrelationId());
                if (future != null) {
                    future.complete(new GrpcCallResult(response.getId(), response.getText()));
                }
            }

            @Override
            public void onError(Throwable throwable) {
                closeStream(throwable);
            }

            @Override
            public void onCompleted() {
                closeStream(Status.UNAVAILABLE
                        .withDescription("gRPC response stream was closed")
                        .asRuntimeException());
            }
        };
    }

    private void closeStream(Throwable throwable) {
        synchronized (streamLock) {
            requestStream = null;
        }
        failAll(throwable);
    }

    private void failAll(Throwable throwable) {
        pending.forEach((correlationId, future) -> future.completeExceptionally(throwable));
        pending.clear();
    }

    @PreDestroy
    void close() {
        synchronized (streamLock) {
            if (requestStream != null) {
                requestStream.onCompleted();
                requestStream = null;
            }
        }
    }

    public record GrpcCallResult(long id, String text) {
    }
}
