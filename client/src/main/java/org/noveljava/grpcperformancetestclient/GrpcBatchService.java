package org.noveljava.grpcperformancetestclient;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.LongAdder;

@Service
public class GrpcBatchService {

    private final GrpcCallService grpcCallService;
    private final ExecutorService executor;

    public GrpcBatchService(
            GrpcCallService grpcCallService,
            @Qualifier("grpcBatchExecutor") ExecutorService executor) {
        this.grpcCallService = grpcCallService;
        this.executor = executor;
    }

    public BatchResult call(long startId, int count, int concurrency) {
        Instant startedAt = Instant.now();
        Semaphore permits = new Semaphore(concurrency);
        LongAdder succeeded = new LongAdder();
        LongAdder failed = new LongAdder();
        List<CompletableFuture<Void>> tasks = new ArrayList<>(count);

        for (int offset = 0; offset < count; offset++) {
            long id = Math.addExact(startId, offset);
            tasks.add(CompletableFuture.runAsync(
                    () -> execute(id, permits, succeeded, failed),
                    executor
            ));
        }

        CompletableFuture.allOf(tasks.toArray(CompletableFuture[]::new)).join();

        long elapsedMillis = Duration.between(startedAt, Instant.now()).toMillis();
        double requestsPerSecond = elapsedMillis == 0
                ? count
                : count * 1_000.0 / elapsedMillis;

        return new BatchResult(
                count,
                succeeded.sum(),
                failed.sum(),
                elapsedMillis,
                requestsPerSecond
        );
    }

    private void execute(
            long id,
            Semaphore permits,
            LongAdder succeeded,
            LongAdder failed) {
        boolean acquired = false;
        try {
            permits.acquire();
            acquired = true;
            grpcCallService.call(id);
            succeeded.increment();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            failed.increment();
        } catch (RuntimeException exception) {
            failed.increment();
        } finally {
            if (acquired) {
                permits.release();
            }
        }
    }

    public record BatchResult(
            int total,
            long succeeded,
            long failed,
            long elapsedMillis,
            double requestsPerSecond) {
    }
}
