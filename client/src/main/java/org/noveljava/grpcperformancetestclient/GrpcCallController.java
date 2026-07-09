package org.noveljava.grpcperformancetestclient;

import io.grpc.StatusRuntimeException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

@RestController
@RequestMapping("/api/call/grpc")
public class GrpcCallController {

    private final GrpcCallService grpcCallService;
    private final GrpcBatchService grpcBatchService;

    public GrpcCallController(
            GrpcCallService grpcCallService,
            GrpcBatchService grpcBatchService) {
        this.grpcCallService = grpcCallService;
        this.grpcBatchService = grpcBatchService;
    }

    @GetMapping("/{id}")
    public GrpcCallService.GrpcCallResult call(@PathVariable long id) {
        try {
            return grpcCallService.call(id);
        } catch (StatusRuntimeException exception) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_GATEWAY,
                    "gRPC server call failed: " + exception.getStatus().getCode(),
                    exception
            );
        }
    }

    @PostMapping("/batch")
    public GrpcBatchService.BatchResult callBatch(@RequestBody BatchRequest request) {
        if (request.count() < 1 || request.count() > 100_000) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "count must be between 1 and 100000"
            );
        }
        if (request.concurrency() < 1 || request.concurrency() > 1_000) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "concurrency must be between 1 and 1000"
            );
        }

        try {
            Math.addExact(request.startId(), request.count() - 1L);
            return grpcBatchService.call(
                    request.startId(),
                    request.count(),
                    request.concurrency()
            );
        } catch (ArithmeticException exception) {
            throw new ResponseStatusException(
                    HttpStatus.BAD_REQUEST,
                    "startId and count exceed the supported id range",
                    exception
            );
        }
    }

    public record BatchRequest(long startId, int count, int concurrency) {
    }
}
