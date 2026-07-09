package org.noveljava.grpcperformancetestclient;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class GrpcClientConfig {

    @Bean(destroyMethod = "shutdown")
    ManagedChannel performanceTestChannel(
            @Value("${grpc.server.host}") String host,
            @Value("${grpc.server.port}") int port) {
        return ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
    }

    @Bean(destroyMethod = "close")
    ExecutorService grpcBatchExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}
