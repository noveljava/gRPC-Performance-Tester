package org.noveljava.grpcperformancetestclient;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.webmvc.test.autoconfigure.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@WebMvcTest(GrpcCallController.class)
class GrpcCallControllerTests {

    @Autowired
    private MockMvc mockMvc;

    @MockitoBean
    private GrpcCallService grpcCallService;

    @MockitoBean
    private GrpcBatchService grpcBatchService;

    @Test
    void callsGrpcServiceWithPathId() throws Exception {
        when(grpcCallService.call(123L))
                .thenReturn(new GrpcCallService.GrpcCallResult(123L, ""));

        mockMvc.perform(get("/api/call/grpc/123"))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.id").value(123))
                .andExpect(jsonPath("$.text").value(""));

        verify(grpcCallService).call(123L);
    }

    @Test
    void callsBatchService() throws Exception {
        when(grpcBatchService.call(1L, 20_000, 100))
                .thenReturn(new GrpcBatchService.BatchResult(
                        20_000,
                        20_000,
                        0,
                        24_000,
                        833.33
                ));

        mockMvc.perform(post("/api/call/grpc/batch")
                        .contentType("application/json")
                        .content("""
                                {
                                  "startId": 1,
                                  "count": 20000,
                                  "concurrency": 100
                                }
                                """))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.total").value(20_000))
                .andExpect(jsonPath("$.succeeded").value(20_000))
                .andExpect(jsonPath("$.failed").value(0))
                .andExpect(jsonPath("$.elapsedMillis").value(24_000));

        verify(grpcBatchService).call(1L, 20_000, 100);
    }

    @Test
    void rejectsInvalidBatchConcurrency() throws Exception {
        mockMvc.perform(post("/api/call/grpc/batch")
                        .contentType("application/json")
                        .content("""
                                {
                                  "startId": 1,
                                  "count": 20000,
                                  "concurrency": 0
                                }
                                """))
                .andExpect(status().isBadRequest());
    }
}
