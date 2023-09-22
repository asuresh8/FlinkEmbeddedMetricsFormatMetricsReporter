package com.github.asuresh8.flink.metrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import software.amazon.cloudwatchlogs.emf.model.MetricsContext;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.*;

class Slf4jSinkTest {

    private Logger logger;

    private Slf4jSink sink;

    @BeforeEach
    void setUp() {
        logger = mock(Logger.class);
        sink = new Slf4jSink(logger);
    }

    @Test
    void testAccept() throws Exception {
        MetricsContext context = mock(MetricsContext.class);
        List<String> events = Arrays.asList("event1", "event2");
        when(context.serialize()).thenReturn(events);

        sink.accept(context);

        // Verify that the info method on the logger is called for each serialized metric event
        events.forEach(event -> verify(logger).info(event));

        // Verify that no error is logged
        verifyNoMoreInteractions(logger);
    }

    @Test
    void testAcceptWithSerializationException() throws Exception {
        Logger logger = mock(Logger.class);
        Slf4jSink sink = new Slf4jSink(logger);

        MetricsContext context = mock(MetricsContext.class);
        when(context.serialize()).thenThrow(new RuntimeException("Serialization Error"));

        sink.accept(context);

        // Verify that no error is thrown
    }

    @Test
    void testShutdown() {
        Logger logger = mock(Logger.class);
        Slf4jSink sink = new Slf4jSink(logger);

        // Verify that the shutdown method returns a completed CompletableFuture
        assertTrue(sink.shutdown().isDone());
    }
}

