package com.github.asuresh8.flink.metrics;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.cloudwatchlogs.emf.model.MetricsContext;
import software.amazon.cloudwatchlogs.emf.sinks.ISink;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

class FlinkEnvironmentTest {

    private FlinkEnvironment flinkEnvironment;
    private ISink sink;

    @BeforeEach
    void setUp() {
        sink = mock(ISink.class);
        flinkEnvironment = new FlinkEnvironment(sink);
    }

    @Test
    void testProbe() {
        // The probe method should always return true
        assertTrue(flinkEnvironment.probe());
    }

    @Test
    void testGetName() {
        // The getName method should return "FlinkEnvironment"
        assertEquals("FlinkEnvironment", flinkEnvironment.getName());
    }

    @Test
    void testGetType() {
        // The getType method should return "FlinkEnvironment"
        assertEquals("FlinkEnvironment", flinkEnvironment.getType());
    }

    @Test
    void testGetLogGroupName() {
        // The getLogGroupName method should return an empty string
        assertEquals("", flinkEnvironment.getLogGroupName());
    }

    @Test
    void testGetSink() {
        // The getSink method should return the sink provided in the constructor
        assertEquals(sink, flinkEnvironment.getSink());
    }

    @Test
    void testConfigureContext() {
        // Create a mocked MetricsContext
        final MetricsContext context = mock(MetricsContext.class);

        // Call the configureContext method
        flinkEnvironment.configureContext(context);

        // Since configureContext is empty, we just test if it runs without any exception
        assertTrue(true);
    }
}
