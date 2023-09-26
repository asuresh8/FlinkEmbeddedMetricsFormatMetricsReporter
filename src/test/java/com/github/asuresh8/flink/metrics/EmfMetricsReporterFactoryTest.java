package com.github.asuresh8.flink.metrics;

import org.apache.flink.metrics.reporter.MetricReporter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class EmfMetricsReporterFactoryTest {

    private EmfMetricsReporterFactory factory;

    @BeforeEach
    public void setUp() {
        factory = new EmfMetricsReporterFactory();
    }

    @Test
    public void testCreateMetricReporterWithValidProperties() {
        Properties properties = new Properties();

        MetricReporter reporter = factory.createMetricReporter(properties);
        assertThat(reporter, is(notNullValue()));
        assertThat(reporter, is(instanceOf(EmfMetricsReporter.class)));
    }

}
