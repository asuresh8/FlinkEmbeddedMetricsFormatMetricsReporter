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
        properties.setProperty(EmfMetricsReporterFactory.APPLICATION_NAME_PROPERTY, "applicationName");
        properties.setProperty(EmfMetricsReporterFactory.HOST_IP_PROPERTY, "hostIp");
        properties.setProperty(EmfMetricsReporterFactory.LOGGER_NAME, "loggerName");
        properties.setProperty(EmfMetricsReporterFactory.NAMESPACE_PROPERTY, "namespace");

        MetricReporter reporter = factory.createMetricReporter(properties);
        assertThat(reporter, is(notNullValue()));
        assertThat(reporter, is(instanceOf(EmfMetricsReporter.class)));
    }

    @Test
    public void testCreateMetricReporterWithMissingProperties() {
        Properties properties = new Properties();

        Exception exception = assertThrows(RuntimeException.class, () -> {
            factory.createMetricReporter(properties);
        });

        assertThat(exception.getMessage(), containsString("Namespace must include at least one non-whitespace character"));
    }

}
