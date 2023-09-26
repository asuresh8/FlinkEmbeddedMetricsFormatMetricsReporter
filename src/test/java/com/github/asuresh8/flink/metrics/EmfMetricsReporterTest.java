package com.github.asuresh8.flink.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.Metric;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.cloudwatchlogs.emf.logger.MetricsLogger;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsEqual.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.*;

public class EmfMetricsReporterTest {

    @Captor
    private ArgumentCaptor<String> metricNameCaptor;

    @Captor
    private ArgumentCaptor<Double> metricValueCaptor;

    @Mock
    private MetricsLogger mockedMetricsLogger;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    void open_setsUpMetricsLoggers() {
        // Arrange
        final String application = "application";
        final String hostIp = "hostIp";
        final String loggerName = "loggerName";
        final String namespace = "namespace";
        final Logger logger = LoggerFactory.getLogger(loggerName);
        final MetricConfig config = mock(MetricConfig.class);

        when(config.getString(EmfMetricsReporter.APPLICATION_NAME_PROPERTY, "")).thenReturn(application);
        when(config.getString(EmfMetricsReporter.HOST_IP_PROPERTY, "")).thenReturn(hostIp);
        when(config.getString(EmfMetricsReporter.LOGGER_NAME, "")).thenReturn(loggerName);
        when(config.getString(EmfMetricsReporter.NAMESPACE_PROPERTY, "")).thenReturn(namespace);

        // Act
        final EmfMetricsReporter reporter = new EmfMetricsReporter();
        reporter.open(config);
    }

    @Test
    void open_setsUpMetricsLoggers_exception() {
        // Arrange
        final String application = "";
        final String hostIp = "";
        final String loggerName = "";
        final String namespace = "";
        final MetricConfig config = mock(MetricConfig.class);

        when(config.getString(EmfMetricsReporter.APPLICATION_NAME_PROPERTY, "")).thenReturn(application);
        when(config.getString(EmfMetricsReporter.HOST_IP_PROPERTY, "")).thenReturn(hostIp);
        when(config.getString(EmfMetricsReporter.LOGGER_NAME, "")).thenReturn(loggerName);
        when(config.getString(EmfMetricsReporter.NAMESPACE_PROPERTY, "")).thenReturn(namespace);

        // Act
        final EmfMetricsReporter reporter = new EmfMetricsReporter();
        assertThrows(RuntimeException.class, () -> reporter.open(config));
    }

    @Test
    public void testReportGauge() throws Exception {
        // Arrange
        final String rawName = "rawName";
        final String expectedMetricName = "extractedMetricName";
        final Double gaugeValue = 123.45;
        final Gauge<Double> gauge = () -> gaugeValue;

        // Act & Assert
        testMetricAndAssert(gauge, rawName, Collections.singletonList(gaugeValue));
    }

    @Test
    public void testReportCounter() throws Exception {
        // Arrange
        final String rawName = "rawName";
        final String expectedMetricName = "extractedMetricName";
        final long counterValue = 123L;
        final Counter counter = mock(Counter.class);
        when(counter.getCount()).thenReturn(counterValue);

        // Act & Assert
        testMetricAndAssert(counter, rawName, Collections.singletonList((double) counterValue));
    }

    @Test
    public void testReportHistogram() throws Exception {
        // Arrange
        final String rawName = "rawName";
        final List<Double> histogramValues = Arrays.asList(1.0, 2.0, 3.0);
        final Histogram histogram = mock(Histogram.class);
        final HistogramStatistics statistics = mock(HistogramStatistics.class);
        when(histogram.getStatistics()).thenReturn(statistics);
        when(statistics.getValues()).thenReturn(histogramValues.stream().mapToLong(Double::longValue).toArray());

        // Act & Assert
        testMetricAndAssert(histogram, rawName, histogramValues);
    }

    @Test
    public void testReportMeter() throws Exception {
        // Arrange
        final String rawName = "rawName";
        final double meterRate = 4.56;
        final Meter meter = mock(Meter.class);
        when(meter.getRate()).thenReturn(meterRate);

        // Act & Assert
        testMetricAndAssert(meter, rawName, Collections.singletonList(meterRate));
    }

    @Test
    private void testMetricAndAssert(final Metric metric, final String metricName, List<Double> expectedValues) throws Exception {
        // Initialize your reporter with a list containing the mocked logger
        EmfMetricsReporter reporter = new EmfMetricsReporter(List.of(mockedMetricsLogger));

        reporter.notifyOfAddedMetric(metric, metricName, new UnregisteredMetricsGroup());

        // Call the report() method to capture metrics
        reporter.report();

        // Verify that the putMetric method on the mocked MetricsLogger was called
        verify(mockedMetricsLogger, times(expectedValues.size())).putMetric(metricNameCaptor.capture(), metricValueCaptor.capture());

        // Perform assertions on the captured metric name and value
        final String capturedMetricName = metricNameCaptor.getValue();
        assertThat(capturedMetricName, equalTo(metricName));

        // Perform assertions on the captured metric values
        final List<Double> capturedMetricValues = metricValueCaptor.getAllValues();
        assertThat(capturedMetricValues, equalTo(expectedValues));

        reporter.close();
    }

    @Test
    void testExtractMetricName_JobManager_StatusComponent() {
        final String fullMetricName = "some.jobmanager.Status.some.metric.name";
        final String expected = "jobmanager.some.metric.name";
        final String actual = EmfMetricsReporter.extractMetricName(fullMetricName);
        assertThat(actual, equalTo(expected));
    }

    @Test
    void testExtractMetricName_TaskManager_StatusComponent() {
        final String fullMetricName = "some.taskmanager.other.Status.some.metric.name";
        final String expected = "taskmanager.some.metric.name";
        final String actual = EmfMetricsReporter.extractMetricName(fullMetricName);
        assertThat(actual, equalTo(expected));
    }

    @Test
    void testExtractMetricName_TaskManager_OtherComponent() {
        final String fullMetricName = "some.taskmanager.port.job.stage.shard.metric";
        final String expected = "stage.metric";
        final String actual = EmfMetricsReporter.extractMetricName(fullMetricName);
        assertThat(actual, equalTo(expected));
    }

    @Test
    void testExtractMetricName_SingleComponent() {
        final String fullMetricName = "singleComponent";
        final String expected = "singleComponent";
        final String actual = EmfMetricsReporter.extractMetricName(fullMetricName);
        assertThat(actual, equalTo(expected));
    }

    @Test
    void testExtractMetricName_MultipleComponents_NoSpecialCase() {
        final String fullMetricName = "some.prefix.other.metric.name";
        final String expected = "name";
        final String actual = EmfMetricsReporter.extractMetricName(fullMetricName);
        assertThat(actual, equalTo(expected));
    }

    @Test
    void testExtractMetricName_EmptyString() {
        final String fullMetricName = "";
        final String expected = "";
        final String actual = EmfMetricsReporter.extractMetricName(fullMetricName);
        assertThat(actual, equalTo(expected));
    }

}
