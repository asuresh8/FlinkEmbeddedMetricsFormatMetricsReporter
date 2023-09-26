/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.asuresh8.flink.metrics;

import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.Meter;
import org.apache.flink.metrics.MetricConfig;
import org.apache.flink.metrics.reporter.AbstractReporter;
import org.apache.flink.metrics.reporter.Scheduled;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.cloudwatchlogs.emf.exception.InvalidMetricException;
import software.amazon.cloudwatchlogs.emf.logger.MetricsLogger;
import software.amazon.cloudwatchlogs.emf.model.DimensionSet;

import java.util.Arrays;
import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class EmfMetricsReporter extends AbstractReporter implements Scheduled {

    private static final Logger LOG = LoggerFactory.getLogger(EmfMetricsReporter.class);

    static final String APPLICATION_NAME_PROPERTY = "applicationName";
    static final String HOST_IP_PROPERTY = "hostIp";
    static final String LOGGER_NAME = "loggerName";
    static final String NAMESPACE_PROPERTY = "namespace";

    private List<MetricsLogger> metricsLoggers = null;

    public EmfMetricsReporter() {
    }
    
    // VisibleForTesting
    EmfMetricsReporter(List<MetricsLogger> metricsLoggers) {
        this.metricsLoggers = metricsLoggers;
    }

    // VisibleForTesting
    static String extractMetricName(String fullMetricName) {
        final String[] components = fullMetricName.split("\\.", -1);
        if (components.length > 1) {
            final String role = components[1];
            if (role.equals("jobmanager")) {
                if (components.length > 2 && components[2].equals("Status")) {
                    return role + "." + String.join(".", Arrays.copyOfRange(components, 3, components.length));
                } else {
                    return components[components.length - 1];
                }
            } else {
                if (components.length > 3 && components[3].equals("Status")) {
                    return role + "." + String.join(".", Arrays.copyOfRange(components, 4, components.length));
                } else if (components.length > 6) {
                    return components[4] + "." + String.join(".", Arrays.copyOfRange(components, 6, components.length));
                } else {
                    return components[components.length - 1];
                }
            }
        } else {
            return fullMetricName;
        }
    }

    @Override
    public void open(MetricConfig config) {
        final String application = config.getString(APPLICATION_NAME_PROPERTY, "");
        final String hostIp = config.getString(HOST_IP_PROPERTY, "");
        final String loggerName = config.getString(LOGGER_NAME, "");
        final String namespace = config.getString(NAMESPACE_PROPERTY, "");

        final Logger logger = LoggerFactory.getLogger(loggerName);
        final Slf4jSink metricLoggerSink = new Slf4jSink(logger);
        final FlinkEnvironment metricLoggerEnvironment = new FlinkEnvironment(metricLoggerSink);

        final MetricsLogger metricsLoggerWithApplicationDimension = new MetricsLogger(metricLoggerEnvironment);
        final MetricsLogger metricsLoggerWithApplicationAndHostDimensions = new MetricsLogger(metricLoggerEnvironment);
        try {
            metricsLoggerWithApplicationDimension.setNamespace(namespace);
            metricsLoggerWithApplicationAndHostDimensions.setNamespace(namespace);
            metricsLoggerWithApplicationDimension.setDimensions(DimensionSet.of("Application", application));
            metricsLoggerWithApplicationAndHostDimensions.setDimensions(
                DimensionSet.of("Application", application, "HostIp", hostIp));
        } catch (final Exception e) {
            LOG.error("Failed to set namespace and dimensions on metrics logger");
            throw new RuntimeException(e);
        }

        this.metricsLoggers = List.of(
            metricsLoggerWithApplicationDimension,
            metricsLoggerWithApplicationAndHostDimensions);
    }

    @Override
    public void report() {
        if (this.metricsLoggers == null) {
            log.warn("Metrics loggers not initialized");
            return;
        }

        try {
            // Report Gauges
            for (Map.Entry<Gauge<?>, String> entry : gauges.entrySet()) {
                reportGauge(entry.getValue(), entry.getKey());
            }

            // Report Counters
            for (Map.Entry<Counter, String> entry : counters.entrySet()) {
                reportCounter(entry.getValue(), entry.getKey());
            }

            // Report Histograms
            for (Map.Entry<Histogram, String> entry : histograms.entrySet()) {
                reportHistogram(entry.getValue(), entry.getKey());
            }

            // Report Meters
            for (Map.Entry<Meter, String> entry : meters.entrySet()) {
                reportMeter(entry.getValue(), entry.getKey());
            }
        } catch (ConcurrentModificationException | NoSuchElementException e) {
            // Ignore - may happen when metrics are concurrently added or removed
            // Report next time
            LOG.warn("Exception occurred while reporting metrics, will attempt to report next time", e);
        }

        for (final MetricsLogger metricsLogger: metricsLoggers) {
            metricsLogger.flush();
        }
    }

    private void reportGauge(final String rawName, final Gauge<?> gauge) {
        final String metricName = extractMetricName(rawName);
        final Object value = gauge.getValue();
        if (value == null) {
            return;
        }

        if (value instanceof Number) {
            try {
                for (final MetricsLogger metricsLogger: metricsLoggers) {
                    metricsLogger.putMetric(metricName, ((Number) value).doubleValue());
                }
            } catch (InvalidMetricException e) {
                LOG.error("Failed to append metrics to metrics logger", e);
            }
        } else {
            LOG.error("Cannot report metric {} because guage value was not numeric", metricName);
        }
    }

    private void reportCounter(final String rawName, final Counter counter) {
        final String metricName = extractMetricName(rawName);
        final long count = counter.getCount();
        try {
            for (final MetricsLogger metricsLogger: metricsLoggers) {
                metricsLogger.putMetric(metricName, count);
            }
        } catch (InvalidMetricException e) {
            LOG.error("Failed to append metrics to metrics logger", e);
        }
    }

    private void reportHistogram(final String rawName, final Histogram histogram) {
        final String metricName = extractMetricName(rawName);
        final HistogramStatistics statistics = histogram.getStatistics();

        try {
            for (final long value: statistics.getValues()) {
                for (final MetricsLogger metricsLogger: metricsLoggers) {
                    metricsLogger.putMetric(metricName, value);
                }
            }
        } catch (InvalidMetricException e) {
            LOG.error("Failed to append metrics to metrics logger", e);
        }
    }

    private void reportMeter(final String rawName, final Meter meter) {
        String metricName = extractMetricName(rawName);
        try {
            for (final MetricsLogger metricsLogger: metricsLoggers) {
                metricsLogger.putMetric(metricName, meter.getRate());
            }
        } catch (InvalidMetricException e) {
            LOG.error("Failed to append metrics to metrics logger", e);
        }
    }

    @Override
    public String filterCharacters(String input) {
        return input;
    }

    @Override
    public void close() {
        for (final MetricsLogger metricsLogger: metricsLoggers) {
            metricsLogger.flush();
        }
    }
}
