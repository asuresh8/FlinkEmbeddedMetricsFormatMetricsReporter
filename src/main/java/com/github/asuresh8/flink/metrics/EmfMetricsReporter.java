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

import java.util.ConcurrentModificationException;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

public class EmfMetricsReporter extends AbstractReporter implements Scheduled {

    private static final Logger LOG = LoggerFactory.getLogger(EmfMetricsReporter.class);

    private final List<MetricsLogger> metricsLoggers;

    public EmfMetricsReporter(final List<MetricsLogger> metricsLoggers) {
        this.metricsLoggers = metricsLoggers;
    }

    private String extractMetricName(String fullMetricName) {
        int underscoreCount = 0;
        for (int i = fullMetricName.length() - 1; i >= 0; i--) {
            if (fullMetricName.charAt(i) == '_') {
                return fullMetricName.substring(0, i);
            }
        }
        // Return the original string if the expected number of underscores are not found
        return fullMetricName;
    }


    @Override
    public void open(MetricConfig config) {

    }

    @Override
    public void report() {
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
