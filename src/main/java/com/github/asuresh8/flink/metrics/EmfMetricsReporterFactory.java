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

import org.apache.flink.metrics.reporter.MetricReporter;
import org.apache.flink.metrics.reporter.MetricReporterFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.cloudwatchlogs.emf.logger.MetricsLogger;
import software.amazon.cloudwatchlogs.emf.model.DimensionSet;

import java.util.List;
import java.util.Properties;

public class EmfMetricsReporterFactory implements MetricReporterFactory {

    private static final Logger LOG = LoggerFactory.getLogger(EmfMetricsReporterFactory.class);

    static final String APPLICATION_NAME_PROPERTY = "applicationName";
    static final String HOST_IP_PROPERTY = "hostIp";

    static final String LOGGER_NAME = "loggerName";
    static final String NAMESPACE_PROPERTY = "namespace";

    @Override
    public MetricReporter createMetricReporter(final Properties properties) {
        final String application = properties.getProperty(APPLICATION_NAME_PROPERTY, "");
        final String hostIp = properties.getProperty(HOST_IP_PROPERTY, "");
        final String loggerName = properties.getProperty(LOGGER_NAME, "");
        final String namespace = properties.getProperty(NAMESPACE_PROPERTY, "");

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

        return new EmfMetricsReporter(List.of(
            metricsLoggerWithApplicationDimension,
            metricsLoggerWithApplicationAndHostDimensions
        ));
    }

}