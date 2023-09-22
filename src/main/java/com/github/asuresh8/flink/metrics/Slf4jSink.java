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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.cloudwatchlogs.emf.model.MetricsContext;
import software.amazon.cloudwatchlogs.emf.sinks.ISink;

import java.util.concurrent.CompletableFuture;

public class Slf4jSink implements ISink {

    private static final Logger LOG = LoggerFactory.getLogger(Slf4jSink.class);

    private final Logger logger;

    public Slf4jSink(final Logger logger) {
        this.logger = logger;
    }

    @Override
    public void accept(MetricsContext context) {
        try {
            for (String event : context.serialize()) {
                // Logging the serialized metric event using Log4j2
                logger.info(event);
            }
        } catch (Exception e) {
            LOG.error("Failed to serialize a MetricsContext: ", e);
        }
    }

    @Override
    public CompletableFuture<Void> shutdown() {
        // Gracefully shutting down the logger can be done if needed
        // For simplicity, we'll complete the future immediately
        return CompletableFuture.completedFuture(null);
    }
}
