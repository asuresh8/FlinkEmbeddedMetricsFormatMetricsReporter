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

import software.amazon.cloudwatchlogs.emf.environment.Environment;
import software.amazon.cloudwatchlogs.emf.model.MetricsContext;
import software.amazon.cloudwatchlogs.emf.sinks.ISink;

public class FlinkEnvironment implements Environment {

    private final ISink sink;

    public FlinkEnvironment(final ISink sink) {
        this.sink = sink;
    }

    @Override
    public boolean probe() {
        // Since we are building this for a specific environment (Flink),
        // we'll always return true here. If there are more specific checks
        // you'd like to do to determine if this is the correct environment,
        // add them here.
        return true;
    }

    @Override
    public String getName() {
        return "FlinkEnvironment";
    }

    @Override
    public String getType() {
        return "FlinkEnvironment";
    }

    @Override
    public String getLogGroupName() {
        // This will be set externally
        return "";
    }

    @Override
    public void configureContext(MetricsContext context) {
        // Implement if you'd like to add any custom configurations to the context
        // based on Flink-specific logic.
    }

    @Override
    public ISink getSink() {
        return this.sink;
    }
}
