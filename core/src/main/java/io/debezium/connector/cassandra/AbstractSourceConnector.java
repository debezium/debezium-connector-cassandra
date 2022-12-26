/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.source.SourceConnector;

import io.debezium.annotation.Immutable;

public abstract class AbstractSourceConnector extends SourceConnector {

    @Immutable
    private Map<String, String> properties;

    @Override
    public String version() {
        return Module.version();
    }

    @Override
    public void start(java.util.Map<String, String> props) {
        this.properties = Collections.unmodifiableMap(new HashMap<>(props));
    }

    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (maxTasks > 1) {
            throw new IllegalArgumentException("Only a single connector task may be started");
        }
        return Collections.singletonList(properties);
    }

    @Override
    public void stop() {
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

}
