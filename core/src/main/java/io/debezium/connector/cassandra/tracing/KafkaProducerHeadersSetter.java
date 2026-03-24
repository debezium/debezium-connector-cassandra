/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.tracing;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

import org.apache.kafka.common.header.Headers;

import io.opentelemetry.context.propagation.TextMapSetter;

public enum KafkaProducerHeadersSetter implements TextMapSetter<Headers> {
    INSTANCE;

    KafkaProducerHeadersSetter() {
    }

    @Override
    public void set(Headers headers, String key, String value) {
        if (Objects.nonNull(headers)) {
            headers.remove(key);
            headers.add(key, value.getBytes(StandardCharsets.UTF_8));
        }
    }
}
