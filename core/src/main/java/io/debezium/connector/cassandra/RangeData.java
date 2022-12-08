/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.errors.DataException;

import io.debezium.DebeziumException;

public class RangeData implements KafkaRecord {

    public final String name;
    public final String method;
    public final Map<String, String> values = new HashMap<>();

    public RangeData(String name, String method, Map<String, String> values) {
        this.name = name;
        this.method = method;
        if (values != null) {
            this.values.putAll(values);
        }
    }

    public static RangeData start(String method, Map<String, String> values) {
        return new RangeData("_range_start", method, values);
    }

    public static RangeData end(String method, Map<String, String> values) {
        return new RangeData("_range_end", method, values);
    }

    @Override
    public Struct record(Schema schema) {
        try {
            return new Struct(schema)
                    .put("method", method)
                    .put("values", values);
        }
        catch (DataException e) {
            throw new DebeziumException(e);
        }
    }

    static Schema rangeSchema(String name) {
        return SchemaBuilder.struct().name(name)
                .field("method", Schema.STRING_SCHEMA)
                .field("values", SchemaBuilder.map(Schema.STRING_SCHEMA, Schema.STRING_SCHEMA))
                .optional()
                .build();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        RangeData that = (RangeData) o;
        return Objects.equals(name, that.name)
                && Objects.equals(method, that.method)
                && values.equals(that.values);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, method, values);
    }

    @Override
    public String toString() {
        return "{"
                + "name=" + name
                + ", method=" + method
                + ", values=" + values
                + '}';
    }
}
