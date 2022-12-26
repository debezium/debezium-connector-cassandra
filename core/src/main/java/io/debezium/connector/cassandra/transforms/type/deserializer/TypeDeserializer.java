/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.nio.ByteBuffer;

import org.apache.kafka.connect.data.SchemaBuilder;

import com.datastax.oss.driver.api.core.type.DataType;

public interface TypeDeserializer {

    Object deserialize(Object abstractType, ByteBuffer bb);

    SchemaBuilder getSchemaBuilder(Object abstractType);

    Object getAbstractType(DataType dataType);
}
