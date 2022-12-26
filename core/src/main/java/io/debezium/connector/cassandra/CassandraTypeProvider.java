/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.List;
import java.util.function.Function;

import io.debezium.connector.cassandra.transforms.type.deserializer.AbstractTypeDeserializer;

public interface CassandraTypeProvider {

    List<AbstractTypeDeserializer> deserializers();

    Function<Object, Object> baseTypeForReversedType();

    String getClusterName();

}
