/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.converter;

import org.apache.cassandra.db.marshal.MapType;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.type.DefaultMapType;

import io.debezium.connector.cassandra.transforms.CassandraTypeConverter;

public class MapTypeConverter implements TypeConverter<MapType<?, ?>> {
    @Override
    public MapType convert(DataType dataType) {
        DefaultMapType mapType = (DefaultMapType) dataType;
        return MapType.getInstance(CassandraTypeConverter.convert(mapType.getKeyType()),
                CassandraTypeConverter.convert(mapType.getValueType()),
                !mapType.isFrozen());
    }
}
