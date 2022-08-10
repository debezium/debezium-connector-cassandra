/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.converter;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ListType;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.type.DefaultListType;

import io.debezium.connector.cassandra.transforms.CassandraTypeConverter;

public class ListTypeConverter implements TypeConverter<ListType<?>> {
    @Override
    public ListType convert(DataType dataType) {
        DefaultListType listType = (DefaultListType) dataType;
        // list only has one inner type.
        DataType innerDataType = listType.getElementType();
        AbstractType<?> innerAbstractType = CassandraTypeConverter.convert(innerDataType);
        return ListType.getInstance(innerAbstractType, !listType.isFrozen());
    }
}
