/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.converter;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;

import io.debezium.connector.cassandra.transforms.CassandraTypeConverter;

public class UserTypeConverter implements TypeConverter<UserType> {

    @Override
    public UserType convert(DataType dataType) {
        DefaultUserDefinedType userType = (DefaultUserDefinedType) dataType;

        String typeNameString = userType.getName().toString();
        List<String> fieldNames = userType.getFieldNames()
                .stream()
                .map(CqlIdentifier::toString)
                .collect(Collectors.toList());

        List<DataType> fieldTypes = userType.getFieldTypes();

        Map<String, DataType> fieldNamesToTypes = new HashMap<>();

        for (int i = 0; i < fieldNames.size(); i++) {
            fieldNamesToTypes.put(fieldNames.get(i), fieldTypes.get(i));
        }

        List<AbstractType<?>> innerAbstractTypes = new ArrayList<>(fieldNames.size());
        ByteBuffer typeNameBuffer = UTF8Type.instance.fromString(typeNameString);

        List<FieldIdentifier> fieldIdentifiers = new ArrayList<>(fieldNames.size());
        for (String fieldName : fieldNames) {
            fieldIdentifiers.add(FieldIdentifier.forInternalString(fieldName));
            innerAbstractTypes.add((CassandraTypeConverter.convert(fieldNamesToTypes.get(fieldName))));
        }

        return new UserType(userType.getKeyspace().toString(),
                typeNameBuffer,
                fieldIdentifiers,
                innerAbstractTypes,
                !userType.isFrozen());
    }

}
