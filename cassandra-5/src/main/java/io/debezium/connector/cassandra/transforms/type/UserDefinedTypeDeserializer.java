/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.cql3.UserTypes;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UserType;

import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;
import com.datastax.oss.protocol.internal.ProtocolConstants;

import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;
import io.debezium.connector.cassandra.transforms.type.deserializer.AbstractUserDefinedTypeDeserializer;

public class UserDefinedTypeDeserializer extends AbstractUserDefinedTypeDeserializer {

    public UserDefinedTypeDeserializer(DebeziumTypeDeserializer deserializer) {
        super(deserializer, ProtocolConstants.DataType.UDT, UserType.class);
    }

    @Override
    protected List<String> fieldNames(Object abstractType) {
        UserType userType = (UserType) abstractType;
        return userType.fieldNames().stream().map(FieldIdentifier::toString).collect(Collectors.toList());
    }

    @Override
    protected List<?> fieldTypes(Object abstractType) {
        UserType userType = (UserType) abstractType;
        return userType.fieldTypes();
    }

    @Override
    protected List<ByteBuffer> bbList(Object abstractType, ByteBuffer bb) {
        UserType userType = (UserType) abstractType;
        UserTypes.Value value = UserTypes.Value.fromSerialized(bb, userType);
        return value.getElements();
    }

    @Override
    protected String structName(Object abstractType) {
        UserType userType = (UserType) abstractType;
        return userType.keyspace + "." + userType.getNameAsString();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Object getAbstractTypeInstance(DefaultUserDefinedType userType, List<String> fieldNames,
                                             List<?> innerAbstractTypes) {
        ByteBuffer typeNameBuffer = UTF8Type.instance.fromString(userType.getName().toString());
        List<FieldIdentifier> fieldIdentifiers = new ArrayList<>(fieldNames.size());
        for (String fieldName : fieldNames) {
            fieldIdentifiers.add(FieldIdentifier.forInternalString(fieldName));
        }
        return new UserType(userType.getKeyspace().toString(),
                typeNameBuffer,
                fieldIdentifiers,
                (List<AbstractType<?>>) innerAbstractTypes,
                !userType.isFrozen());
    }
}
