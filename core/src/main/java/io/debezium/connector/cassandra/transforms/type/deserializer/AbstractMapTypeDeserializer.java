/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms.type.deserializer;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Values;

import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.internal.core.type.DefaultMapType;

import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;
import io.debezium.connector.cassandra.transforms.DebeziumTypeDeserializer;

public abstract class AbstractMapTypeDeserializer extends CollectionTypeDeserializer {

    public AbstractMapTypeDeserializer(DebeziumTypeDeserializer deserializer, Integer dataType, Class<?> abstractTypeClass) {
        super(deserializer, dataType, abstractTypeClass);
    }

    @Override
    public Object deserialize(Object abstractType, ByteBuffer bb) {
        Map<?, ?> deserializedMap = (Map<?, ?>) super.deserialize(abstractType, bb);
        deserializedMap = processKeyValueInDeserializedMap(abstractType, deserializedMap);
        return Values.convertToMap(getSchemaBuilder(abstractType).build(), deserializedMap);
    }

    @Override
    public SchemaBuilder getSchemaBuilder(Object abstractType) {
        Object keysType = getKeysType(abstractType);
        Object valuesType = getValuesType(abstractType);
        Schema keySchema = CassandraTypeDeserializer.getSchemaBuilder(keysType).build();
        Schema valuesSchema = CassandraTypeDeserializer.getSchemaBuilder(valuesType).build();
        return SchemaBuilder.map(keySchema, valuesSchema).optional();
    }

    @Override
    public Object deserialize(Object abstractType, List<ByteBuffer> bbList) {
        Object keysType = getKeysType(abstractType);
        Object valuesType = getValuesType(abstractType);
        Map<Object, Object> deserializedMap = new HashMap<>();
        int i = 0;
        while (i < bbList.size()) {
            ByteBuffer kbb = bbList.get(i++);
            ByteBuffer vbb = bbList.get(i++);
            deserializedMap.put(CassandraTypeDeserializer.deserialize(keysType, kbb), CassandraTypeDeserializer.deserialize(valuesType, vbb));
        }
        return Values.convertToMap(getSchemaBuilder(abstractType).build(), deserializedMap);
    }

    /**
     * If elements in a deserialized map is LogicalType, convert each element to fit in Kafka Schema type
     *
     * @param abstractType    the AbstractType of a column in Cassandra
     * @param deserializedMap Map deserialized from Cassandra
     * @return A deserialized map from Cassandra with each element that fits in Kafka Schema type
     */
    private Map<?, ?> processKeyValueInDeserializedMap(Object abstractType, Map<?, ?> deserializedMap) {
        Object keysType = getKeysType(abstractType);
        Object valuesType = getValuesType(abstractType);
        TypeDeserializer keysTypeDeserializer = CassandraTypeDeserializer.getTypeDeserializer(keysType);
        TypeDeserializer valuesTypeDeserializer = CassandraTypeDeserializer.getTypeDeserializer(valuesType);
        Map<Object, Object> resultedMap = new HashMap<>();

        for (Map.Entry<?, ?> entry : deserializedMap.entrySet()) {
            Object key = entry.getKey();
            if (keysTypeDeserializer instanceof LogicalTypeDeserializer) {
                key = ((LogicalTypeDeserializer) keysTypeDeserializer).formatDeserializedValue(keysType, key);
            }
            else if (keysTypeDeserializer instanceof AbstractUserDefinedTypeDeserializer || keysTypeDeserializer instanceof AbstractTupleTypeDeserializer) {
                key = keysTypeDeserializer.deserialize(keysType, (ByteBuffer) key);
            }
            Object value = entry.getValue();
            if (valuesTypeDeserializer instanceof LogicalTypeDeserializer) {
                value = ((LogicalTypeDeserializer) valuesTypeDeserializer).formatDeserializedValue(valuesType, value);
            }
            else if (valuesTypeDeserializer instanceof AbstractUserDefinedTypeDeserializer || valuesTypeDeserializer instanceof AbstractTupleTypeDeserializer) {
                value = valuesTypeDeserializer.deserialize(valuesType, (ByteBuffer) value);
            }
            resultedMap.put(key, value);
        }

        return resultedMap;
    }

    @Override
    public Object getAbstractType(DataType dataType) {
        DefaultMapType mapType = (DefaultMapType) dataType;
        DataType innerKeyType = mapType.getKeyType();
        Object innerKeyAbstractType = CassandraTypeDeserializer.getTypeDeserializer(innerKeyType)
                .getAbstractType(innerKeyType);
        DataType innerValueType = mapType.getValueType();
        Object innerValueAbstractType = CassandraTypeDeserializer.getTypeDeserializer(innerValueType)
                .getAbstractType(innerValueType);
        return getAbstractTypeInstance(innerKeyAbstractType, innerValueAbstractType, !mapType.isFrozen());
    }

    protected abstract Object getKeysType(Object abstractType);

    protected abstract Object getValuesType(Object abstractType);

    protected abstract Object getAbstractTypeInstance(Object innerKeyAbstractType, Object innerValueAbstractType,
                                                      boolean isMultiCell);
}
