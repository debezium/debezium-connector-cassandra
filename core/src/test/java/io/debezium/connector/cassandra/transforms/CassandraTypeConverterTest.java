/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.transforms;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.cassandra.cql3.FieldIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.ByteType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.DecimalType;
import org.apache.cassandra.db.marshal.DoubleType;
import org.apache.cassandra.db.marshal.DurationType;
import org.apache.cassandra.db.marshal.FloatType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.db.marshal.ShortType;
import org.apache.cassandra.db.marshal.SimpleDateType;
import org.apache.cassandra.db.marshal.TimeType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.TimestampType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.junit.Assert;
import org.junit.Test;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.internal.core.type.DefaultUserDefinedType;

public class CassandraTypeConverterTest {

    @Test
    public void testAscii() {
        DataType asciiType = DataTypes.ASCII;
        AbstractType<?> convertedType = CassandraTypeConverter.convert(asciiType);

        AsciiType expectedType = AsciiType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testBlob() {
        DataType blobType = DataTypes.BLOB;
        AbstractType<?> convertedType = CassandraTypeConverter.convert(blobType);

        BytesType expectedType = BytesType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testBigInt() {
        DataType bigIntType = DataTypes.BIGINT;
        AbstractType<?> convertedType = CassandraTypeConverter.convert(bigIntType);

        LongType expectedType = LongType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testBoolean() {
        DataType booleanType = DataTypes.BOOLEAN;
        AbstractType<?> convertedType = CassandraTypeConverter.convert(booleanType);

        BooleanType expectedType = BooleanType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testCounter() {
        DataType counterType = DataTypes.COUNTER;
        AbstractType<?> convertedType = CassandraTypeConverter.convert(counterType);

        CounterColumnType expectedType = CounterColumnType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testDate() {
        DataType dateType = DataTypes.DATE;
        AbstractType<?> convertedType = CassandraTypeConverter.convert(dateType);

        SimpleDateType expectedType = SimpleDateType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testDecimal() {
        DataType decimalType = DataTypes.DECIMAL;
        AbstractType<?> convertedType = CassandraTypeConverter.convert(decimalType);

        DecimalType expectedType = DecimalType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testDouble() {
        DataType doubleType = DataTypes.DOUBLE;
        AbstractType<?> convertedType = CassandraTypeConverter.convert(doubleType);

        DoubleType expectedType = DoubleType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testDuration() {
        DataType durationType = DataTypes.DURATION;
        AbstractType<?> convertedType = CassandraTypeConverter.convert(durationType);

        DurationType expectedType = DurationType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testFloat() {
        DataType floatType = DataTypes.FLOAT;
        AbstractType<?> convertedType = CassandraTypeConverter.convert(floatType);

        FloatType expectedType = FloatType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testInet() {
        DataType inetType = DataTypes.INET;
        AbstractType<?> convertedType = CassandraTypeConverter.convert(inetType);

        InetAddressType expectedType = InetAddressType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testInt() {
        DataType intType = DataTypes.INT;
        AbstractType<?> convertedType = CassandraTypeConverter.convert(intType);

        Int32Type expectedType = Int32Type.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testList() {
        // list of ints
        // test non-frozen
        DataType listType = DataTypes.listOf(DataTypes.INT, false);
        AbstractType<?> convertedType = CassandraTypeConverter.convert(listType);

        ListType<?> expectedType = ListType.getInstance(Int32Type.instance, true);
        Assert.assertEquals(expectedType, convertedType);

        // test frozen
        listType = DataTypes.listOf(DataTypes.INT, true);
        convertedType = CassandraTypeConverter.convert(listType);
        expectedType = ListType.getInstance(Int32Type.instance, false);
        Assert.assertEquals(expectedType, convertedType);
        Assert.assertTrue("Expected convertedType to be frozen", convertedType.isFrozenCollection());
    }

    @Test
    public void testMap() {
        // map from ASCII to Double
        // test non-frozen
        DataType mapType = DataTypes.mapOf(DataTypes.ASCII, DataTypes.DOUBLE, false);
        AbstractType<?> convertedType = CassandraTypeConverter.convert(mapType);

        MapType<?, ?> expectedType = MapType.getInstance(AsciiType.instance, DoubleType.instance, true);
        Assert.assertEquals(expectedType, convertedType);

        // test frozen
        mapType = DataTypes.mapOf(DataTypes.ASCII, DataTypes.DOUBLE, true);
        convertedType = CassandraTypeConverter.convert(mapType);
        expectedType = MapType.getInstance(AsciiType.instance, DoubleType.instance, false);
        Assert.assertEquals(expectedType, convertedType);
        Assert.assertTrue("Expected convertType to be frozen", convertedType.isFrozenCollection());
    }

    @Test
    public void testSet() {
        // set of floats
        // test non-frozen
        DataType setType = DataTypes.setOf(DataTypes.FLOAT, false);
        AbstractType<?> convertedType = CassandraTypeConverter.convert(setType);

        SetType<?> expectedType = SetType.getInstance(FloatType.instance, true);
        Assert.assertEquals(expectedType, convertedType);

        // test frozen
        setType = DataTypes.setOf(DataTypes.FLOAT, true);
        convertedType = CassandraTypeConverter.convert(setType);
        expectedType = SetType.getInstance(FloatType.instance, false);
        Assert.assertEquals(expectedType, convertedType);
        Assert.assertTrue("Expected convertedType to be frozen", convertedType.isFrozenCollection());

    }

    @Test
    public void testSmallInt() {
        DataType smallIntType = DataTypes.SMALLINT;
        AbstractType<?> convertedType = CassandraTypeConverter.convert(smallIntType);

        ShortType expectedType = ShortType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testText() {
        DataType textType = DataTypes.TEXT;
        AbstractType<?> convertedType = CassandraTypeConverter.convert(textType);

        UTF8Type expectedType = UTF8Type.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testTime() {
        DataType timeType = DataTypes.TIME;
        AbstractType<?> convertedType = CassandraTypeConverter.convert(timeType);

        TimeType expectedType = TimeType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testTimestamp() {
        DataType timestampType = DataTypes.TIMESTAMP;
        AbstractType<?> convertedType = CassandraTypeConverter.convert(timestampType);

        TimestampType expectedType = TimestampType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testTimeUUID() {
        DataType timeUUID = DataTypes.TIMEUUID;
        AbstractType<?> convertedType = CassandraTypeConverter.convert(timeUUID);

        TimeUUIDType expectedType = TimeUUIDType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testTinyInt() {
        DataType tinyInt = DataTypes.TINYINT;
        AbstractType<?> convertedType = CassandraTypeConverter.convert(tinyInt);

        ByteType expectedType = ByteType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testVarInt() {
        DataType varInt = DataTypes.VARINT;
        AbstractType<?> convertedType = CassandraTypeConverter.convert(varInt);

        IntegerType expectedType = IntegerType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testTuple() {
        // tuple containing timestamp and smallint.
        // tuples are always frozen, so we don't need to test that.
        // we don't care about the protocol version or the codec registry.
        DataType tupleType = DataTypes.tupleOf(DataTypes.TIMESTAMP, DataTypes.SMALLINT);
        AbstractType<?> convertedType = CassandraTypeConverter.convert(tupleType);

        List<AbstractType<?>> innerAbstractTypes = new ArrayList<>(2);
        innerAbstractTypes.add(TimestampType.instance);
        innerAbstractTypes.add(ShortType.instance);
        org.apache.cassandra.db.marshal.TupleType expectedType = new org.apache.cassandra.db.marshal.TupleType(innerAbstractTypes);

        Assert.assertEquals(expectedType, convertedType);
    }

    @Test
    public void testUdt() {
        DefaultUserDefinedType nonFrozenUserDefinedType = new DefaultUserDefinedType(CqlIdentifier.fromCql("ks1"),
                CqlIdentifier.fromInternal("FooType"),
                false,
                Collections.singletonList(CqlIdentifier.fromCql("field1")),
                Collections.singletonList(DataTypes.TEXT));

        DefaultUserDefinedType frozenUserDefinedType = new DefaultUserDefinedType(CqlIdentifier.fromCql("ks1"),
                CqlIdentifier.fromInternal("FooType"),
                true,
                Collections.singletonList(CqlIdentifier.fromCql("field1")),
                Collections.singletonList(DataTypes.TEXT));

        ByteBuffer expectedTypeName = UTF8Type.instance.decompose("FooType");
        List<FieldIdentifier> expectedFieldIdentifiers = new ArrayList<>();
        expectedFieldIdentifiers.add(new FieldIdentifier(ByteBuffer.wrap("field1".getBytes(Charset.defaultCharset()))));
        List<AbstractType<?>> expectedFieldTypes = new ArrayList<>();
        expectedFieldTypes.add(UTF8Type.instance);

        // non-frozen
        org.apache.cassandra.db.marshal.UserType expectedAbstractType = new org.apache.cassandra.db.marshal.UserType("ks1",
                expectedTypeName,
                expectedFieldIdentifiers,
                expectedFieldTypes,
                true);
        AbstractType<?> convertedType = CassandraTypeConverter.convert(nonFrozenUserDefinedType);
        Assert.assertEquals(expectedAbstractType, convertedType);

        // frozen
        expectedAbstractType = new org.apache.cassandra.db.marshal.UserType("ks1",
                expectedTypeName,
                expectedFieldIdentifiers,
                expectedFieldTypes,
                true).freeze();
        convertedType = CassandraTypeConverter.convert(frozenUserDefinedType);
        Assert.assertEquals(expectedAbstractType, convertedType);
    }

    @Test
    public void testUUID() {
        DataType uuid = DataTypes.UUID;

        AbstractType<?> convertedType = CassandraTypeConverter.convert(uuid);

        UUIDType expectedType = UUIDType.instance;

        Assert.assertEquals(expectedType, convertedType);
    }
}
