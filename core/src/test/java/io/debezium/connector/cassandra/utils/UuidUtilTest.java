/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.UUID;

import org.junit.jupiter.api.Test;

import io.debezium.connector.cassandra.transforms.UuidUtil;

class UuidUtilTest {

    @Test
    void testUuidUtil() {
        UUID uuid = UUID.randomUUID();
        assertEquals(uuid, UuidUtil.asUuid(UuidUtil.asBytes(uuid)));
    }
}
