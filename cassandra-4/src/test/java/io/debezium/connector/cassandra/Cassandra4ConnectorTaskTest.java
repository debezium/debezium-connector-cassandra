/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static org.junit.jupiter.api.Assertions.assertNotNull;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;
import io.debezium.connector.cassandra.utils.TestUtils;
import io.debezium.connector.common.CdcSourceTaskContext;

class Cassandra4ConnectorTaskTest {

    @Test
    void shouldCreateTaskContextDuringPreStart() throws Exception {
        Cassandra4ConnectorTask task = new Cassandra4ConnectorTask();

        CdcSourceTaskContext<?> taskContext = task.preStart(Configuration.from(TestUtils.generateDefaultConfigMap()));

        assertNotNull(taskContext);
    }
}
