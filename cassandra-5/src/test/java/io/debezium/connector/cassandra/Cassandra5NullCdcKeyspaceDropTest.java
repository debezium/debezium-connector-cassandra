/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.apache.cassandra.schema.Schema;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;

import io.debezium.config.Configuration;
import io.debezium.connector.cassandra.spi.ProvidersResolver;
import io.debezium.connector.cassandra.utils.TestUtils;
import io.debezium.doc.FixFor;
import io.debezium.junit.SkipLongRunning;

@SkipLongRunning
public class Cassandra5NullCdcKeyspaceDropTest extends CassandraConnectorTestBase {

    private static final CqlIdentifier CDC = CqlIdentifier.fromInternal("cdc");
    private static final String KEYSPACE = "dbz3036";
    private static final String UNMANAGED_TABLE = "a_unmanaged";
    private static final String MANAGED_TABLE = "z_managed";

    @Test
    @FixFor("debezium/dbz#3036")
    void shouldRemoveKeyspaceContainingTableWithMissingCdcOption() throws Throwable {
        CassandraConnectorContext connectorContext = null;
        boolean nodeCdcEnabled = true;
        try {
            setNodeCdcEnabled(false);
            nodeCdcEnabled = false;
            TestUtils.runCql("CREATE KEYSPACE " + KEYSPACE
                    + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}");
            TestUtils.runCql("CREATE TABLE " + KEYSPACE + "." + UNMANAGED_TABLE + " (id int PRIMARY KEY) WITH cdc = false");
            TestUtils.runCql("CREATE TABLE " + KEYSPACE + "." + MANAGED_TABLE + " (id int PRIMARY KEY) WITH cdc = false");

            setNodeCdcEnabled(true);
            nodeCdcEnabled = true;
            TestUtils.runCql("ALTER TABLE " + KEYSPACE + "." + MANAGED_TABLE + " WITH cdc = true");
            assertCdcMetadataPrecondition();

            provider = ProvidersResolver.resolveConnectorContextProvider();
            connectorContext = provider.provideContext(Configuration.from(TestUtils.generateDefaultConfigMap()));
            final var initializedContext = connectorContext;
            await().atMost(10, SECONDS).untilAsserted(() -> {
                assertNotNull(initializedContext.getSchemaHolder().getKeyValueSchema(new KeyspaceTable(KEYSPACE, MANAGED_TABLE)));
                assertNotNull(Schema.instance.getKeyspaceMetadata(KEYSPACE));
            });

            TestUtils.runCql("DROP KEYSPACE " + KEYSPACE);

            await().atMost(10, SECONDS).untilAsserted(() -> assertNull(Schema.instance.getKeyspaceMetadata(KEYSPACE)));
        }
        finally {
            if (connectorContext != null) {
                connectorContext.cleanUp();
            }
            TestUtils.runCql("DROP KEYSPACE IF EXISTS " + KEYSPACE);
            if (!nodeCdcEnabled) {
                setNodeCdcEnabled(true);
            }
            if (Schema.instance.getKeyspaceMetadata(KEYSPACE) != null) {
                Schema.instance.updateHandler.apply(schema -> schema.without(KEYSPACE), true);
            }
        }
    }

    private void assertCdcMetadataPrecondition() {
        try (CqlSession session = CqlSession.builder().build()) {
            final TableMetadata unmanagedTable = session.getMetadata().getKeyspace(KEYSPACE).orElseThrow()
                    .getTable(UNMANAGED_TABLE).orElseThrow();
            final TableMetadata managedTable = session.getMetadata().getKeyspace(KEYSPACE).orElseThrow()
                    .getTable(MANAGED_TABLE).orElseThrow();

            assertNull(unmanagedTable.getOptions().get(CDC));
            assertEquals("true", managedTable.getOptions().get(CDC).toString());
        }
    }

    private void setNodeCdcEnabled(boolean enabled) throws Exception {
        final var value = Boolean.toString(enabled);
        final var config = "/opt/cassandra/conf/cassandra.yaml";
        final var command = "sed 's/^cdc_enabled: .*/cdc_enabled: " + value + "/' " + config
                + " > /tmp/cassandra.yaml && cat /tmp/cassandra.yaml > " + config;
        final Container.ExecResult result = cassandra.execInContainer("sh", "-c", command);
        assertEquals(0, result.getExitCode(), result.getStdout() + result.getStderr());

        cassandra.getDockerClient().restartContainerCmd(cassandra.getContainerId()).exec();
        waitForCql();
    }
}
