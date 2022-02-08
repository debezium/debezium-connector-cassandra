/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.TestUtils.TEST_KEYSPACE_NAME;
import static io.debezium.connector.cassandra.TestUtils.createTestKeyspace;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.github.nosan.embedded.cassandra.Cassandra;
import com.github.nosan.embedded.cassandra.CassandraBuilder;
import com.github.nosan.embedded.cassandra.Version;
import com.github.nosan.embedded.cassandra.commons.ClassPathResource;

import io.debezium.config.Configuration;
import io.debezium.kafka.KafkaCluster;
import io.debezium.util.Testing;

public abstract class CassandraConnectorTestBase {

    public static final String EMBEDDED_CASSANDRA_VERSION = System.getProperty("cassandra.version", "3.11.10");

    public static Cassandra cassandra;
    private static Path cassandraDir;

    private KafkaCluster kafkaCluster;
    private File kafkaDataDir;

    @BeforeClass
    public static void setUpClass() {
        cassandraDir = Testing.Files.createTestingDirectory("embeddedCassandra").toPath();
        cassandra = getCassandra(cassandraDir, EMBEDDED_CASSANDRA_VERSION);
        cassandra.start();
        waitForCql();
        createTestKeyspace();
    }

    @AfterClass
    public static void tearDownClass() {
        destroyTestKeyspace();
        cassandra.stop();
        Testing.Files.delete(cassandraDir);
    }

    @Before
    public void beforeEach() throws Exception {
        kafkaDataDir = Testing.Files.createTestingDirectory("kafkaCluster");
        Testing.Files.delete(kafkaDataDir);
        kafkaCluster = new KafkaCluster().usingDirectory(kafkaDataDir)
                .deleteDataUponShutdown(true)
                .addBrokers(1)
                .withPorts(2181, 9092)
                .startup();
    }

    @After
    public void afterEach() {
        kafkaCluster.shutdown();
        Testing.Files.delete(kafkaDataDir);
    }

    public static void destroyTestKeyspace() {
        try (CqlSession session = CqlSession.builder().build()) {
            session.execute(SchemaBuilder.dropKeyspace(TEST_KEYSPACE_NAME).build());
        }
    }

    protected abstract CassandraConnectorContext generateTaskContext(Configuration configuration) throws Exception;

    protected CassandraConnectorContext generateTaskContext() throws Exception {
        return generateTaskContext(Configuration.from(TestUtils.generateDefaultConfigMap()));
    }

    protected CassandraConnectorContext generateTaskContext(Map<String, Object> configs) throws Exception {
        return generateTaskContext(Configuration.from(configs));
    }

    protected static Cassandra getCassandra(final Path cassandraHome, final String version) {
        return new CassandraBuilder().version(Version.parse(version))
                .jvmOptions("-Dcassandra.ring_delay_ms=1000", "-Xms1g", "-Xmx1g")
                .workingDirectory(() -> cassandraHome)
                .startupTimeout(Duration.ofMinutes(5))
                .configFile(new ClassPathResource("cassandra-unit.yaml")).build();
    }

    protected static void waitForCql() {
        await()
                .pollInterval(10, SECONDS)
                .pollInSameThread()
                .timeout(1, MINUTES)
                .until(() -> {
                    try (final CqlSession ignored = CqlSession.builder().build()) {
                        return true;
                    }
                    catch (Exception ex) {
                        return false;
                    }
                });
    }
}
