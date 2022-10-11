/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.TestUtils.TEST_KEYSPACE_NAME;
import static io.debezium.connector.cassandra.TestUtils.TEST_KEYSPACE_NAME_2;
import static io.debezium.connector.cassandra.TestUtils.createTestKeyspace;
import static io.debezium.connector.cassandra.TestUtils.deleteTestKeyspaceTables;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;

import io.debezium.config.Configuration;
import io.debezium.util.Testing;

public abstract class CassandraConnectorTestBase {

    public static final String CASSANDRA_SERVER_DIR = "/var/lib/cassandra";
    private static final String cassandraDir = createCassandraDir();
    private static final String dockerDir = System.getProperty("docker.dir", "docker");
    private static final Consumer<CreateContainerCmd> cmd = e -> e.getHostConfig().withPortBindings(new PortBinding(Ports.Binding.bindPort(9042), new ExposedPort(9042)));

    @ClassRule
    public static GenericContainer cassandra = new GenericContainer(new ImageFromDockerfile().withFileFromPath(".", (new File(dockerDir)).toPath()))
            .withExposedPorts(9042)
            .withStartupTimeout(Duration.ofMinutes(3))
            .withCreateContainerCmdModifier(cmd)
            .withFileSystemBind(cassandraDir, CASSANDRA_SERVER_DIR, BindMode.READ_WRITE);

    @BeforeClass
    public static void setUpClass() throws Exception {
        waitForCql();
        createTestKeyspace();
    }

    @AfterClass
    public static void tearDownClass() throws IOException, InterruptedException {
        destroyTestKeyspace(TEST_KEYSPACE_NAME);
        destroyTestKeyspace(TEST_KEYSPACE_NAME_2);
        cassandra.stop();

        GenericContainer clenaup = new GenericContainer(new ImageFromDockerfile()
                .withDockerfileFromBuilder(builder -> builder
                        .from("eclipse-temurin:8-jre-focal")
                        .volume("/var/lib/cassandra")
                        .cmd("sleep", "10") // Give testcontainers some time to find out container is running.
                        .build()))
                                .withFileSystemBind(cassandraDir, CASSANDRA_SERVER_DIR, BindMode.READ_WRITE);
        clenaup.start();
        clenaup.execInContainer(
                "rm", "-rf",
                CASSANDRA_SERVER_DIR + "/data",
                CASSANDRA_SERVER_DIR + "/cdc_raw_directory",
                CASSANDRA_SERVER_DIR + "/commitlog",
                CASSANDRA_SERVER_DIR + "/hints",
                CASSANDRA_SERVER_DIR + "/saved_caches");
        clenaup.stop();
    }

    public static void destroyTestKeyspace() throws Exception {
        deleteTestKeyspaceTables(TEST_KEYSPACE_NAME);
    }

    public static void destroyTestKeyspace(String keyspace) {
        try (CqlSession session = CqlSession.builder().build()) {
            session.execute(SchemaBuilder.dropKeyspace(keyspace).ifExists().build());
        }
    }

    protected abstract CassandraConnectorContext generateTaskContext(Configuration configuration) throws Exception;

    protected CassandraConnectorContext generateTaskContext() throws Exception {
        return generateTaskContext(Configuration.from(TestUtils.generateDefaultConfigMap()));
    }

    protected CassandraConnectorContext generateTaskContext(Map<String, Object> configs) throws Exception {
        return generateTaskContext(Configuration.from(configs));
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

    protected static String createCassandraDir() {
        File cassandraDir = Testing.Files.createTestingDirectory("cassandra");
        // The directory will be bind-mounted into container where Cassandra runs under cassandra user.
        // Therefor we have to change permissions for all users so that Cassandra from container can write into this dir.
        Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rwxrwxrwx");
        try {
            Files.setPosixFilePermissions(cassandraDir.toPath(), permissions);
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return cassandraDir.toString();
    }
}
