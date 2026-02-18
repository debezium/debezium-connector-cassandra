/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.Set;
import java.util.function.Consumer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.images.builder.ImageFromDockerfile;
import org.testcontainers.junit.jupiter.Testcontainers;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.github.dockerjava.api.command.CreateContainerCmd;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.PortBinding;
import com.github.dockerjava.api.model.Ports;
import com.sun.security.auth.module.UnixSystem;

import io.debezium.connector.cassandra.spi.CassandraTestProvider;
import io.debezium.connector.cassandra.utils.TestUtils;
import io.debezium.util.Testing;

@Testcontainers
public abstract class CassandraConnectorTestBase {

    public static final String CLUSTER_NAME = "Test Cluster";
    public static final String CASSANDRA_SERVER_DIR = "/var/lib/cassandra";
    private static final String cassandraDir = createCassandraDir();
    private static final String dockerDir = System.getProperty("docker.dir", "docker");
    private static final Consumer<CreateContainerCmd> cmd = createCmd -> createCmd.getHostConfig()
            .withPortBindings(new PortBinding(Ports.Binding.bindPort(9042), new ExposedPort(9042)));

    protected CassandraConnectorContext context;
    protected CassandraTestProvider provider;

    @org.testcontainers.junit.jupiter.Container
    static GenericContainer cassandra = new GenericContainer(new ImageFromDockerfile().withFileFromPath(".", (new File(dockerDir)).toPath()))
            .withExposedPorts(9042)
            .withStartupTimeout(Duration.ofMinutes(3))
            .withCreateContainerCmdModifier(cmd)
            .withFileSystemBind(cassandraDir, CASSANDRA_SERVER_DIR, BindMode.READ_WRITE)
            .withCommand("-Dcassandra.ring_delay_ms=5000 -Dcassandra.superuser_setup_delay_ms=1000");

    @BeforeAll
    static void setUpClass() throws Exception {
        waitForCql();
        TestUtils.createTestKeyspace();
    }

    @AfterAll
    static void tearDownClass() throws Throwable {
        Container.ExecResult rm = cassandra.execInContainer("rm",
                "-rf",
                "/var/lib/cassandra/hints",
                "/var/lib/cassandra/data",
                "/var/lib/cassandra/metadata",
                "/var/lib/cassandra/commitlog",
                "/var/lib/cassandra/cdc_raw",
                "/var/lib/cassandra/saved_caches");
        String owner = resolveHostOwner();
        if (owner != null) {
            cassandra.execInContainer("sh", "-c", "chown -R " + owner + " " + CASSANDRA_SERVER_DIR + " || true");
        }
        cassandra.stop();
    }

    public static void destroyTestKeyspace() throws Exception {
        TestUtils.deleteTestKeyspaceTables(TestUtils.TEST_KEYSPACE_NAME);
    }

    public static void destroyTestKeyspace(String keyspace) {
        try (CqlSession session = CqlSession.builder().build()) {
            session.execute(SchemaBuilder.dropKeyspace(keyspace).ifExists().build());
        }
    }

    protected static void waitForCql() {
        await()
                .pollInterval(1, SECONDS)
                .pollInSameThread()
                .timeout(1, MINUTES)
                .until(() -> {
                    try (CqlSession ignored = CqlSession.builder().build()) {
                        return true;
                    }
                    catch (Exception ex) {
                        return false;
                    }
                });
    }

    protected static String createCassandraDir(String path) {
        try {
            File cassandraDir = Testing.Files.createTestingDirectory(path, false);
            // The directory will be bind-mounted into container where Cassandra runs under
            // cassandra user. Therefore we have to change permissions for all users so that
            // Cassandra from container can write into this dir.
            if (System.getProperty("os.name").toLowerCase().contains("linux")) {
                Set<PosixFilePermission> permissions = PosixFilePermissions.fromString("rwxrwxrwx");
                Files.setPosixFilePermissions(cassandraDir.toPath(), permissions);
            }
            return cassandraDir.toString();
        }
        catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static String createCassandraDir() {
        String cassandraDir = createCassandraDir("cassandra");
        createCassandraDir("cassandra/hints");
        createCassandraDir("cassandra/data");
        createCassandraDir("cassandra/metadata");
        createCassandraDir("cassandra/commitlog");
        createCassandraDir("cassandra/cdc_raw");
        createCassandraDir("cassandra/saved_caches");
        return cassandraDir;
    }

    private static String resolveHostOwner() {
        if (!System.getProperty("os.name").toLowerCase().contains("linux")) {
            return null;
        }
        try {
            UnixSystem unixSystem = new UnixSystem();
            return unixSystem.getUid() + ":" + unixSystem.getGid();
        }
        catch (Exception ignored) {
            return null;
        }
    }
}
