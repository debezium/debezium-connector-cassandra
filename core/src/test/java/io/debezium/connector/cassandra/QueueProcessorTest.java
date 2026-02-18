/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static io.debezium.connector.cassandra.utils.TestUtils.generateDefaultConfigMap;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;

import io.debezium.config.Configuration;

class QueueProcessorTest {

    @Test
    void shouldMoveCommitLogToErrorFolderWhenFlushMarksItErroneous() throws Exception {
        CassandraConnectorConfig config = new CassandraConnectorConfig(Configuration.from(generateDefaultConfigMap()));
        DefaultCassandraConnectorContext context = new DefaultCassandraConnectorContext(config);
        String commitLogFileName = "CommitLog-6-123.log";
        Emitter emitter = new Emitter() {
            @Override
            public void emit(Record record) {
            }

            @Override
            public void flush() {
                context.getErroneousCommitLogs().add(commitLogFileName);
            }

            @Override
            public void close() {
            }
        };
        QueueProcessor processor = new QueueProcessor(context, 0, emitter);

        Path commitLogDir = Files.createTempDirectory("dbz-1610-commitlog");
        File commitLog = commitLogDir.resolve(commitLogFileName).toFile();
        assertTrue(commitLog.createNewFile());

        try {
            processor.initialize();
            context.getQueues().get(0).enqueue(new EOFEvent(commitLog));
            processor.process();

            Path errorFile = Path.of(config.commitLogRelocationDir(), QueueProcessor.ERROR_FOLDER, commitLogFileName);
            assertTrue(Files.exists(errorFile));
        }
        finally {
            context.cleanUp();
            Files.deleteIfExists(commitLog.toPath());
            Files.deleteIfExists(commitLogDir);
        }
    }
}
