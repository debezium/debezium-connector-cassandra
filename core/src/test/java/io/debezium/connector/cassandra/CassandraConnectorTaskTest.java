/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.CassandraConnectorTaskTemplate.ProcessorGroup;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.pipeline.ErrorHandler;

class CassandraConnectorTaskTest {

    @Test
    @Timeout(60)
    void testProcessorGroup() throws Exception {
        ProcessorGroup processorGroup = new ProcessorGroup();
        AtomicInteger running = new AtomicInteger(0);
        AtomicInteger iteration = new AtomicInteger(0);
        AbstractProcessor processor1 = new AbstractProcessor("processor1", Duration.ofMillis(100)) {
            @Override
            public void initialize() {
                running.incrementAndGet();
            }

            @Override
            public void destroy() {
                running.decrementAndGet();
            }

            @Override
            public void process() {
                iteration.incrementAndGet();
            }
        };
        AbstractProcessor processor2 = new AbstractProcessor("processor2", Duration.ofMillis(100)) {
            @Override
            public void initialize() {
                running.incrementAndGet();
            }

            @Override
            public void destroy() {
                running.decrementAndGet();
            }

            @Override
            public void process() {
                iteration.incrementAndGet();
            }
        };

        processorGroup.addProcessor(processor1);
        processorGroup.addProcessor(processor2);
        processorGroup.start();
        while (!processor1.isRunning() || !processor2.isRunning()) {
            Thread.sleep(100);
        }
        assertTrue(processorGroup.isRunning());
        assertEquals(2, running.get());
        assertTrue(iteration.get() >= 1);

        processorGroup.terminate();
        assertFalse(processor1.isRunning());
        assertFalse(processor2.isRunning());
        assertEquals(0, running.get());
    }

    @Test
    @Timeout(60)
    void testProcessorFailureIsReportedToErrorHandler() throws Exception {
        Configuration configuration = Configuration.empty()
                .edit()
                .with(CassandraConnectorConfig.TOPIC_PREFIX, "someconnector")
                .build();
        CassandraConnectorConfig config = new CassandraConnectorConfig(configuration);
        ChangeEventQueue<DataChangeEvent> queue = new ChangeEventQueue.Builder<DataChangeEvent>()
                .pollInterval(config.pollInterval())
                .maxBatchSize(config.maxBatchSize())
                .maxQueueSize(config.maxQueueSize())
                .loggingContextSupplier(() -> null)
                .build();
        ErrorHandler errorHandler = new ErrorHandler(AbstractSourceConnector.class, config, queue, null);

        ProcessorGroup processorGroup = new ProcessorGroup();
        processorGroup.setErrorHandler(errorHandler);
        processorGroup.addProcessor(new AbstractProcessor("failing", Duration.ofMillis(100)) {
            @Override
            public void process() {
                throw new IllegalStateException("failing processor");
            }
        });

        processorGroup.start();

        // The failure is raised on an executor thread, so it has to be reported for the task thread
        // to ever learn about it. Without that the group is just stopped and nothing else happens.
        while (errorHandler.getProducerThrowable() == null) {
            Thread.sleep(100);
        }
        assertEquals("failing processor", errorHandler.getProducerThrowable().getMessage());

        processorGroup.terminate();
    }
}
