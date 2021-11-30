/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.IOException;
import java.time.Duration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An abstract processor designed to be a convenient superclass for all concrete processors for Cassandra
 * connector task. The class handles concurrency control for starting and stopping the processor.
 */
public abstract class AbstractProcessor {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractProcessor.class);

    private final String name;
    private final long delayMs;
    private boolean running;

    public AbstractProcessor(String name, Duration delay) {
        this.name = name;
        this.delayMs = delay.toMillis();
        this.running = false;
    }

    /**
     * The actual work the processor is doing. This method will be executed in a while loop
     * until processor stops or encounters exception.
     */
    public abstract void process() throws InterruptedException, IOException;

    /**
     * Override initialize to initialize resources before starting the processor
     */
    public void initialize() throws Exception {
    }

    /**
     * Override destroy to clean up resources after stopping the processor
     */
    public void destroy() throws Exception {
    }

    public boolean isRunning() {
        return running;
    }

    public void start() throws Exception {
        if (running) {
            LOGGER.warn("Ignoring start signal for {} because it is already started", name);
            return;
        }

        LOGGER.info("Started {}", name);
        running = true;
        while (isRunning()) {
            process();
            Thread.sleep(delayMs);
        }
        LOGGER.info("Stopped {}", name);
    }

    public void stop() {
        if (isRunning()) {
            LOGGER.info("Stopping {}", name);
            running = false;
        }
    }

    public String getName() {
        return name;
    }
}
