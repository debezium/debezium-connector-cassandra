/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.time.Duration;

import io.debezium.util.Clock;
import io.debezium.util.ElapsedTimeStrategy;

/**
 * This policy determines how frequently the offset is flushed to disk.
 *
 * Periodic means that the offset is flushed to disk periodically according to the offset flush interval.
 * An interval of 1000 ms that the latest offset will only be flushed to disk if the amount time that has
 * passed since the last flush is at least 1000 ms.
 *
 * Always means that the offset if flushed to disk every time a record is processed.
 */
public interface OffsetFlushPolicy {
    boolean shouldFlush();

    static OffsetFlushPolicy always() {
        return new AlwaysFlushOffsetPolicy();
    }

    static OffsetFlushPolicy periodic(Duration offsetFlushInterval, long maxOffsetFlushSize) {
        return new PeriodicFlushOffsetPolicy(offsetFlushInterval, maxOffsetFlushSize);
    }

    class PeriodicFlushOffsetPolicy implements OffsetFlushPolicy {
        private final long maxOffsetFlushSize;
        private long unflushedRecordCount;
        private final ElapsedTimeStrategy elapsedTimeStrategy;

        PeriodicFlushOffsetPolicy(Duration offsetFlushInterval, long maxOffsetFlushSize) {
            this.maxOffsetFlushSize = maxOffsetFlushSize;
            this.unflushedRecordCount = 0;
            this.elapsedTimeStrategy = ElapsedTimeStrategy.constant(Clock.system(), offsetFlushInterval);
        }

        @Override
        public boolean shouldFlush() {
            if (unflushedRecordCount >= this.maxOffsetFlushSize) {
                clear();
                return true;
            }
            if (elapsedTimeStrategy.hasElapsed()) {
                clear();
                return true;
            }
            unflushedRecordCount += 1;
            return false;
        }

        private void clear() {
            unflushedRecordCount = 0;
        }
    }

    class AlwaysFlushOffsetPolicy implements OffsetFlushPolicy {

        @Override
        public boolean shouldFlush() {
            return true;
        }
    }
}
