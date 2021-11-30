/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.util.List;
import java.util.stream.Collectors;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.metadata.schema.SchemaChangeListenerBase;
import com.datastax.oss.driver.api.core.metadata.schema.TableMetadata;
import com.datastax.oss.driver.api.core.session.Session;

import io.debezium.connector.SourceInfoStructMaker;

public class AbstractSchemaChangeListener extends SchemaChangeListenerBase {

    protected final String kafkaTopicPrefix;
    protected final SourceInfoStructMaker<SourceInfo> sourceInfoStructMaker;
    protected final SchemaHolder schemaHolder;

    public AbstractSchemaChangeListener(String kafkaTopicPrefix,
                                        SourceInfoStructMaker<SourceInfo> sourceInfoStructMaker,
                                        SchemaHolder schemaHolder) {
        this.kafkaTopicPrefix = kafkaTopicPrefix;
        this.sourceInfoStructMaker = sourceInfoStructMaker;
        this.schemaHolder = schemaHolder;
    }

    public List<TableMetadata> getCdcEnabledTableMetadataList(final Session session) {
        return session.getMetadata()
                .getKeyspaces()
                .values()
                .stream()
                .flatMap(kmd -> kmd.getTables().values().stream())
                .filter(tm -> tm.getOptions().get(CqlIdentifier.fromCql("cdc")).toString().equals("true"))
                .collect(Collectors.toList());
    }

    public SchemaHolder getSchemaHolder() {
        return schemaHolder;
    }
}
