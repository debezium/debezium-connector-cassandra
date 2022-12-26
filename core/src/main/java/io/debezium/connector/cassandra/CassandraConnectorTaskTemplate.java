/*
 * Copyright Debezium Authors.
 *
 * Licensed under the Apache Software License version 2.0, available at http://www.apache.org/licenses/LICENSE-2.0
 */
package io.debezium.connector.cassandra;

import java.io.FileInputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.health.HealthCheckRegistry;
import com.codahale.metrics.jmx.JmxReporter;
import com.codahale.metrics.servlets.HealthCheckServlet;
import com.codahale.metrics.servlets.MetricsServlet;
import com.codahale.metrics.servlets.PingServlet;

import io.debezium.config.Configuration;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorConfigException;
import io.debezium.connector.cassandra.exceptions.CassandraConnectorTaskException;
import io.debezium.connector.cassandra.network.BuildInfoServlet;
import io.debezium.connector.cassandra.transforms.CassandraTypeDeserializer;

public class CassandraConnectorTaskTemplate {

    private static final Logger LOGGER = LoggerFactory.getLogger(CassandraConnectorTaskTemplate.class);

    public static final MetricRegistry METRIC_REGISTRY_INSTANCE = new MetricRegistry();

    private final CassandraConnectorConfig config;
    private final CassandraTypeProvider deserializerProvider;
    private CassandraConnectorContext taskContext;
    private ProcessorGroup processorGroup;
    private Server httpServer;
    private JmxReporter jmxReporter;
    private final SchemaLoader schemaLoader;
    private final SchemaChangeListenerProvider schemaChangeListenerProvider;
    private final CassandraSpecificProcessors cassandraSpecificProcessors;
    private final ComponentFactory factory;

    public static void main(String[] args,
                            Function<CassandraConnectorConfig, CassandraConnectorTaskTemplate> template)
            throws Exception {
        if (args.length == 0) {
            throw new CassandraConnectorConfigException("CDC config file is required");
        }

        try (FileInputStream fis = new FileInputStream(args[0])) {
            CassandraConnectorConfig config = new CassandraConnectorConfig(Configuration.load(fis));
            template.apply(config).run();
        }
    }

    public CassandraConnectorTaskTemplate(CassandraConnectorConfig config,
                                          CassandraTypeProvider deserializerProvider,
                                          SchemaLoader schemaLoader,
                                          SchemaChangeListenerProvider schemaChangeListener,
                                          CassandraSpecificProcessors cassandraSpecificProcessors,
                                          ComponentFactory factory) {
        this.config = config;
        this.deserializerProvider = deserializerProvider;
        this.schemaLoader = schemaLoader;
        this.schemaChangeListenerProvider = schemaChangeListener;
        this.cassandraSpecificProcessors = cassandraSpecificProcessors;
        this.factory = factory;
    }

    private void initJmxReporter(String domain) {
        jmxReporter = JmxReporter.forRegistry(METRIC_REGISTRY_INSTANCE).inDomain(domain).build();
    }

    private HealthCheckRegistry registerHealthCheck() {
        CassandraConnectorTaskHealthCheck healthCheck = new CassandraConnectorTaskHealthCheck(processorGroup, taskContext.getCassandraClient());
        HealthCheckRegistry healthCheckRegistry = new HealthCheckRegistry();
        healthCheckRegistry.register("cassandra-cdc-health-check", healthCheck);
        return healthCheckRegistry;
    }

    private static Map<String, String> getBuildInfoMap(Class<?> clazz) {
        Map<String, String> buildInfo = new HashMap<>();
        buildInfo.put("version", clazz.getPackage().getImplementationVersion());
        buildInfo.put("service_name", clazz.getPackage().getImplementationTitle());
        return buildInfo;
    }

    public void start() throws Exception {
        if (!config.validateAndRecord(config.getValidationFieldSet(), LOGGER::error)) {
            throw new CassandraConnectorConfigException("Failed to start connector with invalid configuration " +
                    "(see logs for actual errors)");
        }

        LOGGER.info("Initializing Cassandra type deserializer ...");
        initDeserializer();

        LOGGER.info("Initializing Cassandra connector task context ...");
        taskContext = new CassandraConnectorContext(config, schemaLoader, schemaChangeListenerProvider, factory.offsetWriter(config));

        LOGGER.info("Starting processor group ...");
        AbstractProcessor[] processors = cassandraSpecificProcessors.getProcessors(taskContext);
        processorGroup = initProcessorGroup(taskContext, factory.recordEmitter(taskContext), processors);
        processorGroup.start();

        LOGGER.info("Starting HTTP server ...");
        initHttpServer();
        httpServer.start();

        LOGGER.info("Starting JMX reporter ...");
        initJmxReporter(config.getLogicalName());
        jmxReporter.start();
    }

    private void run() throws Exception {
        try {
            start();
            while (processorGroup.isRunning()) {
                Thread.sleep(1000);
            }
        }
        finally {
            stopAll();
        }
    }

    public CassandraConnectorContext getTaskContext() {
        return taskContext;
    }

    private void initDeserializer() {
        CassandraTypeDeserializer.init(deserializerProvider.deserializers(), config.getDecimalMode(),
                config.getVarIntMode(), deserializerProvider.baseTypeForReversedType());
    }

    protected ProcessorGroup initProcessorGroup(CassandraConnectorContext taskContext, Emitter recordEmitter,
                                                AbstractProcessor... cassandraSpecificProcessors) {
        try {
            ProcessorGroup processorGroup = new ProcessorGroup();

            for (AbstractProcessor processor : cassandraSpecificProcessors) {
                processorGroup.addProcessor(processor);
            }

            processorGroup.addProcessor(new SnapshotProcessor(taskContext, deserializerProvider.getClusterName()));
            List<ChangeEventQueue<Event>> queues = taskContext.getQueues();
            for (int i = 0; i < queues.size(); i++) {
                processorGroup.addProcessor(new QueueProcessor(taskContext, i, recordEmitter));
            }
            if (taskContext.getCassandraConnectorConfig().postProcessEnabled()) {
                processorGroup.addProcessor(new CommitLogPostProcessor(taskContext.getCassandraConnectorConfig()));
            }
            LOGGER.info("Initialized Processor Group.");
            return processorGroup;
        }
        catch (Exception e) {
            throw new CassandraConnectorTaskException("Failed to initialize Processor Group.", e);
        }
    }

    private void initHttpServer() {
        int httpPort = config.httpPort();
        LOGGER.debug("HTTP port is {}", httpPort);
        httpServer = new Server(httpPort);

        ServletContextHandler contextHandler = new ServletContextHandler(ServletContextHandler.SESSIONS);
        contextHandler.setContextPath("/");
        httpServer.setHandler(contextHandler);

        contextHandler.addServlet(new ServletHolder(new PingServlet()), "/ping");
        contextHandler.addServlet(new ServletHolder(new BuildInfoServlet(getBuildInfoMap(this.getClass()))), "/buildinfo");
        contextHandler.addServlet(new ServletHolder(new MetricsServlet(METRIC_REGISTRY_INSTANCE)), "/metrics");
        contextHandler.addServlet(new ServletHolder(new HealthCheckServlet(registerHealthCheck())), "/health");
    }

    public void stopAll() throws Exception {
        LOGGER.info("Stopping Cassandra Connector Task ...");
        if (processorGroup != null) {
            processorGroup.terminate();
            LOGGER.info("Terminated processor group.");
        }

        if (httpServer != null) {
            httpServer.stop();
            LOGGER.info("Stopped HTTP server.");
        }

        if (jmxReporter != null) {
            jmxReporter.stop();
            LOGGER.info("Stopped JMX reporter.");
        }

        if (taskContext != null) {
            taskContext.cleanUp();
            LOGGER.info("Cleaned up Cassandra connector context.");
        }
        LOGGER.info("Stopped Cassandra Connector Task.");
    }

    /**
     * A processor group consist of one or more processors; each processor will be running on a separate thread.
     * The processors are interdependent of one another: if one of the processors is stopped, all other processors
     * will be signaled to stop as well.
     */
    public static class ProcessorGroup {

        private static final Logger LOGGER = LoggerFactory.getLogger(ProcessorGroup.class);

        private final Set<AbstractProcessor> processors;
        private ExecutorService executorService;

        ProcessorGroup() {
            this.processors = new HashSet<>();
        }

        public boolean isRunning() {
            for (AbstractProcessor processor : processors) {
                if (!processor.isRunning()) {
                    return false;
                }
            }
            return true;
        }

        void addProcessor(AbstractProcessor processor) {
            processors.add(processor);
        }

        void start() {
            for (AbstractProcessor processor : processors) {
                try {
                    processor.initialize();
                }
                catch (Exception e) {
                    throw new CassandraConnectorTaskException("Failed to initialize processors", e);
                }
            }
            executorService = Executors.newFixedThreadPool(processors.size());
            for (AbstractProcessor processor : processors) {
                Runnable runnable = () -> {
                    try {
                        processor.start();
                    }
                    catch (Exception e) {
                        LOGGER.error("Encountered exception while running {}; stopping all processors.", processor.getName(), e);
                        try {
                            stopProcessors();
                        }
                        catch (Exception e2) {
                            LOGGER.error("Encountered exceptions while stopping all processors", e2);
                        }
                    }
                };
                executorService.submit(runnable);
            }
        }

        void terminate() {
            LOGGER.info("Terminating processor group ...");
            try {
                stopProcessors();
                if (executorService != null && !executorService.isShutdown()) {
                    executorService.shutdown();
                    if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                        executorService.shutdownNow();
                    }
                }
            }
            catch (Exception e) {
                throw new CassandraConnectorTaskException("Failed to terminate processor group.", e);
            }
        }

        private void stopProcessors() throws Exception {
            for (AbstractProcessor processor : processors) {
                processor.stop();
                processor.destroy();
            }
        }
    }
}
