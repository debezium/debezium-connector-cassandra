[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.debezium/debezium-connector-cassandra/badge.svg)](http://search.maven.org/#search%7Cga%7C1%7Cg%3A%22io.debezium%22)
[![Build Status](https://github.com/debezium/debezium-connector-cassandra/workflows/Maven%20CI/badge.svg?branch=main)](https://github.com/debezium/debezium-connector-cassandra/actions)
[![User chat](https://img.shields.io/badge/chat-users-brightgreen.svg)](https://gitter.im/debezium/user)
[![Developer chat](https://img.shields.io/badge/chat-devs-brightgreen.svg)](https://gitter.im/debezium/dev)
[![Google Group](https://img.shields.io/:mailing%20list-debezium-brightgreen.svg)](https://groups.google.com/forum/#!forum/debezium)
[![Stack Overflow](http://img.shields.io/:stack%20overflow-debezium-brightgreen.svg)](http://stackoverflow.com/questions/tagged/debezium)

Copyright Debezium Authors.
Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).

# Debezium Connector for Apache Cassandra

Debezium is an open source project that provides a low latency data streaming platform for change data capture (CDC).

This repository contains incubating connector for Apache Cassandra which is in an **early stage of its development**.
You are encouraged to explore this connector and test it, but it is not recommended yet for production usage.

Please see the [README.md](https://github.com/debezium/debezium#building-debezium) in the main repository for general instructions on building Debezium from source (prerequisites, usage of Docker etc).

## Building the Cassandra 3.x connector

Building this connector first requires the main [debezium](https://github.com/debezium/debezium) code repository to be built locally using `mvn clean install`.

In order to build the Cassandra connector you'll need JDK 8 because Cassandra 3.x
doesn't support Java versions above Java 8. That also means dependencies like
`debezium-core` have to be built as Java 8 bytecode version 52.0 as well,
either by compiling it with Java 8 or specifying Java 8 bytecode generation
on newer versions of Java.

Then the Cassandra connector can be built like so:

    $ mvn clean install
    
If you have multiple Java installation on your machine you can select the correct
version by setting JAVA_HOME env var:

    $ JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64 mvn clean install

### Building just the artifacts, without running tests, CheckStyle, etc.

You can skip all non-essential plug-ins (tests, integration tests, CheckStyle, formatter, API compatibility check, etc.) using the "quick" build profile:

    $ mvn clean verify -Dquick

This provides the fastes way for solely producing the output artifacts, without running any of the QA related Maven plug-ins.

## Getting Started

For getting started please check the [tutorial example](https://github.com/debezium/debezium-examples/tree/master/tutorial#using-cassandra).
## Contributing

The Debezium community welcomes anyone that wants to help out in any way, whether that includes reporting problems, helping with documentation, or contributing code changes to fix bugs, add tests, or implement new features. See [this document](https://github.com/debezium/debezium/blob/master/CONTRIBUTE.md) for details.
