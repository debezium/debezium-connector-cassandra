[![License](http://img.shields.io/:license-apache%202.0-brightgreen.svg)](http://www.apache.org/licenses/LICENSE-2.0.html)
[![Maven Central](https://maven-badges.herokuapp.com/maven-central/io.debezium/debezium-connector-reactor-cassandra/badge.svg)](https://search.maven.org/search?q=debezium%20connector%20cassandra)
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

## Cassandra compatibility

The following table list supported version of Apache Cassandra:


| Connector version | Apache Cassandra  |
|--|---|
| 1.9+ | Compatible with 3.x and 4.x version  |
| Up to 1.8 | Compatible with only 3.x  |

## Building the Debezium Cassandra connector

Please follow the steps described in the main repository to build the connector: [building-debezium](https://github.com/debezium/debezium#building-debezium)

## Getting Started

For getting started please check the [tutorial example](https://github.com/debezium/debezium-examples/tree/master/tutorial#using-cassandra).
## Contributing

The Debezium community welcomes anyone that wants to help out in any way, whether that includes reporting problems, helping with documentation, or contributing code changes to fix bugs, add tests, or implement new features. See [this document](https://github.com/debezium/debezium/blob/master/CONTRIBUTE.md) for details.
