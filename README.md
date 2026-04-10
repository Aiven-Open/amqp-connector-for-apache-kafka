<!--
    Copyright 2026 Aiven Oy and project contributors

    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

    https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

    SPDX-License-Identifier: Apache-2
-->
Aiven connectors for AMQP on Apache Kafka
======================

[![Main checks](https://github.com/Aiven-Open/amqp-connector-for-apache-kafka/actions/workflows/Merge_check.yml/badge.svg)](https://github.com/Aiven-Open/salesforce-connector-for-apache-kafka/actions/workflows/Merge_check.yml)

This is an open source connector developed by Aiven that integrates with Apache Kafka and AMQP providers.  It provides a source connector.

Overview
========

## Features
 
 - A utility library to connect to and provide convenience methods for AMQP interactions. 
 - An Apache Kafka source connector for AMQP.

## Prerequisites

* Java 17
* Maven

## Documentation

Documentation for this project can be found at https://aiven-open.github.io/amqp-connector-for-apache-kafka.  Documentation can be generated from source by executing: `mvn site site:stage`.  The documentation will then be found in `/target/staging`.

This project is built upon [Aiven framework for connectors on Apache Kafka](https://github.com/Aiven-Open/aiven-kafka-connector-framework) and familuraity with the [framework documentation](httpd://aiven-open.github.io/aiven-kafka-connector-framework) is recommended.

License
============
AMQP Connector for Apache Kafka is licensed under the Apache License, version 2.0. Full license text is available in the [LICENSE](LICENSE) file.

Please note that the project explicitly does not require a CLA (Contributor License Agreement) from its contributors.

Contact
============
Bug reports and patches are very welcome, please post them as GitHub issues and pull requests at https://github.com/aiven-open/amqp-connector-for-apache-kafka 
To report any possible vulnerabilities or other serious issues please see our [security](SECURITY.md) policy.
