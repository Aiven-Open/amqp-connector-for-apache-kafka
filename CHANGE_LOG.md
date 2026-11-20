## v0.2.0
### What is changed

- Add enforcer plugin
- add framework based integration tests
- add missing versionMap
- Bump com.diffplug.spotless:spotless-maven-plugin from 3.5.1 to 3.9.0
- Bump com.fasterxml.jackson.core:jackson-annotations from 2.21 to 2.22
- Bump com.fasterxml.jackson.core:jackson-databind from 2.21.2 to 2.22.4
- Bump com.fasterxml.uuid:java-uuid-generator from 4.1.0 to 5.2.0
- Bump com.github.luben:zstd-jni from 1.5.7-7 to 1.5.7-12
- Bump com.github.spotbugs:spotbugs-maven-plugin from 4.9.8.3 to 4.10.3.0
- Bump com.google.cloud:libraries-bom from 26.83.0 to 26.86.0
- Bump com.google.guava:guava from 32.0.1-jre to 33.7.1-jre
- Bump com.google.re2j:re2j from 1.7 to 1.8
- Bump io.aiven.commons:aiven-commons from 3 to 5
- Bump io.aiven.commons:common-util from 0.2.0 to 0.3.0
- Bump io.aiven.commons:framework-for-connectors from 0.3.0 to 0.4.0
- Bump io.aiven.commons:kafka-config form 0.2.0 to 0.3.0
- Bump junit.version from 6.1.0 to 6.1.3
- Bump org.apache.commons:commons-lang3 from 3.17.0 to 3.20.0
- Bump org.apache.maven.doxia:doxia-site-renderer from 2.0.0 to 2.1.0
- Bump org.apache.maven.plugins:maven-jar-plugin from 3.5.0 to 3.5.1
- Bump org.apache.maven.plugins:maven-site-plugin from 3.21.0 to 3.22.0
- Bump org.apache.parquet:parquet-avro from 1.17.0 to 1.17.1
- Bump org.cyclonedx:cyclonedx-maven-plugin from 2.9.1 to 2.9.3
- Bump org.slf4j:slf4j-api from 2.0.11 to 2.0.19
- Bump org.spdx:spdx-maven-plugin from 1.0.3 to 1.0.4
- Bump plugin.surefire from 3.5.5 to 3.5.6
- Bump proton.version from 1.0.0 to 1.4.0
- Bump testcontainers.version from 2.0.4 to 2.0.5
- changed AmqpContext builder name
- changed to Encoder/Decorder naming
- cleaned up poms
- cleaned up versions and dependencies
- implemented AMQP Sink
- refactored source to provide data for sink
- removed Java23 requirement from roundtrip tests
- removed unused properties
- replace 'adopt' with 'temurin'
- site: customize maven site branding and favicon behavior
- update codeql-analysis.yml
- updated documentation
 
### Co-authored by
 
 - Aindriú Lavelle
 - Claude Warren
 - Emmanuel Evbuomwan
 
 
### Full Changelog
https://github.com/Aiven-Open/amqp-connector-for-apache-kafka/compare/v0.1.0...vv0.2.0
 
## v0.1.0
### Initial Release
 
 - based on framework to 0.2.0
 - Ensure that the receiver is correctly closed on stop
 - Add assembly to package a zip and tar of the connector
 - removed recordcount from offset manager key
 - fixed body serialization issue
 - fixed primary key storage in offset manager
 - Bump com.rabbitmq:amqp-client from 5.26.0 to 5.29.0
 - fixed uncaught number format exception and broken test (#5)
 - update readme (#3)
 - Fix for Potential input resource leak (#4)
 - added spdx sbom
 - added sbom
 
 
### Co-authored by
 
 - Aindriú Lavelle
 - Claude Warren
 
 
### Full Changelog
https://github.com/Aiven-Open/amqp-connector-for-apache-kafka/compare/4494ca45ca9ca9f7ff9c9bdd95e43654fed387a0...v0.1.0
 
