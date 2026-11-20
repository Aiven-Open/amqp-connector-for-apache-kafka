## v0.2.0
### What is changed
 
 - spotless apply
 - added logging to test
 - Merge pull request #60 from Aiven-Open/upgrade-maven-commons-v5
 - removed invalid respoitory
 - cleaned up poms
 - cleaned up versions and dependencies
 - adjusted dependencies
 - updated adopt to temurin
 - removed unused properties
 - update to aiven-commons v5
 - Merge pull request #62 from Aiven-Open/update-codeql-analysis
 - update codeql-analysis.yml
 - Merge pull request #52 from Aiven-Open/dependabot/maven/proton.version-1.4.0
 - Merge pull request #61 from Aiven-Open/fix-release_detail.sh
 - modified release detail to match other uses
 - Bump io.netty:netty-transport from 4.2.15.Final to 4.2.19.Final (#50)
 - Merge branch 'main' into dependabot/maven/proton.version-1.4.0
 - Bump com.fasterxml.jackson.core:jackson-databind from 2.22.3 to 2.22.4 (#51)
 - Bump io.netty:netty-transport-native-epoll (#53)
 - Bump proton.version from 1.3.0 to 1.4.0
 - Bump com.fasterxml.jackson.core:jackson-databind from 2.21.5 to 2.22.3 (#45)
 - Merge pull request #43 from Aiven-Open/EC-1447-impleent-AMQP-sink
 - updated documentation
 - updated documentation and removed specifc versions
 - Bump com.fasterxml.jackson.core:jackson-annotations from 2.21 to 2.22 (#47)
 - changed AmqpContext builder name
 - fixed typos
 - updates as per review
 - changes as per review
 - removed Java23 requirement from roundtrip tests
 - add missing versionMap
 - roundtrip cleanup
 - cleaned up maven build
 - chanaged to EncoderDecorder naming
 - Initial sink implementation
 - Bump proton.version from 1.2.0 to 1.3.0 (#40)
 - refactored source to provide data for sink
 - cleaed up code
 - added additional encoding tests
 - added spotless formatting
 - initial encoding update to source
 - Merge pull request #41 from Aiven-Open/dependabot/maven/org.cyclonedx-cyclonedx-maven-plugin-2.9.3
 - Merge pull request #42 from Aiven-Open/dependabot/maven/slf4j.version-2.0.18
 - Merge pull request #39 from Aiven-Open/dependabot/maven/org.spdx-spdx-maven-plugin-1.0.4
 - Merge pull request #38 from Aiven-Open/dependabot/maven/org.apache.maven.plugins-maven-jar-plugin-3.5.1
 - Bump slf4j.version from 2.0.17 to 2.0.18
 - Bump org.cyclonedx:cyclonedx-maven-plugin from 2.9.1 to 2.9.3
 - Bump org.spdx:spdx-maven-plugin from 1.0.3 to 1.0.4
 - Bump org.apache.maven.plugins:maven-jar-plugin from 3.5.0 to 3.5.1
 - Merge pull request #22 from Aiven-Open/dependabot/maven/org.apache.maven.doxia-doxia-site-renderer-2.1.0
 - Merge branch 'main' into dependabot/maven/org.apache.maven.doxia-doxia-site-renderer-2.1.0
 - Bump io.netty:netty-transport-native-epoll in /source (#36)
 - Merge pull request #35 from Aiven-Open/Issue_34_bump-proton-to-v1.2.0
 - fixed source documentation
 - fixed site build
 - fixed dependency
 - reverted accidental edit
 - Bump proton to v1.2.0
 - Merge pull request #23 from Aiven-Open/dependabot/maven/io.netty-netty-transport-native-epoll-4.2.12.Final
 - Merge pull request #1 from Aiven-Open/dependabot/maven/com.fasterxml.uuid-java-uuid-generator-5.2.0
 - Merge branch 'main' into dependabot/maven/io.netty-netty-transport-native-epoll-4.2.12.Final
 - Bump io.netty:netty-transport-native-epoll in /source (#30)
 - Merge branch 'main' into dependabot/maven/com.fasterxml.uuid-java-uuid-generator-5.2.0
 - Bump testcontainers.version from 2.0.4 to 2.0.5 (#24)
 - Bump com.fasterxml.jackson.core:jackson-databind from 2.21.2 to 2.21.5 (#33)
 - Merge pull request #31 from Aiven-Open/claudenw/framework-testing
 - changes as per review
 - fixed typo and style
 - removed dead code
 - fixed typo
 - updated aiven commons to version 4
 - add framework based integration tests
 - fixed pom
 - fixed integration tests and pom to exeucte them
 - test(amqp): align ITs with updated kafka-testkit & AMQP payload
 - site: fix source site images (#29)
 - Merge pull request #27 from Aiven-Open/fix-claude-warren-email
 - fix claude warren email address
 - update site branding and favicon behavior (#26)
 - site: customize maven site branding and favicon behavior
 - Bump io.netty:netty-transport-native-epoll
 - Bump org.apache.maven.doxia:doxia-site-renderer from 2.0.0 to 2.1.0
 - Merge branch 'main' into dependabot/maven/com.fasterxml.uuid-java-uuid-generator-5.2.0
 - Merge pull request #19 from Aiven-Open/update-documentation
 - change as per review
 - fixed RAT issue
 - updated documentation
 - Merge pull request #18 from Aiven-Open/release-0.1.0
 - Bump version to 0.2.0-SNAPSHOT
 - Release version 0.1.0
 - Merge pull request #17 from Aiven-Open/add-publish-profile
 - added publish profile
 - Merge pull request #16 from Aiven-Open/revert-14-release-0.1.0
 - Revert "Release version 0.1.0"
 - Merge pull request #14 from Aiven-Open/release-0.1.0
 - Bump version to 0.2.0-SNAPSHOT
 - Bump com.fasterxml.uuid:java-uuid-generator from 4.1.0 to 5.2.0
 
 
### Co-authored by
 
 - Aindriú Lavelle
 - Claude Warren
 - dependabot[bot]
 - Emmanuel Evbuomwan
 - github-actions[bot]
 
 
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
 
