/*
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

       SPDX-License-Identifier: Apache-2.0
*/
package io.aiven.kafka.connect.amqp.source;

import io.aiven.commons.kafka.connector.source.AbstractSourceTask;
import io.aiven.commons.kafka.connector.source.EvolvingSourceRecordIterator;
import io.aiven.commons.kafka.connector.source.OffsetManager;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.kafka.connect.amqp.source.config.AmqpSourceConfig;
import java.util.Map;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The AMQP source task. */
public final class AmqpSourceTask extends AbstractSourceTask {
  /** The logger to write to */
  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpSourceTask.class);

  private AmqpSourceConfig amqpSourceConfig;
  private AmqpSourceData amqpSourceData;

  @Override
  protected AmqpSourceConfig configure(Map<String, String> props, OffsetManager offsetManager) {
    LOGGER.info("AMQP Source task started.");
    this.amqpSourceConfig = new AmqpSourceConfig(props);
    try {
      this.amqpSourceData = new AmqpSourceData(amqpSourceConfig, offsetManager);
    } catch (ClientException e) {
      throw new RuntimeException(e);
    }
    return amqpSourceConfig;
  }

  @Override
  protected EvolvingSourceRecordIterator getIterator(SourceCommonConfig config) {
    return new EvolvingSourceRecordIterator(config, amqpSourceData);
  }

  @Override
  protected void closeResources() {
    try {
      amqpSourceData.close();
    } catch (Exception e) {
      LOGGER.error("Unable to close amqpSourceData: {}", e.getMessage(), e);
    }
  }

  @Override
  public String version() {
    return AmqpVersionInfo.VERSION;
  }
}
