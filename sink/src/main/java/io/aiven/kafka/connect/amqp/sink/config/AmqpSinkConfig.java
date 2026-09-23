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
package io.aiven.kafka.connect.amqp.sink.config;

import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;
import io.aiven.commons.kafka.connector.common.config.ConnectorCommonConfig;
import io.aiven.kafka.connect.amqp.common.config.AmqpCommonConfig;
import io.aiven.kafka.connect.amqp.common.config.AmqpFormat;
import io.aiven.kafka.connect.amqp.common.config.AmqpFragment;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.slf4j.LoggerFactory;

/** The AMQP Sink configuration. */
public class AmqpSinkConfig extends ConnectorCommonConfig implements AmqpCommonConfig {
  private final AmqpFragment amqpFragment;
  private final FragmentDataAccess dataAccess;

  /**
   * Constructor.
   *
   * @param originals the initial configuration data.
   */
  public AmqpSinkConfig(Map<String, String> originals) {
    super(new AmqpSinkConfigDef(), originals);
    dataAccess = FragmentDataAccess.from(this);
    amqpFragment = new AmqpFragment(dataAccess);
  }

  /**
   * Called directly after user configs got parsed (and thus default values got set). This allows to
   * change default values for "secondary defaults" if required.
   *
   * @param parsedValues unmodifiable map of current configuration
   */
  protected void fragmentPostProcess(ChangeTrackingMap parsedValues) {
    super.fragmentPostProcess(parsedValues);
    if (parsedValues.get(AmqpFragment.FORMAT).toString().equalsIgnoreCase(AmqpFormat.RAW.name())
        && !parsedValues
            .get(AmqpSinkConfigDef.STRATEGY)
            .toString()
            .equalsIgnoreCase(AmqpStrategy.RAW.name())) {
      LoggerFactory.getLogger(AmqpSinkConfig.class)
          .warn(
              "{} must be set to '{}' when the message format is set to '{}'.  Making corrections",
              AmqpSinkConfigDef.STRATEGY,
              AmqpStrategy.RAW,
              AmqpFormat.RAW);
      parsedValues.override(AmqpSinkConfigDef.STRATEGY, AmqpStrategy.RAW.name());
    }
  }

  @Override
  public Receiver getReceiver(Connection connection)
      throws ClientException, ExecutionException, InterruptedException {
    return amqpFragment.getReceiver(connection);
  }

  @Override
  public Client getClient() {
    return amqpFragment.getClient();
  }

  @Override
  public Connection getConnection(Client client) throws ClientException {
    return amqpFragment.getConnection(client);
  }

  @Override
  public Sender getSender(Connection connection)
      throws ClientException, ExecutionException, InterruptedException {
    return amqpFragment.getSender(connection);
  }

  @Override
  public AmqpFormat getMessageFormat() {
    return amqpFragment.getMessageFormat();
  }

  /**
   * Gets the write strategy for this sink.
   *
   * @return the Write strategy for this sink.
   */
  public AmqpStrategy getWriteStrategy() {
    return AmqpStrategy.valueOf(
        dataAccess.getString(AmqpSinkConfigDef.STRATEGY).toUpperCase(Locale.ROOT));
  }
}
