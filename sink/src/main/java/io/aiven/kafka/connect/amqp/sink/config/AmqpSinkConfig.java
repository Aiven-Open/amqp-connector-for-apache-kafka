package io.aiven.kafka.connect.amqp.sink.config;

import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;
import io.aiven.commons.kafka.connector.common.config.ConnectorCommonConfig;
import io.aiven.kafka.connect.amqp.common.config.AmqpCommonConfig;
import io.aiven.kafka.connect.amqp.common.config.AmqpFormat;
import io.aiven.kafka.connect.amqp.common.config.AmqpFragment;

import java.text.MessageFormat;
import java.util.Collections;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.exceptions.ClientException;
import org.slf4j.LoggerFactory;

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
   * Called directly after user configs got parsed (and thus default values got set).
   * This allows to change default values for "secondary defaults" if required.
   *
   * @param parsedValues unmodifiable map of current configuration
   * @return a map of updates that should be applied to the configuration (will be validated to prevent bad updates)
   */
  protected void fragmentPostProcess(ChangeTrackingMap parsedValues) {
    super.fragmentPostProcess(parsedValues);
    if (parsedValues.get(AmqpFragment.FORMAT).toString().equalsIgnoreCase(AmqpFormat.RAW.name()) &&
            !parsedValues.get(AmqpSinkConfigDef.STRATEGY).toString().equalsIgnoreCase(AmqpStrategy.RAW.name())) {
      LoggerFactory.getLogger(AmqpSinkConfig.class).warn(
              "{} must be set to '{}' when the message format is set to '{}'.  Making corrections",
              AmqpSinkConfigDef.STRATEGY, AmqpStrategy.RAW, AmqpFormat.RAW);
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

  public AmqpStrategy getWriteStrategy() {
    return AmqpStrategy.valueOf(dataAccess.getString(AmqpSinkConfigDef.STRATEGY).toUpperCase(Locale.ROOT));
  }
}
