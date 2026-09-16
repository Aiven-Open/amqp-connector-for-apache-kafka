package io.aiven.kafka.connect.amqp.sink.config;

import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;
import io.aiven.commons.kafka.connector.common.config.ConnectorCommonConfig;
import io.aiven.kafka.connect.amqp.common.config.AmqpCommonConfig;
import io.aiven.kafka.connect.amqp.common.config.AmqpFragment;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.Sender;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

public class AmqpSinkConfig extends ConnectorCommonConfig implements AmqpCommonConfig {
  private final AmqpFragment amqpFragment;

  /**
   * Constructor.
   *
   * @param originals the initial configuration data.
   */
  public AmqpSinkConfig(Map<String, String> originals) {
    super(new AmqpSinkConfigDef(), originals);
    FragmentDataAccess dataAccess = FragmentDataAccess.from(this);
    amqpFragment = new AmqpFragment(dataAccess);
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
}
