package io.aiven.kafka.connect.amqp.source.config;

import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.transformer.Transformer;
import io.aiven.kafka.connect.amqp.common.config.AmqpCommonConfig;
import io.aiven.kafka.connect.amqp.common.config.AmqpFragment;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

import java.util.Map;

public final class AmqpSourceConfig extends SourceCommonConfig implements AmqpCommonConfig {
	private final AmqpFragment amqpFragment;
	/**
	 * Constructor.
	 *
	 * @param originals
	 *            the initial configuration data.
	 */
	public AmqpSourceConfig(Map<String, String> originals) {
		super(new AmqpSourceConfigDef(), originals);
		amqpFragment = new AmqpFragment(FragmentDataAccess.from(this));
	}

	@Override
	public Receiver getReceiver(Connection connection) throws ClientException {
		return amqpFragment.getReceiver(connection);
	}

	@Override
	public Client getClient() {
		return amqpFragment.getClient();
///			for (int i = 0; i < MESSAGE_COUNT; ++i) {
//				Delivery delivery = receiver.receive();  10
//				Message<String> message = delivery.message();  11
//
//				System.out.println("Received message with body: " + message.body());
//			}
//		}
	}

	@Override
	public Connection getConnection(Client client) throws ClientException {
		return amqpFragment.getConnection(client);
	}

	public Transformer getTransformer() {
		throw new UnsupportedOperationException();
	}
}
