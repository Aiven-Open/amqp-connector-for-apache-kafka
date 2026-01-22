package io.aiven.kafka.connect.amqp.common.config;


import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.exceptions.ClientException;


public interface AmqpCommonConfig  {

	Client getClient();

	Connection getConnection(Client client) throws ClientException;

	Receiver getReceiver(Connection connection) throws ClientException;
}
