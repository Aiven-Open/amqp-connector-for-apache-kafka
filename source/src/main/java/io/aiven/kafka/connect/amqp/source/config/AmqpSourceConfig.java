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

        SPDX-License-Identifier: Apache-2
 */
package io.aiven.kafka.connect.amqp.source.config;

import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.commons.kafka.connector.source.config.SourceConfigFragment;
import io.aiven.kafka.connect.amqp.common.config.AmqpCommonConfig;
import io.aiven.kafka.connect.amqp.common.config.AmqpFragment;
import io.aiven.kafka.connect.amqp.source.transformer.AmqpTransformer;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

import java.util.Map;

/**
 * The configuration for an AMQP Source connector.
 */
public final class AmqpSourceConfig extends SourceCommonConfig implements AmqpCommonConfig {
	private final AmqpFragment amqpFragment;
	/**
	 * Constructor.
	 *
	 * @param originals
	 *            the initial configuration data.
	 */
	public AmqpSourceConfig(Map<String, String> originals) {
		super(new AmqpSourceConfigDef(), setTransformer(originals));
		FragmentDataAccess dataAccess = FragmentDataAccess.from(this);
		amqpFragment = new AmqpFragment(dataAccess);
	}

	private static Map<String, String> setTransformer(Map<String, String> props) {
		SourceConfigFragment.setter(props).transformerClass(AmqpTransformer.class);
		return props;
	}

	@Override
	public Receiver getReceiver(Connection connection) throws ClientException {
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
}
