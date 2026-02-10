/*
         Copyright 2026 Aiven Oy and project contributors

        Licensed under the Apache License, Version 2.0 (the "License");
        you may not use this file except in compliance with the License.
        You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

        Unless required by applicable law or agreed to in writing,
        software distributed under the License is distributed on an
        "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
        KIND, either express or implied.  See the License for the
        specific language governing permissions and limitations
        under the License.

        SPDX-License-Identifier: Apache-2
 */
package io.aiven.kafka.connect.amqp.common.config;

import io.aiven.commons.kafka.config.ExtendedConfigKey;
import io.aiven.commons.kafka.config.SinceInfo;
import io.aiven.commons.kafka.config.fragment.AbstractFragmentSetter;
import io.aiven.commons.kafka.config.fragment.CommonConfigFragment;
import io.aiven.commons.kafka.config.fragment.ConfigFragment;
import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;
import io.aiven.commons.kafka.config.validator.UrlValidator;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;
import org.apache.qpid.protonj2.client.Client;
import org.apache.qpid.protonj2.client.Connection;
import org.apache.qpid.protonj2.client.ConnectionOptions;
import org.apache.qpid.protonj2.client.Receiver;
import org.apache.qpid.protonj2.client.exceptions.ClientException;

import java.util.Map;

/**
 * The AMQP Fragment.
 */
public class AmqpFragment extends ConfigFragment implements AmqpCommonConfig {

	private static final String GROUP_AMQP_CONNECTIVITY = "AMQP Connectivity";
	private static final String HOST = "amqp.host";
	private static final String PORT = "amqp.port";
	private static final String ADDRESS = "amqp.address";
	private static final String USER = "amqp.user";
	private static final String PASSWORD = "amqp.password";

	/**
	 * Construct the ConfigFragment.
	 *
	 * @param dataAccess
	 *            the FragmentDataAccess that this fragment is associated with.
	 */
	public AmqpFragment(FragmentDataAccess dataAccess) {
		super(dataAccess);
	}

	/**
	 * Adds the configuration options for compression to the configuration
	 * definition.
	 *
	 * @param configDef
	 *            the Configuration definition.
	 * @return the update configuration definition
	 */
	public static ConfigDef update(final ConfigDef configDef) {
		// later
		addAMQPConnectivity(configDef);
		return configDef;
	}

	/**
	 * Creates the setter for this fragment.
	 * 
	 * @param data
	 *            the data to add values to.
	 * @return the Setter.
	 */
	public static Setter setter(Map<String, String> data) {
		return new Setter(data);
	}

	/**
	 * Override of the validate method
	 *
	 * @param configMap
	 *            The map of all values for configuration
	 */
	@Override
	public void validate(Map<String, ConfigValue> configMap) {// NOPMD useless overriding method ignore as we will add
		super.validate(configMap);
		// handle any restrictions between options here.

	}

	/**
	 *
	 * @param configDef
	 */
	static void addAMQPConnectivity(final ConfigDef configDef) {
		SinceInfo.Builder siBuilder = SinceInfo.builder().groupId("io.aiven.commons")
				.artifactId("kafka-source-connector-framework");
		var amqpCounter = 0;
		configDef.define(ExtendedConfigKey.builder(HOST).group(GROUP_AMQP_CONNECTIVITY).orderInGroup(++amqpCounter)
				.since(siBuilder.version("1.0.0").build()).validator(UrlValidator.builder().schemes("https").build())
				.importance(ConfigDef.Importance.MEDIUM).documentation("The host address for the AMQP service").build())
				.define(ExtendedConfigKey.builder(PORT).group(GROUP_AMQP_CONNECTIVITY).orderInGroup(++amqpCounter)
						.since(siBuilder.version("1.0.0").build()).type(ConfigDef.Type.INT).defaultValue(5672)
						.validator(ConfigDef.Range.between(1, 65534)).importance(ConfigDef.Importance.MEDIUM)
						.documentation("The port for the AMQP server.").build())
				.define(ExtendedConfigKey.builder(ADDRESS).group(GROUP_AMQP_CONNECTIVITY).orderInGroup(++amqpCounter)
						.since(siBuilder.version("1.0.0").build())
						.validator(new ConfigDef.NonEmptyStringWithoutControlChars())
						.importance(ConfigDef.Importance.MEDIUM).documentation("The address (topic) to listend to.")
						.build())
				.define(ExtendedConfigKey.builder(USER).group(GROUP_AMQP_CONNECTIVITY).orderInGroup(++amqpCounter)
						.since(siBuilder.version("1.0.0").build())
						.validator(new ConfigDef.NonEmptyStringWithoutControlChars())
						.importance(ConfigDef.Importance.MEDIUM).documentation("The user to log into the AMQP server.")
						.build())
				.define(ExtendedConfigKey.builder(PASSWORD).group(GROUP_AMQP_CONNECTIVITY).orderInGroup(++amqpCounter)
						.since(siBuilder.version("1.0.0").build())
						.validator(new ConfigDef.NonEmptyStringWithoutControlChars())
						.importance(ConfigDef.Importance.MEDIUM)
						.documentation("The password for the user to log into the AMQP server.").build());

	}

	@Override
	public Client getClient() {
		return Client.create();
	}

	@Override
	public Connection getConnection(Client client) throws ClientException {
		return client.connect(dataAccess.getString(HOST), dataAccess.getInt(PORT), new ConnectionOptions()
				.user(dataAccess.getString(USER)).password(dataAccess.getPassword(PASSWORD).value()));
	}

	@Override
	public Receiver getReceiver(Connection connection) throws ClientException {
		return connection.openReceiver(dataAccess.getString(ADDRESS));
	}

	/**
	 * The Setter for the AMQP fragment.
	 */
	public static class Setter extends AbstractFragmentSetter<CommonConfigFragment.Setter> {

		/**
		 * Constructor.
		 *
		 * @param data
		 *            the map of data items being set.
		 */
		protected Setter(Map<String, String> data) {
			super(data);
		}

		/**
		 * Sets the host value.
		 * 
		 * @param host
		 *            the host for AMQP connection.
		 * @return this
		 */
		public Setter setHost(String host) {
			setValue(HOST, host);
			return this;
		}

		/**
		 * Sets the port for the AMQP connection.
		 * 
		 * @param port
		 *            the port for the AMQP connection.
		 * @return this.
		 */
		public Setter setPort(int port) {
			setValue(PORT, Integer.toString(port));
			return this;
		}

		/**
		 * Sets the address for the AMQP receiver.
		 * 
		 * @param address
		 *            the address (topic) to listen to
		 * @return this.
		 */
		public Setter setAddress(String address) {
			setValue(ADDRESS, address);
			return this;
		}

		/**
		 * Sets the user name to connect to the AMQP server.
		 * 
		 * @param user
		 *            the user name.
		 * @return this.
		 */
		public Setter setUser(String user) {
			setValue(USER, user);
			return this;
		}

		/**
		 * Sets the user passwrod to connecto the AMQP server.
		 * 
		 * @param password
		 *            the password.
		 * @return this.
		 */
		public Setter setPassword(String password) {
			setValue(PASSWORD, password);
			return this;
		}
	}
}
