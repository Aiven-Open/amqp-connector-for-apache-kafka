package io.aiven.kafka.connect.amqp.common.config;

import io.aiven.commons.kafka.config.ExtendedConfigKey;
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
import java.util.concurrent.Future;

public class AmqpFragment extends ConfigFragment {

    private static final String GROUP_AMQP_CONNECTIVITY = "AMQP Connectivity";
    private static final String HOST = "amqp.host";
    private static final String PORT = "amqp.port";
    private static final String ADDRESS = "amqp.address";
    private static final String USER = "amqp.user";
    private static final String PASSWORD = "amqp.password";

    /**
     * Construct the ConfigFragment.
     *
     * @param dataAccess the FragmentDataAccess that this fragment is associated with.
     */
    public AmqpFragment(FragmentDataAccess dataAccess) {
        super(dataAccess);
    }

    /**
     * Adds the configuration options for compression to the configuration
     * definition.
     *
     * @param configDef the Configuration definition.
     * @return the update configuration definition
     */
    public static ConfigDef update(final ConfigDef configDef) {
        // later
        addAMQPConnectivity(configDef);
        return configDef;
    }

    public static Setter setter(Map<String, String> data) {
        return new Setter(data);
    }

    /**
     * Override of the validate method
     *
     * @param configMap The map of all values for configuration
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
        var amqpCounter = 0;
        configDef.define(ExtendedConfigKey.builder(HOST).group(GROUP_AMQP_CONNECTIVITY).orderInGroup(++amqpCounter)
                .since("1.0.0").type(ConfigDef.Type.STRING)
                .validator(new UrlValidator(true)).importance(ConfigDef.Importance.MEDIUM)
                .documentation("The host address for the AMQP service")
                .build()
        ).define(ExtendedConfigKey.builder(PORT).group(GROUP_AMQP_CONNECTIVITY).orderInGroup(++amqpCounter)
                .since("1.0.0").type(ConfigDef.Type.INT).defaultValue(5672)
                .validator(ConfigDef.Range.between(1, 65534)).importance(ConfigDef.Importance.MEDIUM)
                .documentation("The port for the AMQP server.")
                .build()
        ).define(ExtendedConfigKey.builder(ADDRESS).group(GROUP_AMQP_CONNECTIVITY).orderInGroup(++amqpCounter)
                .since("1.0.0").type(ConfigDef.Type.STRING)
                .validator(new ConfigDef.NonEmptyStringWithoutControlChars()).importance(ConfigDef.Importance.MEDIUM)
                .documentation("The address (topic) to listend to.")
                .build()
        ).define(ExtendedConfigKey.builder(USER).group(GROUP_AMQP_CONNECTIVITY).orderInGroup(++amqpCounter)
                .since("1.0.0").type(ConfigDef.Type.STRING)
                .validator(new ConfigDef.NonEmptyStringWithoutControlChars()).importance(ConfigDef.Importance.MEDIUM)
                .documentation("The user to log into the AMQP server.")
                .build()
        ).define(ExtendedConfigKey.builder(PASSWORD).group(GROUP_AMQP_CONNECTIVITY).orderInGroup(++amqpCounter)
                .since("1.0.0").type(ConfigDef.Type.PASSWORD)
                .validator(new ConfigDef.NonEmptyStringWithoutControlChars()).importance(ConfigDef.Importance.MEDIUM)
                .documentation("The password for the user to log into the AMQP server.")
                .build()
        );
    }

    public Client getClient() {
        return Client.create();
    }

    public Connection getConnection(Client client) throws ClientException {
        return client.connect(
                dataAccess.getString(HOST),
                dataAccess.getInt(PORT),
                new ConnectionOptions()
                        .user(dataAccess.getString(USER))
                        .password(dataAccess.getPassword(PASSWORD).value())
        );
    }

    public Receiver getReceiver(Connection connection) throws ClientException {
        return connection.openReceiver(dataAccess.getString(ADDRESS));
    }

    public static class Setter extends AbstractFragmentSetter<CommonConfigFragment.Setter> {

        /**
         * Constructor.
         *
         * @param data the map of data items being set.
         */
        protected Setter(Map<String, String> data) {
            super(data);
        }

        public Setter setHost(String host) {
            data().put(HOST, host);
            return this;
        }

        public Setter setPort(int port) {
            data().put(PORT, Integer.toString(port));
            return this;
        }

        public Setter setAddress(String address) {
            data().put(ADDRESS, address);
            return this;
        }

        public Setter setUser(String user) {
            data().put(USER, user );
            return this;
        }

        public Setter setPassword(String password) {
            data().put(PASSWORD, password);
            return this;
        }
    }
}
