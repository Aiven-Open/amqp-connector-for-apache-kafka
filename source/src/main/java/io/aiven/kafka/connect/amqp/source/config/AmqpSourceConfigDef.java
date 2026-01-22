package io.aiven.kafka.connect.amqp.source.config;

import io.aiven.commons.kafka.config.CommonConfig;
import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;
import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
import io.aiven.kafka.connect.amqp.common.config.AmqpFragment;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

import java.util.Map;

public final class AmqpSourceConfigDef extends SourceCommonConfig.SourceCommonConfigDef {
    AmqpSourceConfigDef() {
        super();
        AmqpFragment.update(this);
    }

    /**
     * Validates the Salesforce configuration is correct and meets requirements
     *
     * @param valueMap
     *            the map of configuration names to values.
     * @return the updated map.
     */
    @Override
    public Map<String, ConfigValue> multiValidate(final Map<String, ConfigValue> valueMap) {
        Map<String, ConfigValue> values = super.multiValidate(valueMap);
        // validate that the config fragment options are good.
        FragmentDataAccess fragmentDataAccess = FragmentDataAccess.from(valueMap);
        new AmqpFragment(fragmentDataAccess).validate(values);
        return values;
    }


}
