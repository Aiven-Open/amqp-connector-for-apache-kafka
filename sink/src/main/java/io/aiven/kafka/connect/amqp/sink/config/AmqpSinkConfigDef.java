package io.aiven.kafka.connect.amqp.sink.config;

import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;
import io.aiven.commons.kafka.connector.common.config.ConnectorCommonConfigDef;
import io.aiven.kafka.connect.amqp.common.config.AmqpFragment;
import java.util.Map;
import org.apache.kafka.common.config.ConfigValue;

public class AmqpSinkConfigDef extends ConnectorCommonConfigDef {

  /** Constructor. */
  public AmqpSinkConfigDef() {
    super();
    AmqpFragment.update(this);
  }

  /**
   * Validates the AMQP configuration is correct and meets requirements
   *
   * @param valueMap the map of configuration names to values.
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
