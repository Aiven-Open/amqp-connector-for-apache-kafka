package io.aiven.kafka.connect.amqp.sink.config;

import io.aiven.commons.kafka.config.ExtendedConfigKey;
import io.aiven.commons.kafka.config.SinceInfo;
import io.aiven.commons.kafka.config.fragment.AbstractFragmentSetter;
import io.aiven.commons.kafka.config.fragment.FragmentDataAccess;
import io.aiven.commons.kafka.config.validator.EnumValidator;
import io.aiven.commons.kafka.connector.common.config.ConnectorCommonConfigDef;
import io.aiven.kafka.connect.amqp.common.config.AmqpFragment;
import java.util.Map;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigValue;

public class AmqpSinkConfigDef extends ConnectorCommonConfigDef {

  static final String STRATEGY = "amqp.strategy";
  private static final String SINK_GROUP = "AMQP Sink";

  /** Constructor. */
  public AmqpSinkConfigDef() {
    super();
    AmqpFragment.update(this);
    int sinkCounter = 0;
    SinceInfo.Builder siBuilder =
        SinceInfo.builder().groupId("io.aiven.kafka.connect").artifactId("sink-connector-for-amqp");

    this.define(
        ExtendedConfigKey.builder(STRATEGY)
            .group(SINK_GROUP)
            .defaultValue(ConfigDef.NO_DEFAULT_VALUE)
            .orderInGroup(++sinkCounter)
            .since(siBuilder.version("0.2.0").build())
            .type(ConfigDef.Type.STRING)
            .validator(EnumValidator.caseInsensitive(AmqpStrategy.class))
            .importance(ConfigDef.Importance.MEDIUM)
            .documentation("The strategy for building the AMQP message.")
            .build());
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
    AmqpFragment amqpFragment = new AmqpFragment(fragmentDataAccess);
    amqpFragment.validate(values);
    return values;
  }

  /** The Setter for the AMQP sink specific options. */
  public static class Setter extends AbstractFragmentSetter<Setter> {

    /**
     * Constructor.
     *
     * @param data the map of data items being set.getMessageFormat
     */
    public Setter(Map<String, String> data) {
      super(data);
    }

    /**
     * Sets the write strategy.
     *
     * @param strategy The write strategy to use.
     * @return this
     */
    public Setter strategy(AmqpStrategy strategy) {
      return setValue(STRATEGY, strategy.name());
    }
  }
}
