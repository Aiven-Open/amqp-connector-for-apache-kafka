package io.aiven.kafka.connect.amqp.source;

import io.aiven.commons.kafka.connector.source.task.Context;
import java.util.Objects;
import org.apache.qpid.protonj2.client.Delivery;

/** The context for the AMQP processes. */
public class AmqpContext extends Context {
  /** The key for the delivery property. */
  public static final String DELIVERY_KEY = Delivery.class.getName();

  private AmqpContext(AmqpContext.Builder builder) {
    super(builder);
  }

  /**
   * Gets the Delivery
   *
   * @return the Delivery.
   */
  public Delivery getDelivery() {
    return getObject(DELIVERY_KEY, Delivery.class::cast).orElseThrow();
  }

  @Override
  public Builder builder() {
    return new Builder(this);
  }

  /** The builder fro the AMQP Context. */
  public static class Builder extends Context.Builder<AmqpContext.Builder> {

    /** Validator for delivery property. */
    private static final Validator deliveryValidator =
        properties ->
            Objects.requireNonNull(properties.get(DELIVERY_KEY), "delivery must not be null");

    /**
     * Construct the builder from a context.
     *
     * @param context the context to create the builder from.
     */
    public Builder(Context context) {
      super(context);
      addValidator(deliveryValidator);
    }

    /**
     * Construct a builder from a native key and a Delivery object.
     *
     * @param nativeKey the native key.
     * @param delivery the Delivery object.
     */
    public Builder(Comparable<?> nativeKey, Delivery delivery) {
      super(nativeKey);
      delivery(delivery);
      addValidator(deliveryValidator);
    }

    /**
     * Set the delivery in the context.
     *
     * @param delivery the delivery value for the context.
     * @return this.
     */
    public Builder delivery(Delivery delivery) {
      Objects.requireNonNull(delivery, "delivery must not be null");
      set(DELIVERY_KEY, delivery);
      return self();
    }

    @Override
    public AmqpContext build() {
      return new AmqpContext(this);
    }
  }
}
