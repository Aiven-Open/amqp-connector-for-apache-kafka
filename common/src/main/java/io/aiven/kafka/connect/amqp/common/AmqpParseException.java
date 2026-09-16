package io.aiven.kafka.connect.amqp.common;

/** Exception thrown when data from AMQP can not be parsed into a Kafka friendly format. */
public class AmqpParseException extends Exception {

  /**
   * Constructor.
   *
   * @param message the message to identify the error.
   */
  public AmqpParseException(String message) {
    super(message);
  }
}
