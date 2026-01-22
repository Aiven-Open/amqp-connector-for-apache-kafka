 package io.aiven.kafka.connect.amqp.source;

 import de.huxhorn.sulky.ulid.ULID;

 import io.aiven.commons.kafka.connector.source.AbstractSourceRecordIterator;
 import io.aiven.commons.kafka.connector.source.AbstractSourceTask;
 import io.aiven.commons.kafka.connector.source.OffsetManager;
 import io.aiven.commons.kafka.connector.source.config.SourceCommonConfig;
 import io.aiven.commons.kafka.connector.source.transformer.Transformer;
 import io.aiven.commons.timing.Backoff;
 import io.aiven.commons.timing.BackoffConfig;
 import io.aiven.commons.version.VersionInfo;
 import io.aiven.kafka.connect.amqp.source.config.AmqpSourceConfig;
 import org.apache.commons.collections4.IteratorUtils;
 import org.apache.kafka.connect.source.SourceRecord;
 import org.apache.qpid.protonj2.client.Delivery;
 import org.slf4j.Logger;
 import org.slf4j.LoggerFactory;

 import java.util.Iterator;
 import java.util.Map;
 import java.util.Objects;

 public class AmqpSourceTask extends AbstractSourceTask {
  /** The logger to write to */
  private static final Logger LOGGER = LoggerFactory.getLogger(AmqpSourceTask.class);
 private Iterator<AmqpSourceRecord> amqpSourceRecordIterator;
  private AmqpSourceConfig amqpSourceConfig;
 private Transformer transformer;
 private OffsetManager<AmqpOffsetManagerEntry> offsetManager;
private AmqpSourceData amqpSourceData;

 @Override
 protected Iterator<SourceRecord> getIterator(BackoffConfig config) {
//  return new AbstractSourceRecordIterator<AmqpNativeInfo, AmqpNativeInfo, AmqpOffsetManagerEntry, AmqpSourceRecord>(config,
//          transformer, offsetManager, amqpSourceData);
  final Iterator<SourceRecord> inner = new Iterator<>() {
   /**
    * The backoff for Amazon retryable exceptions
    */
   final Backoff backoff = new Backoff(config);

   @Override
   public boolean hasNext() {
    return amqpSourceRecordIterator.hasNext();
   }

   @Override
   public SourceRecord next() {
    final AmqpSourceRecord amqpSourceRecord = amqpSourceRecordIterator.next();
    return amqpSourceRecord.getSourceRecord(amqpSourceConfig.getErrorsTolerance(), offsetManager);
   }
  };
  return IteratorUtils.filteredIterator(inner, Objects::nonNull);
 }

 @Override
 protected AmqpSourceConfig configure(Map<String, String> props) {

   LOGGER.info("AMQP Source task started.");
   this.amqpSourceConfig = new AmqpSourceConfig(props);
   this.transformer = amqpSourceConfig.getTransformer();
   this.offsetManager = new OffsetManager<>(context);
   this.amqpSourceData = new AmqpSourceData(amqpSourceConfig);
   setSourceRecordIterator(
           new AmqpSourceRecordIterator(amqpSourceConfig, offsetManager, transformer, amqpSourceData));
   return amqpSourceConfig;

 }

  /**
   * Used in testing.
   * @param iterator the iterator to use.
   */
 void setSourceRecordIterator(AmqpSourceRecordIterator iterator) {
  this.amqpSourceRecordIterator = iterator;
 }

 @Override
 protected void closeResources() {

 }

 @Override
 public String version() {
 return new VersionInfo(this.getClass()).getVersion();
 }
 }
