package io.aiven.kafka.connect.amqp.sink.errant;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Handles records that could not be sent. */
public class ErrantRecordHandler {
  private static final Logger logger = LoggerFactory.getLogger(ErrantRecordHandler.class);
  private final ErrantRecordReporter errantRecordReporter;

  /**
   * Constructor.
   *
   * @param errantRecordReporter The record reporter to report to. May be {@code null}.
   */
  public ErrantRecordHandler(ErrantRecordReporter errantRecordReporter) {
    this.errantRecordReporter =
        errantRecordReporter == null ? new LoggingReporter() : errantRecordReporter;
  }

  public void reportErrantRecord(SinkRecord record, String reason) {
    this.reportErrantRecord(record, new Exception(reason));
  }

  public void reportErrantRecord(SinkRecord record, Exception e) {
    logger.debug("Sending 1 record to DLQ");
    errantRecordReporter.report(record, e);
  }

  public void reportErrantRecords(Set<SinkRecord> records, Exception e) {
    logger.debug("Sending {} records to DLQ", records.size());
    records.forEach(r -> errantRecordReporter.report(r, e));
  }

  public void reportErrantRecords(Map<SinkRecord, Throwable> rowToError) {
    logger.debug("Sending {} records to DLQ", rowToError.size());
    rowToError.forEach(errantRecordReporter::report);
  }

  public static class LoggingReporter implements ErrantRecordReporter {

    public LoggingReporter() {
      logger.warn("No ErrantRecordReporter provided.  All DLQ records will be logged instead.");
    }

    @Override
    public Future<Void> report(SinkRecord record, Throwable error) {
      logger.warn("DLQ: {} reason: {}", record, error.getMessage());
      if (logger.isDebugEnabled()) {
        logger.debug("DLQ: {} reason: {}", record, error.getMessage(), error);
      }
      return CompletableFuture.completedFuture(null);
    }
  }
}
