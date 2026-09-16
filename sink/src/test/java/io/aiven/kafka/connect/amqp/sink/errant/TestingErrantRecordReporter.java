package io.aiven.kafka.connect.amqp.sink.errant;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.kafka.connect.sink.ErrantRecordReporter;
import org.apache.kafka.connect.sink.SinkRecord;

public class TestingErrantRecordReporter implements ErrantRecordReporter {
  List<Pair<SinkRecord, Throwable>> lst = new ArrayList<>();

  @Override
  public Future<Void> report(SinkRecord record, Throwable error) {
    lst.add(ImmutablePair.of(record, error));
    return CompletableFuture.completedFuture(null);
  }

  public List<Pair<SinkRecord, Throwable>> getErrors() {
    return lst;
  }
}
