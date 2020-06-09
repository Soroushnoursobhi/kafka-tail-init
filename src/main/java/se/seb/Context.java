package se.seb;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.jooq.lambda.Seq;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;

import static java.util.Comparator.comparing;
import static org.jooq.lambda.Seq.seq;

public class Context implements AutoCloseable{
  protected static Comparator<TopicPartition> TP_COMP =
          comparing(TopicPartition::topic).thenComparing(TopicPartition::partition);

  protected static Comparator<ConsumerRecord<String, Payment>> CONSUMER_RECORD_COMP =
          comparing((ConsumerRecord<String, Payment> cr) -> cr.topic())
                  .thenComparing(ConsumerRecord::partition)
                  .thenComparingLong(ConsumerRecord::offset);

  private Map<TopicPartition, Set<ConsumerRecord<String, Payment>>> partitionRecords
          = new ConcurrentSkipListMap<>(TP_COMP);

  private final Printer printer;
  private final List<TopicPartition> tpList;
  private final KafkaConsumer<String, Payment> consumer;
  private final String offset;

  public Context(String topic, String offset, Properties properties, Printer printer, Optional<Integer> partition) {
    this.consumer = new KafkaConsumer<>(properties);
    this.printer = printer;
    this.offset = offset;
    this.tpList = consumer.partitionsFor(topic)
            .stream()
            .filter(partitionInfo -> partition.map(p -> p == partitionInfo.partition()).orElse(true))
            .map(info -> new TopicPartition(topic, info.partition()))
            .collect(Collectors.toList());
    this.consumer.assign(tpList);
  }

  public void consumeByPartition() {
    for (ConsumerRecord<String, Payment> cr : consumer.poll(Duration.ofMinutes(1))) {
      TopicPartition tp = new TopicPartition(cr.topic(), cr.partition());
      partitionRecords.computeIfAbsent(tp,
              k -> new ConcurrentSkipListSet<>(CONSUMER_RECORD_COMP)).add(cr);
    }
  }

  protected Seq<ConsumerRecord<String, Payment>> partitionRecords(Long numOffsets) {
    return seq(partitionRecords.values())
            .map(Seq::seq)
            .flatMap(s -> s.limit(numOffsets));
  }

  public <T> T offset(Function<String, T> parser) throws RuntimeException {
    try {
      T value = parser.apply(offset);
      if (value == null || (value instanceof Boolean && !((Boolean) value))) {
        throw new IllegalArgumentException();
      }
      return value;
    } catch (Throwable throwable) {
      throw new IllegalArgumentException();
    }
  }

  public long partitionRecordsMin() {
    return partitionRecords.size() != tpList.size() ? 0 : partitionRecords.values().stream().map(Set::size).min(Comparator.naturalOrder()).orElse(0);
  }

  public Seq<ConsumerRecord<String, Payment>> poll(int timeout) {
    return seq(consumer.poll(timeout).iterator());
  }

  public void seekToBeginning() {
    consumer.seekToBeginning(tpList);
  }

  public void seekToEnd() {
    consumer.seekToEnd(tpList);
  }

  public void seekEndOffset(Long offset) {
    consumer.endOffsets(tpList)
            .forEach((key, value) -> consumer.seek(key, value - offset));
  }

  public void print(ConsumerRecord<String, Payment> cr) {
    try {
      printer.print(cr);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws Exception {
    consumer.close();
  }

  public void executeFirst(Consumer<Context>... consumers) {
    for (Consumer<Context> consumer : consumers) {
      try {
        consumer.accept(this);
        return;
      } catch (IllegalArgumentException e) {
      }
    }
  }

}
