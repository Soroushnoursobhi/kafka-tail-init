package se.seb;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class Printer {
  public void print(ConsumerRecord<String, Payment> cr) {
    System.out.println(cr.value());
  }
}
