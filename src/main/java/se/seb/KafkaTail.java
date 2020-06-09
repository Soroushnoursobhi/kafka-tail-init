package se.seb;

import ch.qos.logback.classic.Level;
import io.confluent.kafka.serializers.AbstractKafkaSchemaSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import joptsimple.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.LoggerFactory;

import java.io.PrintStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class KafkaTail implements HelpFormatter {

  private static final String BOOTSTRAP_SERVERS = Stream.of(
          "localhost:9092").collect(Collectors.joining(","));

  private static Properties PROPS = new Properties();

  static {
    ch.qos.logback.classic.Logger root = (ch.qos.logback.classic.Logger)
            LoggerFactory.getLogger(ch.qos.logback.classic.Logger.ROOT_LOGGER_NAME);
    root.setLevel(Level.WARN);
    PROPS.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-tail-group-" + System.currentTimeMillis());
    PROPS.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
    PROPS.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    PROPS.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
    PROPS.put(AbstractKafkaSchemaSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
    PROPS.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
  }

  public static void main(String[] args) throws Exception {
//    Scanner sc = new Scanner(System.in);
//    while (true) {
//      String s = sc.nextLine();
//      String[] opts = s.trim().split(" ");
//      new KafkaTail().execute(opts);
//    }
    new KafkaTail().execute(args);
  }

  public void execute(String[] args) throws Exception {
    OptionParser parser = new OptionParser();
    parser.formatHelpWith(this);
    parser.allowsUnrecognizedOptions();

    OptionSpec<String> topicOpt = parser.accepts("t", "Name of topic (mandatory)")
            .withRequiredArg().ofType(String.class).describedAs("topic");

    OptionSpec<String> offsetOpt = parser.accepts("o", "Offset to read from, <default> 'tail' or 'head' or 'number' (tail rewind)")
            .withRequiredArg().ofType(String.class).describedAs("offset");

    OptionSpec<String> fieldOpt = parser.accepts("f", "Field to print")
            .withRequiredArg().ofType(String.class).describedAs("field");

    OptionSpec<Integer> partitionOpt = parser.accepts("p", "Partition to consume from (default all partitions will be consumed)")
            .withRequiredArg().ofType(Integer.class).describedAs("partition");

    parser.accepts("s", "Print short");
    parser.accepts("w", "Print Watermarks");
    parser.accepts("h", "Display help");
    parser.accepts("l", "List topics");

    OptionSet options = parser.parse(args);
    if (options.has("h")) {
      parser.printHelpOn(System.out);
      return;
    }
    if (options.has("l")) {
      listTopics(System.out);
      return;
    }
    if (!options.hasArgument(topicOpt)) {
      parser.printHelpOn(System.out);
      return;
    }
    final String offset = options.hasArgument(offsetOpt) ?
            options.valueOf(offsetOpt) : "tail";

    boolean printWatermarks = false;
    if (options.has("w")) {
      printWatermarks = true;
    }

    boolean shortDebug = false;
    if (options.has("s")) {
      shortDebug = true;
    }

    String[] fields = null;
    if (options.hasArgument(fieldOpt)) {
      fields = options.valueOf(fieldOpt).split("\\.");
    }

//    Printer printer = new Printer(fields, shortDebug, printWatermarks);
    String topic = (String) options.valueOf("t");

    final Optional<Integer> partition = options.hasArgument(partitionOpt) ?
            Optional.ofNullable(options.valueOf(partitionOpt)) : Optional.empty();

    Printer printer = new Printer();

    try (Context context = new Context(topic, offset, PROPS, printer, partition)) {
      context.executeFirst(
              new ConsumerTailRewind(),
              new ConsumerHead(),
              new ConsumerTail());
    }
  }

  private static void listTopics(PrintStream out) {
    try (KafkaConsumer<String, Payment> consumer = new KafkaConsumer<>(PROPS)) {
      consumer.listTopics().entrySet().stream()
              .map(entry -> entry.getKey() + " (" + entry.getValue().size() + " partitions)")
              .sorted()
              .forEach(out::println);
    }
  }
  private static final String LINE_SEPARATOR = System.getProperty("line.separator");

  public String format(Map<String, ? extends OptionDescriptor> options) {
    StringBuilder sb = new StringBuilder();
    sb.append("Usage: java -jar ... [options]");
    sb.append("\n");
    sb.append(" [opt] means optional argument.\n");
    sb.append(" <opt> means required argument.\n");
    sb.append(" \"+\" means comma-separated list of values.\n");
    sb.append("\n");
    for (OptionDescriptor each : options.values()) {
      sb.append(lineFor(each));
    }
    return sb.toString();
  }

  private String lineFor(OptionDescriptor d) {
    StringBuilder line = new StringBuilder();

    StringBuilder o = new StringBuilder();
    o.append("  ");
    for (String str : d.options()) {
      if (!d.representsNonOptions()) {
        o.append("-");
      }
      o.append(str);
      if (d.acceptsArguments()) {
        o.append(" ");
        if (d.requiresArgument()) {
          o.append("<");
        } else {
          o.append("[");
        }
        o.append(d.argumentDescription());
        if (d.requiresArgument()) {
          o.append(">");
        } else {
          o.append("]");
        }
      }
    }

    final int optWidth = 30;

    line.append(String.format("%-" + optWidth + "s", o.toString()));
    boolean first = true;
    String desc = d.description();
    List<?> defaults = d.defaultValues();
    if (defaults != null && !defaults.isEmpty()) {
      desc += " (default: " + defaults.toString() + ")";
    }
    for (String l : rewrap(desc)) {
      if (first) {
        first = false;
      } else {
        line.append(LINE_SEPARATOR);
        line.append(String.format("%-" + optWidth + "s", ""));
      }
      line.append(l);
    }

    line.append(LINE_SEPARATOR);
    line.append(LINE_SEPARATOR);
    return line.toString();
  }

  public static Collection<String> rewrap(String lines) {
    Collection<String> result = new ArrayList<>();
    String[] words = lines.split("[ \n]");
    String line = "";
    int cols = 0;
    for (String w : words) {
      cols += w.length();
      line += w + " ";
      if (cols > 40) {
        result.add(line);
        line = "";
        cols = 0;
      }
    }
    if (!line.trim().isEmpty()) {
      result.add(line);
    }
    return result;
  }
}
