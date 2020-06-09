package se.seb;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.function.Consumer;

public class ConsumerTailRewind implements Consumer<Context> {
  @Override
  public void accept(Context ctx) {
    Long numOffsets = ctx.offset(Long::parseLong);
    ctx.seekEndOffset(numOffsets);

    while (ctx.partitionRecordsMin() < numOffsets) {
      ctx.consumeByPartition();
    }
    ctx.partitionRecords(numOffsets).forEach(cr -> {
      Instant instant = Instant.ofEpochMilli(cr.timestamp());
      LocalDateTime date = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
      String info = String.format("\n %s-%s = %s (%s)\n",
              cr.topic(), cr.partition(), date, cr.offset());
      System.out.println(info);
      try {
        ctx.print(cr);
      } catch (Exception e) {
        System.out.println("Could not print message.");
      }
    });
  }
}
