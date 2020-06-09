package se.seb;

import java.util.function.Consumer;

public class ConsumerTail implements Consumer<Context> {
  @Override
  public void accept(Context ctx) {
    ctx.seekToEnd();
    while (true) {
      ctx.poll(1).forEach(ctx::print);
    }
  }
}
