package se.seb;

import java.util.function.Consumer;

public class ConsumerHead implements Consumer<Context> {
  @Override
  public void accept(Context ctx) {
    ctx.offset(offset -> offset.equals("head"));
    ctx.seekToBeginning();
    while (true) {
      ctx.poll(1).forEach(ctx::print);
    }
  }
}
