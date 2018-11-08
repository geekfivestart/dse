package com.datastax.bdp.node.transport;

import io.netty.channel.Channel;
import java.util.concurrent.atomic.AtomicLong;

public abstract class ClientContext<R> {
   private static final AtomicLong idGen = new AtomicLong();
   public final long id;

   public ClientContext() {
      this.id = idGen.incrementAndGet();
   }

   public abstract void onResponse(R var1);

   public abstract void onError(Channel var1, Throwable var2);

   public void onError(Throwable e) {
      this.onError((Channel)null, e);
   }
}
