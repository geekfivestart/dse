package org.apache.cassandra.repair;

import com.google.common.util.concurrent.AbstractFuture;
import java.net.InetAddress;
import java.util.concurrent.RunnableFuture;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.messages.SnapshotMessage;

public class SnapshotTask extends AbstractFuture<InetAddress> implements RunnableFuture<InetAddress> {
   private final RepairJobDesc desc;
   private final InetAddress endpoint;

   public SnapshotTask(RepairJobDesc desc, InetAddress endpoint) {
      this.desc = desc;
      this.endpoint = endpoint;
   }

   public void run() {
      MessagingService.instance().sendSingleTarget(Verbs.REPAIR.SNAPSHOT.newRequest(this.endpoint, new SnapshotMessage(this.desc))).thenAccept((r) -> {
         this.set(this.endpoint);
      }).exceptionally((f) -> {
         this.setException(new RuntimeException("Could not create snapshot at " + this.endpoint));
         return null;
      });
   }
}
