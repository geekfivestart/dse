package org.apache.cassandra.repair.messages;

import java.util.UUID;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.service.ActiveRepairService;

public abstract class ConsistentRepairMessage<T extends ConsistentRepairMessage> extends RepairMessage<T> {
   public final UUID sessionID;

   protected ConsistentRepairMessage(UUID sessionID) {
      super((RepairJobDesc)null);

      assert sessionID != null;

      this.sessionID = sessionID;
   }

   public boolean validate() {
      if(!ActiveRepairService.instance.hasParentRepairSession(this.sessionID)) {
         throw new IllegalStateException("No parent repair session: " + this.sessionID);
      } else {
         return true;
      }
   }
}
