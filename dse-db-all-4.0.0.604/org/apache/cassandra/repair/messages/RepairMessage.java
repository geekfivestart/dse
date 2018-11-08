package org.apache.cassandra.repair.messages;

import com.google.common.annotations.VisibleForTesting;
import java.util.Optional;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.service.ActiveRepairService;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.VersionDependent;

public abstract class RepairMessage<T extends RepairMessage> {
   public final RepairJobDesc desc;

   protected RepairMessage(RepairJobDesc desc) {
      this.desc = desc;
   }

   public boolean validate() {
      if(this.desc != null && this.desc.parentSessionId != null && !ActiveRepairService.instance.hasParentRepairSession(this.desc.parentSessionId)) {
         throw new IllegalStateException("No parent repair session: " + this.desc.parentSessionId);
      } else {
         return true;
      }
   }

   @VisibleForTesting
   public abstract RepairMessage.MessageSerializer<T> serializer(RepairVerbs.RepairVersion var1);

   @VisibleForTesting
   public abstract Optional<Verb<T, ?>> verb();

   public abstract static class MessageSerializer<T extends RepairMessage> extends VersionDependent<RepairVerbs.RepairVersion> implements Serializer<T> {
      protected MessageSerializer(RepairVerbs.RepairVersion version) {
         super(version);
      }
   }
}
