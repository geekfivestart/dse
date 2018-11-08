package org.apache.cassandra.repair;

import com.google.common.util.concurrent.AbstractFuture;
import java.net.InetAddress;
import org.apache.cassandra.exceptions.RepairException;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.repair.messages.ValidationRequest;
import org.apache.cassandra.streaming.PreviewKind;
import org.apache.cassandra.utils.MerkleTrees;

public class ValidationTask extends AbstractFuture<TreeResponse> implements Runnable {
   private final RepairJobDesc desc;
   private final InetAddress endpoint;
   private final int nowInSec;
   private final PreviewKind previewKind;

   public ValidationTask(RepairJobDesc desc, InetAddress endpoint, int nowInSec, PreviewKind previewKind) {
      this.desc = desc;
      this.endpoint = endpoint;
      this.nowInSec = nowInSec;
      this.previewKind = previewKind;
   }

   public void run() {
      ValidationRequest request = new ValidationRequest(this.desc, this.nowInSec);
      MessagingService.instance().send(Verbs.REPAIR.VALIDATION_REQUEST.newRequest(this.endpoint, (Object)request));
   }

   public void treesReceived(MerkleTrees trees) {
      if(trees == null) {
         this.setException(new RepairException(this.desc, this.previewKind, "Validation failed in " + this.endpoint));
      } else {
         this.set(new TreeResponse(this.endpoint, trees));
      }

   }
}
