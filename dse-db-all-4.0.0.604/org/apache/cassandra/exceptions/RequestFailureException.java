package org.apache.cassandra.exceptions;

import com.google.common.base.Joiner;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.db.ConsistencyLevel;

public class RequestFailureException extends RequestExecutionException {
   public final ConsistencyLevel consistency;
   public final int received;
   public final int blockFor;
   public final Map<InetAddress, RequestFailureReason> failureReasonByEndpoint;

   protected RequestFailureException(ExceptionCode code, ConsistencyLevel consistency, int received, int blockFor, Map<InetAddress, RequestFailureReason> failureReasonByEndpoint) {
      super(code, String.format("Operation failed - received %d responses and %d failures (%s)", new Object[]{Integer.valueOf(received), Integer.valueOf(failureReasonByEndpoint.size()), toString(failureReasonByEndpoint)}));
      this.consistency = consistency;
      this.received = received;
      this.blockFor = blockFor;
      this.failureReasonByEndpoint = new HashMap(failureReasonByEndpoint);
   }

   private static String toString(Map<InetAddress, RequestFailureReason> failureMap) {
      SetMultimap<RequestFailureReason, InetAddress> byReasons = HashMultimap.create();
      Iterator var2 = failureMap.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<InetAddress, RequestFailureReason> entry = (Entry)var2.next();
         byReasons.put(entry.getValue(), entry.getKey());
      }

      return Joiner.on(", ").withKeyValueSeparator("=").join(byReasons.asMap());
   }
}
