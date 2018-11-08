package org.apache.cassandra.db;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.ExecutionException;

/** @deprecated */
@Deprecated
public interface HintedHandOffManagerMBean {
   void deleteHintsForEndpoint(String var1);

   void truncateAllHints() throws ExecutionException, InterruptedException;

   List<String> listEndpointsPendingHints();

   void scheduleHintDelivery(String var1) throws UnknownHostException;

   void pauseHintsDelivery(boolean var1);
}
