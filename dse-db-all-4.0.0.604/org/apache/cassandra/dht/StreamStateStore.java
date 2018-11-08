package org.apache.cassandra.dht;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.streaming.StreamEvent;
import org.apache.cassandra.streaming.StreamEventHandler;
import org.apache.cassandra.streaming.StreamRequest;
import org.apache.cassandra.streaming.StreamState;

public class StreamStateStore implements StreamEventHandler {
   public StreamStateStore() {
   }

   public CompletableFuture<Set<Range<Token>>> getAvailableRanges(String keyspace, IPartitioner partitioner) {
      return SystemKeyspace.getAvailableRanges(keyspace, partitioner);
   }

   boolean isDataAvailableBlocking(String keyspace, Token token) {
      Set<Range<Token>> availableRanges = (Set)TPCUtils.blockingGet(this.getAvailableRanges(keyspace, token.getPartitioner()));
      Iterator var4 = availableRanges.iterator();

      Range range;
      do {
         if(!var4.hasNext()) {
            return false;
         }

         range = (Range)var4.next();
      } while(!range.contains((RingPosition)token));

      return true;
   }

   public void handleStreamEvent(StreamEvent event) {
      if(event.eventType == StreamEvent.Type.STREAM_COMPLETE) {
         StreamEvent.SessionCompleteEvent se = (StreamEvent.SessionCompleteEvent)event;
         if(se.success) {
            Set<String> keyspaces = se.transferredRangesPerKeyspace.keySet();
            List<CompletableFuture> futures = new ArrayList(keyspaces.size() + se.requests.size());
            Iterator var5 = keyspaces.iterator();

            while(var5.hasNext()) {
               String keyspace = (String)var5.next();
               futures.add(SystemKeyspace.updateTransferredRanges(se.streamOperation, se.peer, keyspace, (Collection)se.transferredRangesPerKeyspace.get(keyspace)));
            }

            var5 = se.requests.iterator();

            while(var5.hasNext()) {
               StreamRequest request = (StreamRequest)var5.next();
               futures.add(SystemKeyspace.updateAvailableRanges(request.keyspace, request.ranges));
            }

            TPCUtils.blockingAwait(CompletableFuture.allOf((CompletableFuture[])futures.toArray(new CompletableFuture[0])));
         }
      }

   }

   public void onSuccess(StreamState streamState) {
   }

   public void onFailure(Throwable throwable) {
   }
}
