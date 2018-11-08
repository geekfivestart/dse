package org.apache.cassandra.service.paxos;

import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.UUIDGen;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PrepareCallback extends AbstractPaxosCallback<PrepareResponse> {
   private static final Logger logger = LoggerFactory.getLogger(PrepareCallback.class);
   public boolean promised = true;
   public Commit mostRecentCommit;
   public Commit mostRecentInProgressCommit;
   public Commit mostRecentInProgressCommitWithUpdate;
   private final Map<InetAddress, Commit> commitsByReplica = new ConcurrentHashMap();

   public PrepareCallback(DecoratedKey key, TableMetadata metadata, int targets, ConsistencyLevel consistency, long queryStartNanoTime) {
      super(targets, consistency, queryStartNanoTime);
      this.mostRecentCommit = Commit.emptyCommit(key, metadata);
      this.mostRecentInProgressCommit = Commit.emptyCommit(key, metadata);
      this.mostRecentInProgressCommitWithUpdate = Commit.emptyCommit(key, metadata);
   }

   public synchronized void onResponse(Response<PrepareResponse> message) {
      PrepareResponse response = (PrepareResponse)message.payload();
      logger.trace("Prepare response {} from {}", response, message.from());
      if(response.inProgressCommit.isAfter(this.mostRecentInProgressCommit)) {
         this.mostRecentInProgressCommit = response.inProgressCommit;
      }

      if(response.promised) {
         this.commitsByReplica.put(message.from(), response.mostRecentCommit);
         if(response.mostRecentCommit.isAfter(this.mostRecentCommit)) {
            this.mostRecentCommit = response.mostRecentCommit;
         }

         if(response.inProgressCommit.isAfter(this.mostRecentInProgressCommitWithUpdate) && !response.inProgressCommit.update.isEmpty()) {
            this.mostRecentInProgressCommitWithUpdate = response.inProgressCommit;
         }

         this.latch.countDown();
      } else {
         this.promised = false;

         while(this.latch.getCount() > 0L) {
            this.latch.countDown();
         }

      }
   }

   public Iterable<InetAddress> replicasMissingMostRecentCommit(TableMetadata metadata, int nowInSec) {
      long paxosTtlSec = (long)SystemKeyspace.paxosTtlSec(metadata);
      return (Iterable)((long)UUIDGen.unixTimestampInSec(this.mostRecentCommit.ballot) + paxosTtlSec < (long)nowInSec?Collections.emptySet():Iterables.filter(this.commitsByReplica.keySet(), new Predicate<InetAddress>() {
         public boolean apply(InetAddress inetAddress) {
            return !((Commit)PrepareCallback.this.commitsByReplica.get(inetAddress)).ballot.equals(PrepareCallback.this.mostRecentCommit.ballot);
         }
      }));
   }
}
