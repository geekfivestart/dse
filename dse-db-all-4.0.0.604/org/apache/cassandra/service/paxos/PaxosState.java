package org.apache.cassandra.service.paxos;

import com.google.common.util.concurrent.Striped;
import java.util.concurrent.locks.Lock;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.UUIDGen;

public class PaxosState {
   private static final Striped<Lock> LOCKS = Striped.lazyWeakLock(TPC.getNumCores() * 1024);
   private final Commit promised;
   private final Commit accepted;
   private final Commit mostRecentCommit;

   public PaxosState(DecoratedKey key, TableMetadata metadata) {
      this(Commit.emptyCommit(key, metadata), Commit.emptyCommit(key, metadata), Commit.emptyCommit(key, metadata));
   }

   public PaxosState(Commit promised, Commit accepted, Commit mostRecentCommit) {
      assert promised.update.partitionKey().equals(accepted.update.partitionKey());

      assert accepted.update.partitionKey().equals(mostRecentCommit.update.partitionKey());

      assert promised.update.metadata().id.equals(accepted.update.metadata().id);

      assert accepted.update.metadata().id.equals(mostRecentCommit.update.metadata().id);

      this.promised = promised;
      this.accepted = accepted;
      this.mostRecentCommit = mostRecentCommit;
   }

   public static PrepareResponse prepare(Commit toPrepare) {
      long start = System.nanoTime();

      PrepareResponse var6;
      try {
         Lock lock = (Lock)LOCKS.get(toPrepare.update.partitionKey());
         lock.lock();

         try {
            int nowInSec = UUIDGen.unixTimestampInSec(toPrepare.ballot);
            PaxosState state = (PaxosState)TPCUtils.blockingGet(SystemKeyspace.loadPaxosState(toPrepare.update.partitionKey(), toPrepare.update.metadata(), nowInSec));
            if(!toPrepare.isAfter(state.promised)) {
               Tracing.trace("Promise rejected; {} is not sufficiently newer than {}", toPrepare, state.promised);
               var6 = new PrepareResponse(false, state.promised, state.mostRecentCommit);
               return var6;
            }

            Tracing.trace("Promising ballot {}", (Object)toPrepare.ballot);
            TPCUtils.blockingAwait(SystemKeyspace.savePaxosPromise(toPrepare));
            var6 = new PrepareResponse(true, state.accepted, state.mostRecentCommit);
         } finally {
            lock.unlock();
         }
      } finally {
         Keyspace.open(toPrepare.update.metadata().keyspace).getColumnFamilyStore(toPrepare.update.metadata().id).metric.casPrepare.addNano(System.nanoTime() - start);
      }

      return var6;
   }

   public static Boolean propose(Commit proposal) {
      long start = System.nanoTime();

      Boolean var6;
      try {
         Lock lock = (Lock)LOCKS.get(proposal.update.partitionKey());
         lock.lock();

         try {
            int nowInSec = UUIDGen.unixTimestampInSec(proposal.ballot);
            PaxosState state = (PaxosState)TPCUtils.blockingGet(SystemKeyspace.loadPaxosState(proposal.update.partitionKey(), proposal.update.metadata(), nowInSec));
            if(proposal.hasBallot(state.promised.ballot) || proposal.isAfter(state.promised)) {
               Tracing.trace("Accepting proposal {}", (Object)proposal);
               TPCUtils.blockingAwait(SystemKeyspace.savePaxosProposal(proposal));
               var6 = Boolean.valueOf(true);
               return var6;
            }

            Tracing.trace("Rejecting proposal for {} because inProgress is now {}", proposal, state.promised);
            var6 = Boolean.valueOf(false);
         } finally {
            lock.unlock();
         }
      } finally {
         Keyspace.open(proposal.update.metadata().keyspace).getColumnFamilyStore(proposal.update.metadata().id).metric.casPropose.addNano(System.nanoTime() - start);
      }

      return var6;
   }

   public static void commit(Commit proposal) {
      long start = System.nanoTime();

      try {
         if(UUIDGen.unixTimestamp(proposal.ballot) >= SystemKeyspace.getTruncatedAt(proposal.update.metadata().id)) {
            Tracing.trace("Committing proposal {}", (Object)proposal);
            Mutation mutation = proposal.makeMutation();
            TPCUtils.blockingAwait(Keyspace.open(mutation.getKeyspaceName()).apply(mutation, true));
         } else {
            Tracing.trace("Not committing proposal {} as ballot timestamp predates last truncation time", (Object)proposal);
         }

         TPCUtils.blockingAwait(SystemKeyspace.savePaxosCommit(proposal));
      } finally {
         Keyspace.open(proposal.update.metadata().keyspace).getColumnFamilyStore(proposal.update.metadata().id).metric.casCommit.addNano(System.nanoTime() - start);
      }

   }
}
