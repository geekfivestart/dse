package com.datastax.bdp.db.nodesync;

import com.datastax.bdp.db.utils.concurrent.CompletableFutures;
import java.net.InetAddress;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.NodeSyncConfig;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaChangeListener;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.IEndpointLifecycleSubscriber;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.units.RateValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class ValidationScheduler extends SchemaChangeListener implements IEndpointLifecycleSubscriber {
   private static final Logger logger = LoggerFactory.getLogger(ValidationScheduler.class);
   final NodeSyncState state;
   private final NodeSyncConfig config = DatabaseDescriptor.getNodeSyncConfig();
   private final Map<TableId, ContinuousValidationProposer> continuousValidations = new ConcurrentHashMap();
   private final PriorityQueue<ContinuousValidationProposer.Proposal> continuousProposals = new PriorityQueue();
   private final Map<UserValidationID, UserValidationProposer> userValidations = new ConcurrentHashMap();
   private final Queue<UserValidationProposer> pendingUserValidations = new ArrayDeque();
   private UserValidationProposer currentUserValidation;
   private final ReentrantLock lock = new ReentrantLock();
   private final Condition hasProposals;
   private final Condition hasNonRate;
   @Nullable
   private RateValue rateToRestore;
   private final AtomicLong scheduledValidations;
   private volatile boolean isShutdown;
   private final ExecutorService eventExecutor;

   ValidationScheduler(NodeSyncState state) {
      this.hasProposals = this.lock.newCondition();
      this.hasNonRate = this.lock.newCondition();
      this.rateToRestore = null;
      this.scheduledValidations = new AtomicLong();
      this.eventExecutor = DebuggableThreadPoolExecutor.createWithMaximumPoolSize("ValidationSchedulerEventExecutor", 8, 60, TimeUnit.SECONDS);
      this.state = state;
   }

   private NodeSyncTracing tracing() {
      return this.state.service().tracing();
   }

   void addUserValidation(UserValidationOptions options) {
      UserValidationProposer proposer = UserValidationProposer.create(this.state, options);
      if(this.userValidations.putIfAbsent(proposer.id(), proposer) != null) {
         throw new IllegalStateException(String.format("Cannot submit user validation with identifier %s as that identifier is already used by an ongoing validation", new Object[]{proposer.id()}));
      } else {
         proposer.completionFuture().whenComplete((s, e) -> {
            this.userValidations.remove(proposer.id());
            if(e != null && !(e instanceof CancellationException)) {
               logger.error("Unexpected error during user triggered validation #{} on table {}", new Object[]{proposer.id(), options.table, e});
            } else {
               logger.info("User triggered validation #{} on table {} {}", new Object[]{proposer.id(), options.table, e == null?"finished successfully":"was cancelled"});
            }

         });
         this.lock.lock();

         try {
            this.pendingUserValidations.offer(proposer);
            this.hasProposals.signalAll();
            int pendingBefore = (this.currentUserValidation == null?0:1) + this.pendingUserValidations.size() - 1;
            if(pendingBefore > 0) {
               logger.info("Created user triggered validation #{} on table {}: queued after {} other user validations", new Object[]{proposer.id(), options.table, Integer.valueOf(pendingBefore)});
            }
         } finally {
            this.lock.unlock();
         }

      }
   }

   @Nullable
   ContinuousValidationProposer getContinuousProposer(TableMetadata table) {
      return (ContinuousValidationProposer)this.continuousValidations.get(table.id);
   }

   @Nullable
   UserValidationProposer getUserValidation(UserValidationID id) {
      return (UserValidationProposer)this.userValidations.get(id);
   }

   private CompletableFuture<ContinuousValidationProposer> addContinuous(TableMetadata table) {
      ContinuousValidationProposer proposer = (ContinuousValidationProposer)this.continuousValidations.get(table.id);
      return proposer != null?CompletableFuture.completedFuture(proposer):CompletableFuture.supplyAsync(() -> {
         TableState tableState = this.state.getOrLoad(table);
         this.lock.lock();

         ContinuousValidationProposer var3;
         try {
            var3 = this.addContinuousInternal(tableState);
         } finally {
            this.lock.unlock();
         }

         return var3;
      }, this.eventExecutor).exceptionally((t) -> {
         if(!Throwables.isCausedBy(t, UnknownTableException.class)) {
            logger.error("Unexpected error while starting NodeSync on {} following the table creation; The table will not be validated by NodeSync on this node until this is resolved", table, Throwables.unwrapped(t));
         }

         throw Throwables.cleaned(t);
      });
   }

   CompletableFuture<List<TableMetadata>> addAllContinuous(Stream<TableMetadata> tables) {
      List<CompletableFuture<TableState>> stateLoaders = (List)tables.map((t) -> {
         return CompletableFuture.supplyAsync(() -> {
            return this.state.getOrLoad(t);
         }, this.eventExecutor);
      }).collect(Collectors.toList());
      return stateLoaders.isEmpty()?CompletableFuture.completedFuture(Collections.emptyList()):CompletableFutures.allAsList(stateLoaders).thenApply((states) -> {
         this.lock.lock();

         try {
            List<TableMetadata> added = new ArrayList(states.size());
            Iterator var3 = states.iterator();

            while(var3.hasNext()) {
               TableState state = (TableState)var3.next();
               if(this.addContinuousInternal(state) == null) {
                  added.add(state.table());
               }
            }

            ArrayList var8 = added;
            return var8;
         } finally {
            this.lock.unlock();
         }
      });
   }

   private boolean removeContinuous(TableMetadata table) {
      this.lock.lock();

      boolean var2;
      try {
         var2 = this.cancelAndRemove(table);
      } finally {
         this.lock.unlock();
      }

      return var2;
   }

   private ContinuousValidationProposer addContinuousInternal(TableState tableState) {
      TableId id = tableState.table().id;
      ContinuousValidationProposer previous = (ContinuousValidationProposer)this.continuousValidations.get(id);
      if(previous != null) {
         return previous;
      } else {
         ContinuousValidationProposer proposer = new ContinuousValidationProposer(tableState, this::queueProposal);
         this.continuousValidations.put(id, proposer);
         this.tracing().trace("Adding continuous proposer for {}", new Object[]{tableState.table()});
         proposer.start();
         return null;
      }
   }

   private boolean cancelAndRemove(TableMetadata table) {
      ContinuousValidationProposer proposer = (ContinuousValidationProposer)this.continuousValidations.remove(table.id);
      if(proposer == null) {
         return false;
      } else {
         this.tracing().trace("Removing continuous proposer for {}", new Object[]{table});
         proposer.cancel();
         return true;
      }
   }

   int continuouslyValidatedTables() {
      return this.continuousValidations.size();
   }

   long scheduledValidations() {
      return this.scheduledValidations.get();
   }

   Validator getNextValidation(boolean blockUntilAvailable) {
      if(this.isShutdown) {
         throw new ValidationScheduler.ShutdownException();
      } else {
         Validator validator;
         do {
            ValidationProposal proposal = this.getProposal(blockUntilAvailable);
            if(proposal == null) {
               return null;
            }

            validator = proposal.activate();
         } while(validator == null);

         this.scheduledValidations.incrementAndGet();
         return validator;
      }
   }

   void shutdown() {
      this.lock.lock();

      try {
         if(this.isShutdown) {
            return;
         }

         this.continuousValidations.values().forEach(ValidationProposer::cancel);
         this.continuousValidations.clear();
         this.continuousProposals.clear();
         this.userValidations.values().forEach(ValidationProposer::cancel);
         this.userValidations.clear();
         this.pendingUserValidations.clear();
         this.currentUserValidation = null;
         this.isShutdown = true;
         this.hasProposals.signalAll();
         this.hasNonRate.signalAll();
      } finally {
         this.lock.unlock();
      }

   }

   private ValidationProposal getProposal(boolean blockUntilAvailable) {
      this.lock.lock();

      try {
         while(true) {
            ValidationProposal proposal;
            ValidationProposal var3;
            if((proposal = this.nextProposal(blockUntilAvailable)) == null) {
               if(blockUntilAvailable) {
                  if(this.isShutdown) {
                     throw new ValidationScheduler.ShutdownException();
                  }

                  this.hasProposals.awaitUninterruptibly();
                  continue;
               }

               var3 = null;
               return var3;
            }

            var3 = proposal;
            return var3;
         }
      } finally {
         this.lock.unlock();
      }
   }

   private ValidationProposal nextProposal(boolean blockUntilAvailable) {
      ValidationProposal p = this.nextUserValidationProposal(blockUntilAvailable);
      return p != null?p:(ValidationProposal)this.continuousProposals.poll();
   }

   private ValidationProposal nextUserValidationProposal(boolean blockUntilAvailable) {
      if(this.currentUserValidation == null || !this.currentUserValidation.hasNext()) {
         while(true) {
            while(this.rateToRestore == null) {
               this.currentUserValidation = (UserValidationProposer)this.pendingUserValidations.poll();
               if(this.currentUserValidation == null) {
                  return null;
               }

               this.currentUserValidation.rate().ifPresent((rate) -> {
                  this.rateToRestore = this.config.getRate();
                  this.config.setRate(rate);
                  this.currentUserValidation.completionFuture().whenComplete((s, e) -> {
                     this.lock.lock();

                     try {
                        this.config.setRate(this.rateToRestore);
                        this.rateToRestore = null;
                        this.hasNonRate.signal();
                     } finally {
                        this.lock.unlock();
                     }

                  });
               });
               logger.info("Starting user triggered validation #{} on table {}", this.currentUserValidation.id(), this.currentUserValidation.table());
               if(this.currentUserValidation.hasNext()) {
                  return this.currentUserValidation.next();
               }
            }

            if(!blockUntilAvailable) {
               return null;
            }

            if(this.isShutdown) {
               throw new ValidationScheduler.ShutdownException();
            }

            this.hasNonRate.awaitUninterruptibly();
         }
      } else {
         return this.currentUserValidation.next();
      }
   }

   void setRate(RateValue rate) {
      this.lock.lock();

      try {
         if(this.rateToRestore != null) {
            throw new IllegalStateException("Cannot set NodeSync rate because a user triggered validation with custom rate is running: " + this.currentUserValidation.id());
         }

         this.config.setRate(rate);
      } finally {
         this.lock.unlock();
      }

   }

   private void queueProposal(ContinuousValidationProposer.Proposal proposal) {
      this.lock.lock();

      try {
         if(this.isShutdown) {
            return;
         }

         this.continuousProposals.add(proposal);
         this.hasProposals.signal();
      } finally {
         this.lock.unlock();
      }

   }

   public void onJoinCluster(InetAddress endpoint) {
      if(this.continuouslyValidatedTables() == 0) {
         this.addAllContinuous(NodeSyncHelpers.nodeSyncEnabledTables()).thenAccept((tables) -> {
            if(!tables.isEmpty()) {
               logger.info("{} has joined the cluster: starting NodeSync validations on tables {} as consequence", endpoint, tables);
            }

         });
      } else {
         this.maybeUpdateLocalRanges();
      }

   }

   private void maybeUpdateLocalRanges() {
      Iterator var1 = this.continuousValidations.values().iterator();

      while(var1.hasNext()) {
         ContinuousValidationProposer proposer = (ContinuousValidationProposer)var1.next();
         this.eventExecutor.submit(() -> {
            proposer.state.update(NodeSyncHelpers.localRanges(proposer.table().keyspace));
         });
      }

   }

   public void onLeaveCluster(InetAddress endpoint) {
      this.maybeUpdateLocalRanges();
   }

   public void onUp(InetAddress endpoint) {
   }

   public void onDown(InetAddress endpoint) {
   }

   public void onMove(InetAddress endpoint) {
      this.maybeUpdateLocalRanges();
   }

   public void onAlterKeyspace(String keyspace) {
      Keyspace ks = Schema.instance.getKeyspaceInstance(keyspace);
      if(ks != null) {
         if(NodeSyncHelpers.isReplicated(ks)) {
            this.addAllContinuous(NodeSyncHelpers.nodeSyncEnabledTables(ks)).thenAccept((tables) -> {
               if(!tables.isEmpty()) {
                  logger.info("Starting NodeSync validations on tables {} following increase of the replication factor on {}", tables, keyspace);
               }

               this.maybeUpdateLocalRanges();
            });
         } else {
            Set<TableMetadata> removed = new HashSet();
            ks.getColumnFamilyStores().forEach((s) -> {
               if(this.removeContinuous(s.metadata())) {
                  removed.add(s.metadata());
               }

            });
            if(!removed.isEmpty()) {
               logger.info("Stopping NodeSync validations on tables {} because keyspace {} is not replicated anymore", removed, keyspace);
            }
         }

      }
   }

   public void onCreateTable(String keyspace, String table) {
      ColumnFamilyStore store = ColumnFamilyStore.getIfExists(keyspace, table);
      if(store != null && NodeSyncHelpers.isNodeSyncEnabled(store)) {
         this.addContinuous(store.metadata()).thenAccept((previous) -> {
            if(previous == null) {
               logger.info("Starting NodeSync validations on newly created table {}", store.metadata());
            }

         });
      }
   }

   public void onAlterTable(String keyspace, String table, boolean affectsStatements) {
      ColumnFamilyStore store = ColumnFamilyStore.getIfExists(keyspace, table);
      if(store != null) {
         if(StorageService.instance.getTokenMetadata().getAllEndpoints().size() != 1) {
            TableMetadata metadata = store.metadata();
            if(NodeSyncHelpers.isNodeSyncEnabled(store)) {
               CompletableFuture.runAsync(() -> {
                  this.state.getOrLoad(metadata).onTableUpdate(metadata);
               }, this.eventExecutor).thenCompose((v) -> {
                  return this.addContinuous(metadata);
               }).thenAccept((previous) -> {
                  if(previous == null) {
                     logger.info("Starting NodeSync validations on table {}: it has been enabled with ALTER TABLE", metadata);
                  }

               });
            } else {
               this.eventExecutor.submit(() -> {
                  if(this.removeContinuous(metadata)) {
                     logger.info("Stopping NodeSync validations on table {} following user deactivation", metadata);
                  }

               });
            }

         }
      }
   }

   public void onDropTable(String keyspace, String table) {
      this.eventExecutor.execute(() -> {
         this.continuousValidations.values().stream().filter((p) -> {
            return p.table().keyspace.equals(keyspace) && p.table().name.equals(table);
         }).map((p) -> {
            return p.table().id;
         }).findAny().ifPresent((id) -> {
            ContinuousValidationProposer proposer = (ContinuousValidationProposer)this.continuousValidations.remove(id);
            if(proposer != null) {
               proposer.cancel();
            }

            logger.debug("Stopping NodeSync validations on table {}.{} as the table has been dropped", keyspace, table);
         });
         Iterator iter = this.userValidations.values().iterator();

         while(iter.hasNext()) {
            UserValidationProposer proposer = (UserValidationProposer)iter.next();
            if(proposer.table().keyspace.equals(keyspace) && proposer.table().name.equals(table)) {
               proposer.cancel();
               iter.remove();
            }
         }

      });
   }

   static class ShutdownException extends RuntimeException {
      ShutdownException() {
      }
   }
}
