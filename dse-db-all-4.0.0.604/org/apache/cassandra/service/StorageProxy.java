package org.apache.cassandra.service;

import com.google.common.base.Function;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.collect.PeekingIterator;
import com.google.common.primitives.Ints;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Uninterruptibles;
import io.reactivex.Completable;
import io.reactivex.CompletableSource;
import io.reactivex.Single;
import io.reactivex.functions.Action;
import io.reactivex.functions.Consumer;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.cassandra.batchlog.Batch;
import org.apache.cassandra.batchlog.BatchRemove;
import org.apache.cassandra.batchlog.BatchlogManager;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.DigestVersion;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.db.HintedHandOffManager;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.PartitionPosition;
import org.apache.cassandra.db.PartitionRangeReadCommand;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.ReadContext;
import org.apache.cassandra.db.SinglePartitionReadCommand;
import org.apache.cassandra.db.Truncation;
import org.apache.cassandra.db.WriteType;
import org.apache.cassandra.db.filter.DataLimits;
import org.apache.cassandra.db.partitions.FilteredPartition;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.FlowablePartition;
import org.apache.cassandra.db.rows.FlowablePartitions;
import org.apache.cassandra.db.rows.RowIterator;
import org.apache.cassandra.dht.AbstractBounds;
import org.apache.cassandra.dht.Bounds;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.ReadFailureException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestFailureException;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteFailureException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.hints.Hint;
import org.apache.cassandra.hints.HintsService;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.LocalStrategy;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.metrics.CASClientRequestMetrics;
import org.apache.cassandra.metrics.CASClientWriteRequestMetrics;
import org.apache.cassandra.metrics.ClientRequestMetrics;
import org.apache.cassandra.metrics.ClientWriteRequestMetrics;
import org.apache.cassandra.metrics.HintedHandoffMetrics;
import org.apache.cassandra.metrics.ReadRepairMetrics;
import org.apache.cassandra.metrics.StorageMetrics;
import org.apache.cassandra.metrics.ViewWriteMetrics;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.FailureResponse;
import org.apache.cassandra.net.MessageCallback;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.Request;
import org.apache.cassandra.net.Response;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.CommitCallback;
import org.apache.cassandra.service.paxos.PrepareCallback;
import org.apache.cassandra.service.paxos.ProposeCallback;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.triggers.TriggerExecutor;
import org.apache.cassandra.utils.AbstractIterator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.AsyncLatch;
import org.apache.cassandra.utils.flow.Flow;
import org.apache.cassandra.utils.flow.Threads;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StorageProxy implements StorageProxyMBean {
   public static final String MBEAN_NAME = "org.apache.cassandra.db:type=StorageProxy";
   private static final Logger logger = LoggerFactory.getLogger(StorageProxy.class);
   public static final String UNREACHABLE = "UNREACHABLE";
   public static final StorageProxy instance = new StorageProxy();
   private static volatile int maxHintsInProgress = 128 * FBUtilities.getAvailableProcessors();
   private static final CacheLoader<InetAddress, AtomicInteger> hintsInProgress = new CacheLoader<InetAddress, AtomicInteger>() {
      public AtomicInteger load(InetAddress inetAddress) {
         return new AtomicInteger(0);
      }
   };
   private static final ClientRequestMetrics readMetrics = new ClientRequestMetrics("Read");
   private static final ClientRequestMetrics rangeMetrics = new ClientRequestMetrics("RangeSlice");
   private static final ClientWriteRequestMetrics writeMetrics = new ClientWriteRequestMetrics("Write");
   private static final CASClientWriteRequestMetrics casWriteMetrics = new CASClientWriteRequestMetrics("CASWrite");
   private static final CASClientRequestMetrics casReadMetrics = new CASClientRequestMetrics("CASRead");
   private static final ViewWriteMetrics viewWriteMetrics = new ViewWriteMetrics("ViewWrite");
   private static final Map<ConsistencyLevel, ClientRequestMetrics> readMetricsMap = new EnumMap(ConsistencyLevel.class);
   private static final Map<ConsistencyLevel, ClientWriteRequestMetrics> writeMetricsMap = new EnumMap(ConsistencyLevel.class);
   private static final double CONCURRENT_SUBREQUESTS_MARGIN = 0.1D;

   private StorageProxy() {
   }

   public static Optional<RowIterator> cas(String keyspaceName, String cfName, DecoratedKey key, CASRequest request, ConsistencyLevel consistencyForPaxos, ConsistencyLevel consistencyForCommit, ClientState state, long queryStartNanoTime) throws UnavailableException, IsBootstrappingException, RequestFailureException, RequestTimeoutException, InvalidRequestException {
      if(logger.isTraceEnabled()) {
         logger.trace("Execute cas on {}.{} for pk {}", new Object[]{keyspaceName, cfName, key});
      }

      long startTimeForMetrics = System.nanoTime();
      int contentions = 0;
      boolean var37 = false;

      long size;
      Optional var42;
      label101: {
         Optional var27;
         try {
            var37 = true;
            consistencyForPaxos.validateForCas();
            consistencyForCommit.validateForCasCommit(keyspaceName);
            TableMetadata metadata = Schema.instance.getTableMetadata(keyspaceName, cfName);
            long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getCasContentionTimeout());

            while(true) {
               if(System.nanoTime() - queryStartNanoTime >= timeout) {
                  throw new WriteTimeoutException(WriteType.CAS, String.format("CAS timed out due to contention - tried %d times", new Object[]{Integer.valueOf(contentions)}), consistencyForPaxos, 0, consistencyForPaxos.blockFor(Keyspace.open(metadata.keyspace)));
               }

               Pair<WriteEndpoints, Integer> p = getPaxosParticipants(metadata, key, consistencyForPaxos);
               List<InetAddress> liveEndpoints = ((WriteEndpoints)p.left).live();
               int requiredParticipants = ((Integer)p.right).intValue();
               Pair<UUID, Integer> pair = beginAndRepairPaxos(queryStartNanoTime, key, metadata, liveEndpoints, requiredParticipants, consistencyForPaxos, consistencyForCommit, true, state);
               UUID ballot = (UUID)pair.left;
               contentions += ((Integer)pair.right).intValue();
               Tracing.trace("Reading existing values for CAS precondition");
               SinglePartitionReadCommand readCommand = request.readCommand(FBUtilities.nowInSeconds());
               ConsistencyLevel readConsistency = consistencyForPaxos == ConsistencyLevel.LOCAL_SERIAL?ConsistencyLevel.LOCAL_QUORUM:ConsistencyLevel.QUORUM;
               FilteredPartition current = readOne(readCommand, readConsistency, queryStartNanoTime);
               if(!request.appliesTo(current)) {
                  Tracing.trace("CAS precondition does not match current values {}", (Object)current);
                  casWriteMetrics.conditionNotMet.inc();
                  var42 = Optional.of(current.rowIterator());
                  var37 = false;
                  break label101;
               }

               PartitionUpdate updates = request.makeUpdates(current);
               size = (long)updates.dataSize();
               casWriteMetrics.mutationSize.update(size);
               ((ClientWriteRequestMetrics)writeMetricsMap.get(consistencyForPaxos)).mutationSize.update(size);
               updates = TriggerExecutor.instance.execute(updates);
               Commit proposal = Commit.newProposal(ballot, updates);
               Tracing.trace("CAS precondition is met; proposing client-requested updates for {}", (Object)ballot);
               if(proposePaxos(proposal, liveEndpoints, requiredParticipants, true, consistencyForPaxos, queryStartNanoTime)) {
                  commitPaxos(proposal, consistencyForCommit, true, queryStartNanoTime);
                  Tracing.trace("CAS successful");
                  var27 = Optional.empty();
                  var37 = false;
                  break;
               }

               Tracing.trace("Paxos proposal not accepted (pre-empted by a higher ballot)");
               ++contentions;
               Uninterruptibles.sleepUninterruptibly((long)ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
            }
         } catch (ReadTimeoutException | WriteTimeoutException var38) {
            casWriteMetrics.timeouts.mark();
            ((ClientWriteRequestMetrics)writeMetricsMap.get(consistencyForPaxos)).timeouts.mark();
            throw var38;
         } catch (ReadFailureException | WriteFailureException var39) {
            casWriteMetrics.failures.mark();
            ((ClientWriteRequestMetrics)writeMetricsMap.get(consistencyForPaxos)).failures.mark();
            throw var39;
         } catch (UnavailableException var40) {
            casWriteMetrics.unavailables.mark();
            ((ClientWriteRequestMetrics)writeMetricsMap.get(consistencyForPaxos)).unavailables.mark();
            throw var40;
         } finally {
            if(var37) {
               recordCasContention(contentions);
               long latency = System.nanoTime() - startTimeForMetrics;
               casWriteMetrics.addNano(latency);
               ((ClientWriteRequestMetrics)writeMetricsMap.get(consistencyForPaxos)).addNano(latency);
            }
         }

         recordCasContention(contentions);
         long latency = System.nanoTime() - startTimeForMetrics;
         casWriteMetrics.addNano(latency);
         ((ClientWriteRequestMetrics)writeMetricsMap.get(consistencyForPaxos)).addNano(latency);
         return var27;
      }

      recordCasContention(contentions);
      size = System.nanoTime() - startTimeForMetrics;
      casWriteMetrics.addNano(size);
      ((ClientWriteRequestMetrics)writeMetricsMap.get(consistencyForPaxos)).addNano(size);
      return var42;
   }

   private static void recordCasContention(int contentions) {
      if(contentions > 0) {
         casWriteMetrics.contention.update(contentions);
      }

   }

   private static Pair<WriteEndpoints, Integer> getPaxosParticipants(TableMetadata table, DecoratedKey key, ConsistencyLevel consistencyForPaxos) throws UnavailableException {
      WriteEndpoints endpoints = WriteEndpoints.compute(table.keyspace, key);
      if(consistencyForPaxos == ConsistencyLevel.LOCAL_SERIAL) {
         endpoints = endpoints.restrictToLocalDC();
      }

      int participants = endpoints.count();
      int requiredParticipants = participants / 2 + 1;
      if(endpoints.liveCount() < requiredParticipants) {
         throw new UnavailableException(consistencyForPaxos, requiredParticipants, endpoints.liveCount());
      } else if(endpoints.pendingCount() > 1) {
         throw new UnavailableException(String.format("Cannot perform LWT operation as there is more than one (%d) pending range movement", new Object[]{Integer.valueOf(endpoints.pendingCount())}), consistencyForPaxos, participants + 1, endpoints.liveCount());
      } else {
         return Pair.create(endpoints, Integer.valueOf(requiredParticipants));
      }
   }

   private static Flow<Pair<UUID, Integer>> beginAndRepairPaxosAsync(long queryStartNanoTime, DecoratedKey key, TableMetadata metadata, List<InetAddress> liveEndpoints, int requiredParticipants, ConsistencyLevel consistencyForPaxos, ConsistencyLevel consistencyForCommit, boolean isWrite, ClientState state) {
      return Flow.fromCallable(() -> {
         return beginAndRepairPaxos(queryStartNanoTime, key, metadata, liveEndpoints, requiredParticipants, consistencyForPaxos, consistencyForCommit, isWrite, state);
      }).lift(Threads.requestOnIo(TPCTaskType.EXECUTE_STATEMENT));
   }

   private static Pair<UUID, Integer> beginAndRepairPaxos(long queryStartNanoTime, DecoratedKey key, TableMetadata metadata, List<InetAddress> liveEndpoints, int requiredParticipants, ConsistencyLevel consistencyForPaxos, ConsistencyLevel consistencyForCommit, boolean isWrite, ClientState state) throws WriteTimeoutException, WriteFailureException {
      long timeout = TimeUnit.MILLISECONDS.toNanos(DatabaseDescriptor.getCasContentionTimeout());
      PrepareCallback summary = null;
      int contentions = 0;

      while(true) {
         while(System.nanoTime() - queryStartNanoTime < timeout) {
            long minTimestampMicrosToUse = summary == null?-9223372036854775808L:1L + UUIDGen.microsTimestamp(summary.mostRecentInProgressCommit.ballot);
            long ballotMicros = state.getTimestampForPaxos(minTimestampMicrosToUse);
            UUID ballot = UUIDGen.getRandomTimeUUIDFromMicros(ballotMicros);
            Tracing.trace("Preparing {}", (Object)ballot);
            Commit toPrepare = Commit.newPrepare(key, metadata, ballot);
            summary = preparePaxos(toPrepare, liveEndpoints, requiredParticipants, consistencyForPaxos, queryStartNanoTime);
            if(summary.promised) {
               Commit inProgress = summary.mostRecentInProgressCommitWithUpdate;
               Commit mostRecent = summary.mostRecentCommit;
               if(inProgress.update.isEmpty() || !inProgress.isAfter(mostRecent)) {
                  int nowInSec = Ints.checkedCast(TimeUnit.MICROSECONDS.toSeconds(ballotMicros));
                  Iterable<InetAddress> missingMRC = summary.replicasMissingMostRecentCommit(metadata, nowInSec);
                  if(Iterables.size(missingMRC) > 0) {
                     Tracing.trace("Repairing replicas that missed the most recent commit");
                     commitPaxosAndWaitAll(mostRecent, ImmutableList.copyOf(missingMRC));
                  }

                  return Pair.create(ballot, Integer.valueOf(contentions));
               }

               Tracing.trace("Finishing incomplete paxos round {}", (Object)inProgress);
               if(isWrite) {
                  casWriteMetrics.unfinishedCommit.inc();
               } else {
                  casReadMetrics.unfinishedCommit.inc();
               }

               Commit refreshedInProgress = Commit.newProposal(ballot, inProgress.update);
               if(proposePaxos(refreshedInProgress, liveEndpoints, requiredParticipants, false, consistencyForPaxos, queryStartNanoTime)) {
                  try {
                     commitPaxos(refreshedInProgress, consistencyForCommit, false, queryStartNanoTime);
                  } catch (WriteTimeoutException var24) {
                     recordCasContention(contentions);
                     throw new WriteTimeoutException(WriteType.CAS, var24.consistency, var24.received, var24.blockFor);
                  }
               } else {
                  Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
                  ++contentions;
                  Uninterruptibles.sleepUninterruptibly((long)ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
               }
            } else {
               Tracing.trace("Some replicas have already promised a higher ballot than ours; aborting");
               ++contentions;
               Uninterruptibles.sleepUninterruptibly((long)ThreadLocalRandom.current().nextInt(100), TimeUnit.MILLISECONDS);
            }
         }

         recordCasContention(contentions);
         throw new WriteTimeoutException(WriteType.CAS, String.format("CAS timed out due to contention - tried %d times", new Object[]{Integer.valueOf(contentions)}), consistencyForPaxos, 0, consistencyForPaxos.blockFor(Keyspace.open(metadata.keyspace)));
      }
   }

   private static PrepareCallback preparePaxos(Commit toPrepare, List<InetAddress> endpoints, int requiredParticipants, ConsistencyLevel consistencyForPaxos, long queryStartNanoTime) throws WriteTimeoutException {
      PrepareCallback callback = new PrepareCallback(toPrepare.update.partitionKey(), toPrepare.update.metadata(), requiredParticipants, consistencyForPaxos, queryStartNanoTime);
      MessagingService.instance().send((Request.Dispatcher)Verbs.LWT.PREPARE.newDispatcher(endpoints, toPrepare), callback);
      callback.await();
      return callback;
   }

   private static boolean proposePaxos(Commit proposal, List<InetAddress> endpoints, int requiredParticipants, boolean timeoutIfPartial, ConsistencyLevel consistencyLevel, long queryStartNanoTime) throws WriteTimeoutException {
      ProposeCallback callback = new ProposeCallback(endpoints.size(), requiredParticipants, !timeoutIfPartial, consistencyLevel, queryStartNanoTime);
      MessagingService.instance().send((Request.Dispatcher)Verbs.LWT.PROPOSE.newDispatcher(endpoints, proposal), callback);
      callback.await();
      if(callback.isSuccessful()) {
         return true;
      } else if(timeoutIfPartial && !callback.isFullyRefused()) {
         throw new WriteTimeoutException(WriteType.CAS, String.format("CAS propose partially refused - received only %d accepts out of %d required", new Object[]{Integer.valueOf(callback.getAcceptCount()), Integer.valueOf(requiredParticipants)}), consistencyLevel, callback.getAcceptCount(), requiredParticipants);
      } else {
         return false;
      }
   }

   private static void commitPaxosAndWaitAll(Commit proposal, List<InetAddress> endpoints) throws WriteTimeoutException {
      CommitCallback callback = new CommitCallback(endpoints.size(), ConsistencyLevel.ALL, System.nanoTime());
      MessagingService.instance().send((Request.Dispatcher)Verbs.LWT.COMMIT.newDispatcher(endpoints, proposal), callback);
      callback.await();
      Map<InetAddress, RequestFailureReason> failureReasons = callback.getFailureReasons();
      if(!failureReasons.isEmpty()) {
         throw new WriteFailureException(ConsistencyLevel.ALL, callback.getResponseCount(), endpoints.size(), WriteType.CAS, failureReasons);
      }
   }

   private static void commitPaxos(Commit proposal, ConsistencyLevel consistencyLevel, boolean shouldHint, long queryStartNanoTime) throws WriteTimeoutException {
      commitPaxos(proposal, WriteEndpoints.compute(proposal), consistencyLevel, shouldHint, queryStartNanoTime);
   }

   private static void commitPaxos(Commit proposal, WriteEndpoints endpoints, ConsistencyLevel consistencyLevel, boolean shouldHint, long queryStartNanoTime) throws WriteTimeoutException {
      Mutation mutation = proposal.makeMutation();
      checkHintOverload(endpoints);
      if(shouldHint) {
         maybeSubmitHint(mutation, (Iterable)endpoints.dead(), (WriteHandler)null);
      }

      WriteHandler.Builder builder = WriteHandler.builder(endpoints, consistencyLevel, WriteType.SIMPLE, queryStartNanoTime, TPC.bestTPCTimer()).withIdealConsistencyLevel(DatabaseDescriptor.getIdealConsistencyLevel());
      if(shouldHint) {
         builder.hintOnTimeout(mutation).hintOnFailure(mutation);
      }

      WriteHandler handler = builder.build();
      MessagingService.instance().send((Request.Dispatcher)Verbs.LWT.COMMIT.newDispatcher(endpoints.live(), proposal), handler);
      if(consistencyLevel != ConsistencyLevel.ANY) {
         handler.get();
      }

   }

   public static Single<ResultMessage.Void> mutate(Collection<? extends IMutation> mutations, ConsistencyLevel consistencyLevel, long queryStartNanoTime) throws UnavailableException, OverloadedException, WriteTimeoutException, WriteFailureException {
      Tracing.trace("Determining replicas for mutation");
      long startTime = System.nanoTime();
      ArrayList responseHandlers = new ArrayList(mutations.size());

      try {
         Iterator var7 = mutations.iterator();

         while(var7.hasNext()) {
            IMutation mutation = (IMutation)var7.next();
            if(mutation instanceof CounterMutation) {
               responseHandlers.add(mutateCounter((CounterMutation)mutation, queryStartNanoTime));
            } else {
               WriteType wt = mutations.size() <= 1?WriteType.SIMPLE:WriteType.UNLOGGED_BATCH;
               responseHandlers.add(mutateStandard((Mutation)mutation, consistencyLevel, wt, queryStartNanoTime));
            }
         }
      } catch (OverloadedException | UnavailableException var10) {
         if(logger.isTraceEnabled()) {
            logger.trace("Unavailable or overloaded exception", var10);
         }

         writeMetrics.unavailables.mark();
         ((ClientWriteRequestMetrics)writeMetricsMap.get(consistencyLevel)).unavailables.mark();
         Tracing.trace("Unavailable");
         return Single.error(var10);
      }

      Completable ret = Completable.concat(Lists.transform(responseHandlers, WriteHandler::toObservable));
      return ret.onErrorResumeNext((ex) -> {
         if(logger.isTraceEnabled()) {
            logger.trace("Failed to wait for handlers", ex);
         }

         if(!(ex instanceof WriteTimeoutException) && !(ex instanceof WriteFailureException)) {
            if(ex instanceof UnavailableException) {
               writeMetrics.unavailables.mark();
               ((ClientWriteRequestMetrics)writeMetricsMap.get(consistencyLevel)).unavailables.mark();
               Tracing.trace("Unavailable");
            } else if(ex instanceof OverloadedException) {
               writeMetrics.unavailables.mark();
               ((ClientWriteRequestMetrics)writeMetricsMap.get(consistencyLevel)).unavailables.mark();
               Tracing.trace("Overloaded");
            }
         } else {
            if(consistencyLevel == ConsistencyLevel.ANY) {
               hintMutations(mutations);
               return Completable.complete();
            }

            if(ex instanceof WriteFailureException) {
               writeMetrics.failures.mark();
               ((ClientWriteRequestMetrics)writeMetricsMap.get(consistencyLevel)).failures.mark();
               WriteFailureException fe = (WriteFailureException)ex;
               Tracing.trace("Write failure; received {} of {} required replies, failed {} requests", new Object[]{Integer.valueOf(fe.received), Integer.valueOf(fe.blockFor), Integer.valueOf(fe.failureReasonByEndpoint.size())});
            } else {
               writeMetrics.timeouts.mark();
               ((ClientWriteRequestMetrics)writeMetricsMap.get(consistencyLevel)).timeouts.mark();
               WriteTimeoutException te = (WriteTimeoutException)ex;
               Tracing.trace("Write timeout; received {} of {} required replies", Integer.valueOf(te.received), Integer.valueOf(te.blockFor));
            }
         }

         return Completable.error(ex);
      }).doFinally(() -> {
         recordLatency(consistencyLevel, startTime);
      }).toSingleDefault(new ResultMessage.Void());
   }

   private static void hintMutations(Collection<? extends IMutation> mutations) {
      Iterator var1 = mutations.iterator();

      while(var1.hasNext()) {
         IMutation mutation = (IMutation)var1.next();
         if(!(mutation instanceof CounterMutation)) {
            hintMutation((Mutation)mutation);
         }
      }

      Tracing.trace("Wrote hints to satisfy CL.ANY after no replicas acknowledged the write");
   }

   private static void hintMutation(Mutation mutation) {
      String keyspaceName = mutation.getKeyspaceName();
      Token token = mutation.key().getToken();
      Iterable<InetAddress> endpoints = StorageService.instance.getNaturalAndPendingEndpoints(keyspaceName, token);
      maybeSubmitHint(mutation, (Iterable)endpoints, (WriteHandler)null);
   }

   public boolean appliesLocally(Mutation mutation) {
      String keyspaceName = mutation.getKeyspaceName();
      Token token = mutation.key().getToken();
      InetAddress local = FBUtilities.getBroadcastAddress();
      return StorageService.instance.getNaturalEndpoints((String)keyspaceName, (RingPosition)token).contains(local) || StorageService.instance.getTokenMetadata().pendingEndpointsFor(token, keyspaceName).contains(local);
   }

   public static Completable mutateMV(ByteBuffer dataKey, Collection<Mutation> mutations, boolean writeCommitLog, AtomicLong baseComplete, long queryStartNanoTime) throws UnavailableException, OverloadedException, WriteTimeoutException {
      Tracing.trace("Determining replicas for mutation");
      long startTime = System.nanoTime();
      UUID batchUUID = UUIDGen.getTimeUUID();
      if(!StorageService.instance.isStarting() && !StorageService.instance.isJoining() && !StorageService.instance.isMoving()) {
         List<StorageProxy.MutationAndEndpoints> mutationsAndEndpoints = new ArrayList(mutations.size());
         List<Mutation> nonLocalMutations = new ArrayList(mutations.size());
         Token baseToken = StorageService.instance.getTokenMetadata().partitioner.getToken(dataKey);
         int latchCount = 0;
         ArrayList<Completable> completables = new ArrayList(mutations.size() + 1);
         Iterator var14 = mutations.iterator();

         while(true) {
            while(var14.hasNext()) {
               Mutation mutation = (Mutation)var14.next();
               WriteEndpoints endpoints = WriteEndpoints.computeForView(baseToken, mutation);
               if(endpoints.naturalCount() == 0) {
                  if(endpoints.pendingCount() == 0) {
                     logger.warn("Received base materialized view mutation for key {} that does not belong to this node. There is probably a range movement happening (move or decommission),but this node hasn't updated its ring metadata yet. Adding mutation to local batchlog to be replayed later.", mutation.key());
                  }
               } else {
                  InetAddress endpoint = (InetAddress)endpoints.natural().get(0);
                  if(endpoint.equals(FBUtilities.getBroadcastAddress()) && endpoints.pendingCount() == 0 && StorageService.instance.isJoined()) {
                     completables.add(mutation.applyAsync(writeCommitLog, true).doOnError((exc) -> {
                        logger.error("Error applying local view update to keyspace {}: {}", mutation.getKeyspaceName(), mutation);
                     }));
                  } else {
                     mutationsAndEndpoints.add(new StorageProxy.MutationAndEndpoints(mutation, endpoints));
                     nonLocalMutations.add(mutation);
                     ++latchCount;
                  }
               }
            }

            if(mutationsAndEndpoints.isEmpty()) {
               return Completable.merge(completables).doFinally(() -> {
                  viewWriteMetrics.addNano(System.nanoTime() - startTime);
               });
            }

            Completable batchlogCompletable = null;
            if(!nonLocalMutations.isEmpty()) {
               batchlogCompletable = BatchlogManager.store(Batch.createLocal(batchUUID, FBUtilities.timestampMicros(), nonLocalMutations), writeCommitLog);
            }

            assert batchlogCompletable != null;

            AsyncLatch cleanupLatch = new AsyncLatch((long)latchCount, () -> {
               asyncRemoveFromBatchlog(Collections.singletonList(FBUtilities.getBroadcastAddress()), batchUUID);
            });
            java.util.function.Consumer<Response<EmptyPayload>> onReplicaResponse = (r) -> {
               viewWriteMetrics.viewReplicasSuccess.inc();
               long delay = Math.max(0L, System.currentTimeMillis() - baseComplete.get());
               viewWriteMetrics.viewWriteLatency.update(delay, TimeUnit.MILLISECONDS);
            };
            List<WriteHandler> handlers = ImmutableList.copyOf(Lists.transform(mutationsAndEndpoints, (mae) -> {
               viewWriteMetrics.viewReplicasAttempted.inc((long)mae.endpoints.liveCount());
               WriteHandler handler = WriteHandler.builder(mae.endpoints, ConsistencyLevel.ONE, WriteType.BATCH, queryStartNanoTime, TPC.bestTPCTimer()).onResponse(onReplicaResponse).build();
               handler.thenRun(cleanupLatch::countDown);
               return handler;
            }));
            completables.add(batchlogCompletable.doOnComplete(() -> {
               writeBatchedMutations(mutationsAndEndpoints, handlers, Verbs.WRITES.VIEW_WRITE);
            }));
            return Completable.merge(completables).doFinally(() -> {
               viewWriteMetrics.addNano(System.nanoTime() - startTime);
            });
         }
      } else {
         return BatchlogManager.store(Batch.createLocal(batchUUID, FBUtilities.timestampMicros(), mutations), writeCommitLog).doFinally(() -> {
            viewWriteMetrics.addNano(System.nanoTime() - startTime);
         });
      }
   }

   public static Single<ResultMessage.Void> mutateWithTriggers(Collection<? extends IMutation> mutations, ConsistencyLevel consistencyLevel, boolean mutateAtomically, long queryStartNanoTime) throws WriteTimeoutException, WriteFailureException, UnavailableException, OverloadedException, InvalidRequestException {
      Collection<Mutation> augmented = TriggerExecutor.instance.execute(mutations);
      boolean updatesView = Keyspace.open(((IMutation)mutations.iterator().next()).getKeyspaceName()).viewManager.updatesAffectView(mutations, true);
      long size = IMutation.dataSize(mutations);
      writeMetrics.mutationSize.update(size);
      ((ClientWriteRequestMetrics)writeMetricsMap.get(consistencyLevel)).mutationSize.update(size);
      return augmented != null?mutateAtomically(augmented, consistencyLevel, updatesView, queryStartNanoTime).toSingleDefault(new ResultMessage.Void()):(!mutateAtomically && !updatesView?mutate(mutations, consistencyLevel, queryStartNanoTime):mutateAtomically(mutations, consistencyLevel, updatesView, queryStartNanoTime).toSingleDefault(new ResultMessage.Void()));
   }

   private static Completable mutateAtomically(Collection<Mutation> mutations, ConsistencyLevel consistencyLevel, boolean requireQuorumForRemove, long queryStartNanoTime) throws UnavailableException, OverloadedException, WriteTimeoutException {
      Tracing.trace("Determining replicas for atomic batch");
      long startTime = System.nanoTime();
      ArrayList mutationsAndEndpoints = new ArrayList(mutations.size());

      try {
         Iterator var8 = mutations.iterator();

         while(var8.hasNext()) {
            Mutation mutation = (Mutation)var8.next();
            WriteEndpoints endpoints = WriteEndpoints.compute((IMutation)mutation);
            endpoints.checkAvailability(consistencyLevel);
            mutationsAndEndpoints.add(new StorageProxy.MutationAndEndpoints(mutation, endpoints));
         }
      } catch (UnavailableException var15) {
         writeMetrics.unavailables.mark();
         ((ClientWriteRequestMetrics)writeMetricsMap.get(consistencyLevel)).unavailables.mark();
         Tracing.trace("Unavailable");
         return Completable.error(var15);
      }

      ConsistencyLevel batchConsistencyLevel = requireQuorumForRemove && !consistencyLevel.isAtLeastQuorum()?ConsistencyLevel.QUORUM:consistencyLevel;
      WriteEndpoints batchlogEndpoints = getBatchlogEndpoints(batchConsistencyLevel);
      UUID batchUUID = UUIDGen.getTimeUUID();
      Completable batchlogCompletable = writeToBatchlog(mutations, batchlogEndpoints, batchUUID, queryStartNanoTime).toObservable();
      AsyncLatch cleanupLatch = new AsyncLatch((long)mutations.size(), () -> {
         asyncRemoveFromBatchlog(batchlogEndpoints.live(), batchUUID);
      });
      List<WriteHandler> handlers = ImmutableList.copyOf(Lists.transform(mutationsAndEndpoints, (mae) -> {
         Keyspace keyspace = mae.endpoints.keyspace();
         long var10002 = (long)batchConsistencyLevel.blockFor(keyspace);
         cleanupLatch.getClass();
         AsyncLatch toCleanupLatch = new AsyncLatch(var10002, cleanupLatch::countDown);
         return WriteHandler.builder(mae.endpoints, consistencyLevel, WriteType.BATCH, queryStartNanoTime, TPC.bestTPCTimer()).withIdealConsistencyLevel(DatabaseDescriptor.getIdealConsistencyLevel()).onResponse((r) -> {
            toCleanupLatch.countDown();
         }).build();
      }));
      Completable ret = batchlogCompletable.andThen(Completable.defer(() -> {
         writeBatchedMutations(mutationsAndEndpoints, handlers, Verbs.WRITES.WRITE);
         return Completable.concat((Iterable)handlers.stream().map(WriteHandler::toObservable).collect(Collectors.toList()));
      }));
      return ret.onErrorResumeNext((e) -> {
         if(e instanceof UnavailableException) {
            writeMetrics.unavailables.mark();
            ((ClientWriteRequestMetrics)writeMetricsMap.get(consistencyLevel)).unavailables.mark();
            Tracing.trace("Unavailable");
         } else if(e instanceof WriteTimeoutException) {
            writeMetrics.timeouts.mark();
            ((ClientWriteRequestMetrics)writeMetricsMap.get(consistencyLevel)).timeouts.mark();
            WriteTimeoutException wte = (WriteTimeoutException)e;
            Tracing.trace("Write timeout; received {} of {} required replies", Integer.valueOf(wte.received), Integer.valueOf(wte.blockFor));
         } else if(e instanceof WriteFailureException) {
            writeMetrics.failures.mark();
            ((ClientWriteRequestMetrics)writeMetricsMap.get(consistencyLevel)).failures.mark();
            WriteFailureException wfe = (WriteFailureException)e;
            Tracing.trace("Write failure; received {} of {} required replies", Integer.valueOf(wfe.received), Integer.valueOf(wfe.blockFor));
         }

         return Completable.error(e);
      }).doFinally(() -> {
         recordLatency(consistencyLevel, startTime);
      });
   }

   public static boolean isLocalRange(String keyspaceName, AbstractBounds<PartitionPosition> range) {
      assert !AbstractBounds.strictlyWrapsAround(range.left, range.right);

      Collection<Range<Token>> localRanges = StorageService.instance.getNormalizedLocalRanges(keyspaceName);
      AbstractBounds<Token> queriedRange = AbstractBounds.bounds(((PartitionPosition)range.left).getToken(), range.inclusiveLeft(), ((PartitionPosition)range.right).getToken(), range.inclusiveRight());
      Iterator var4 = localRanges.iterator();

      Range localRange;
      do {
         if(!var4.hasNext()) {
            return false;
         }

         localRange = (Range)var4.next();
      } while(!localRange.contains(queriedRange));

      return true;
   }

   public static boolean isLocalToken(String keyspaceName, Token token) {
      Collection<Range<Token>> localRanges = StorageService.instance.getNormalizedLocalRanges(keyspaceName);
      Iterator var3 = localRanges.iterator();

      Range localRange;
      do {
         if(!var3.hasNext()) {
            return false;
         }

         localRange = (Range)var3.next();
      } while(!localRange.contains((RingPosition)token));

      return true;
   }

   private static WriteHandler writeToBatchlog(Collection<Mutation> mutations, WriteEndpoints endpoints, UUID uuid, long queryStartNanoTime) {
      WriteHandler handler = WriteHandler.create(endpoints, endpoints.liveCount() == 1?ConsistencyLevel.ONE:ConsistencyLevel.TWO, WriteType.BATCH_LOG, queryStartNanoTime, TPC.bestTPCTimer());
      Batch batch = Batch.createLocal(uuid, FBUtilities.timestampMicros(), mutations);
      MessagingService.instance().send((Request.Dispatcher)Verbs.WRITES.BATCH_STORE.newDispatcher(endpoints.live(), batch), handler);
      return handler;
   }

   private static void asyncRemoveFromBatchlog(List<InetAddress> endpoints, UUID uuid) {
      MessagingService.instance().send(Verbs.WRITES.BATCH_REMOVE.newDispatcher(endpoints, new BatchRemove(uuid)));
   }

   private static void writeBatchedMutations(List<StorageProxy.MutationAndEndpoints> mutationsAndEndpoints, List<WriteHandler> handlers, Verb.AckedRequest<Mutation> verb) throws OverloadedException {
      for(int i = 0; i < mutationsAndEndpoints.size(); ++i) {
         Mutation mutation = ((StorageProxy.MutationAndEndpoints)mutationsAndEndpoints.get(i)).mutation;
         WriteHandler handler = (WriteHandler)handlers.get(i);
         sendToHintedEndpoints(mutation, handler.endpoints(), handler, verb);
      }

   }

   private static WriteHandler mutateStandard(Mutation mutation, ConsistencyLevel consistencyLevel, WriteType writeType, long queryStartNanoTime) {
      WriteEndpoints endpoints = WriteEndpoints.compute((IMutation)mutation);
      endpoints.checkAvailability(consistencyLevel);
      WriteHandler handler = WriteHandler.builder(endpoints, consistencyLevel, writeType, queryStartNanoTime, TPC.bestTPCTimer()).withIdealConsistencyLevel(DatabaseDescriptor.getIdealConsistencyLevel()).hintOnTimeout(mutation).hintOnFailure(mutation).build();
      sendToHintedEndpoints(mutation, handler.endpoints(), handler, Verbs.WRITES.WRITE);
      return handler;
   }

   private static WriteEndpoints getBatchlogEndpoints(ConsistencyLevel consistencyLevel) throws UnavailableException {
      TokenMetadata.Topology topology = StorageService.instance.getTokenMetadata().cachedOnlyTokenMap().getTopology();
      Multimap<String, InetAddress> localEndpoints = HashMultimap.create((Multimap)topology.getDatacenterRacks().get(DatabaseDescriptor.getLocalDataCenter()));
      String localRack = DatabaseDescriptor.getLocalRack();
      Keyspace keyspace = Keyspace.open("system");
      Collection<InetAddress> chosenEndpoints = BatchlogManager.filterEndpoints(consistencyLevel, localRack, localEndpoints);

      assert chosenEndpoints.size() > 0;

      return WriteEndpoints.withLive(keyspace, chosenEndpoints);
   }

   private static void sendToHintedEndpoints(Mutation mutation, WriteEndpoints targets, WriteHandler handler, Verb.AckedRequest<Mutation> messageDefinition) throws OverloadedException {
      checkHintOverload(targets);
      MessagingService.instance().applyBackPressure(targets.live(), handler.currentTimeout()).thenAccept((v) -> {
         ((Mutation.MutationSerializer)Mutation.rawSerializers.get(EncodingVersion.last())).serializedBuffer(mutation);
         maybeSubmitHint(mutation, (Iterable)targets.dead(), handler);
         MessagingService.instance().send((Request.Dispatcher)messageDefinition.newForwardingDispatcher(targets.live(), DatabaseDescriptor.getLocalDataCenter(), mutation), handler);
      });
   }

   private static void checkHintOverload(WriteEndpoints endpoints) {
      long totalHintsInProgress = StorageMetrics.totalHintsInProgress.getCount();
      if(totalHintsInProgress > (long)maxHintsInProgress) {
         Iterator var3 = endpoints.iterator();

         InetAddress destination;
         int hintsForDestination;
         do {
            if(!var3.hasNext()) {
               return;
            }

            destination = (InetAddress)var3.next();
            hintsForDestination = getHintsInProgressFor(destination).get();
         } while(hintsForDestination <= 0 || !shouldHint(destination));

         throw new OverloadedException(String.format("Too many in flight hints: %d  destination: %s destination hints %d", new Object[]{Long.valueOf(totalHintsInProgress), destination, Integer.valueOf(hintsForDestination)}));
      }
   }

   private static WriteHandler mutateCounter(CounterMutation cm, long queryStartNanoTime) throws UnavailableException, OverloadedException {
      Keyspace keyspace = Keyspace.open(cm.getKeyspaceName());
      InetAddress endpoint = findSuitableEndpoint(keyspace, cm.key(), cm.consistency());
      WriteEndpoints.compute((IMutation)cm).checkAvailability(cm.consistency());
      WriteHandler handler = WriteHandler.create(WriteEndpoints.withLive(keyspace, Collections.singletonList(endpoint)), ConsistencyLevel.ONE, WriteType.COUNTER, queryStartNanoTime, TPC.bestTPCTimer());
      if(!endpoint.equals(FBUtilities.getBroadcastAddress())) {
         Tracing.trace("Forwarding counter update to write leader {}", (Object)endpoint);
      }

      MessagingService.instance().send((Request)Verbs.WRITES.COUNTER_FORWARDING.newRequest(endpoint, cm), handler);
      return handler;
   }

   private static InetAddress findSuitableEndpoint(Keyspace keyspace, DecoratedKey key, ConsistencyLevel cl) throws UnavailableException {
      IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
      ArrayList<InetAddress> endpoints = new ArrayList();
      StorageService var10000 = StorageService.instance;
      StorageService.addLiveNaturalEndpointsToList(keyspace, key, endpoints);
      endpoints.removeIf((endpoint) -> {
         return !StorageService.instance.isRpcReady(endpoint);
      });
      if(endpoints.isEmpty()) {
         throw new UnavailableException(cl, cl.blockFor(keyspace), 0);
      } else {
         List<InetAddress> localEndpoints = new ArrayList(endpoints.size());
         Iterator var6 = endpoints.iterator();

         while(var6.hasNext()) {
            InetAddress endpoint = (InetAddress)var6.next();
            if(snitch.isInLocalDatacenter(endpoint)) {
               localEndpoints.add(endpoint);
            }
         }

         if(localEndpoints.isEmpty()) {
            if(cl.isDatacenterLocal()) {
               throw new UnavailableException(cl, cl.blockFor(keyspace), 0);
            } else {
               snitch.sortByProximity(FBUtilities.getBroadcastAddress(), endpoints);
               return (InetAddress)endpoints.get(0);
            }
         } else {
            return (InetAddress)localEndpoints.get(ThreadLocalRandom.current().nextInt(localEndpoints.size()));
         }
      }
   }

   public static CompletableFuture<Void> applyCounterMutationOnLeader(CounterMutation cm, long queryStartNanoTime) throws UnavailableException, OverloadedException {
      CompletableFuture<Void> ret = new CompletableFuture();
      cm.applyCounterMutation().whenComplete((result, ex) -> {
         if(ex != null) {
            ret.completeExceptionally(ex);
         } else {
            WriteEndpoints endpoints = WriteEndpoints.compute(cm.getKeyspaceName(), cm.key());
            WriteHandler handler = WriteHandler.builder(endpoints, cm.consistency(), WriteType.COUNTER, queryStartNanoTime, TPC.bestTPCTimer()).withIdealConsistencyLevel(DatabaseDescriptor.getIdealConsistencyLevel()).hintOnTimeout(result).hintOnFailure(result).build();
            handler.onLocalResponse();
            WriteEndpoints remainingEndpoints = handler.endpoints().withoutLocalhost(true);
            if(!remainingEndpoints.isEmpty()) {
               sendToHintedEndpoints(result, remainingEndpoints, handler, Verbs.WRITES.WRITE);
            }

            handler.whenComplete((r, t) -> {
               if(t != null) {
                  ret.completeExceptionally(t);
               } else {
                  ret.complete(r);
               }

            });
         }
      });
      return ret;
   }

   private static boolean systemKeyspaceQuery(List<? extends ReadCommand> cmds) {
      Iterator var1 = cmds.iterator();

      ReadCommand cmd;
      do {
         if(!var1.hasNext()) {
            return true;
         }

         cmd = (ReadCommand)var1.next();
      } while(SchemaConstants.isLocalSystemKeyspace(cmd.metadata().keyspace));

      return false;
   }

   private static FilteredPartition readOne(SinglePartitionReadCommand command, ConsistencyLevel consistencyLevel, long queryStartNanoTime) throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException, InvalidRequestException {
      ReadContext ctx = ReadContext.builder(command, consistencyLevel).build(queryStartNanoTime);
      return readOne(command, ctx);
   }

   private static FilteredPartition readOne(SinglePartitionReadCommand command, ReadContext ctx) throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException, InvalidRequestException {
      return (FilteredPartition)read(SinglePartitionReadCommand.Group.one(command), ctx).flatMap((partition) -> {
         return FilteredPartition.create(partition);
      }).take(1L).ifEmpty(FilteredPartition.empty(command)).blockingSingle();
   }

   public static Flow<FlowablePartition> read(SinglePartitionReadCommand.Group group, ConsistencyLevel consistencyLevel, long queryStartNanoTime) throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException, InvalidRequestException {
      assert !consistencyLevel.isSerialConsistency();

      return read(group, ReadContext.builder(group, consistencyLevel).build(queryStartNanoTime));
   }

   private static void checkNotBootstrappingOrSystemQuery(List<? extends ReadCommand> commands, ClientRequestMetrics... metrics) throws IsBootstrappingException {
      if(StorageService.instance.isBootstrapMode() && !systemKeyspaceQuery(commands)) {
         ClientRequestMetrics[] var2 = metrics;
         int var3 = metrics.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            ClientRequestMetrics metric = var2[var4];
            metric.unavailables.mark();
         }

         throw new IsBootstrappingException();
      }
   }

   public static Flow<FlowablePartition> read(SinglePartitionReadCommand.Group group, ReadContext ctx) throws UnavailableException, IsBootstrappingException, ReadFailureException, ReadTimeoutException, InvalidRequestException {
      checkNotBootstrappingOrSystemQuery(group.commands, new ClientRequestMetrics[]{readMetrics, (ClientRequestMetrics)readMetricsMap.get(ctx.consistencyLevel)});
      if(ctx.consistencyLevel.isSerialConsistency()) {
         assert !ctx.forContinuousPaging;

         return readWithPaxos(group, ctx);
      } else {
         return ctx.forContinuousPaging && ctx.consistencyLevel.isSingleNode() && group.queriesOnlyLocalData()?readLocalContinuous(group, ctx):readRegular(group, ctx);
      }
   }

   private static Flow<FlowablePartition> readLocalContinuous(SinglePartitionReadCommand.Group group, ReadContext ctx) throws IsBootstrappingException, UnavailableException, ReadFailureException, ReadTimeoutException {
      assert ctx.consistencyLevel.isSingleNode();

      if(logger.isTraceEnabled()) {
         logger.trace("Querying single partition commands {} for continuous paging", group);
      }

      return group.executeInternal().doOnError((error) -> {
         readMetrics.failures.mark();
         ((ClientRequestMetrics)readMetricsMap.get(ctx.consistencyLevel)).failures.mark();
      });
   }

   private static Flow<FlowablePartition> readWithPaxos(SinglePartitionReadCommand.Group group, ReadContext ctx) throws InvalidRequestException, UnavailableException, ReadFailureException, ReadTimeoutException {
      assert ctx.clientState != null;

      if(group.commands.size() > 1) {
         throw new InvalidRequestException("SERIAL/LOCAL_SERIAL consistency may only be requested for one partition at a time");
      } else {
         long start = System.nanoTime();
         SinglePartitionReadCommand command = (SinglePartitionReadCommand)group.commands.get(0);
         TableMetadata metadata = command.metadata();
         DecoratedKey key = command.partitionKey();
         ConsistencyLevel consistencyLevel = ctx.consistencyLevel;
         Pair<WriteEndpoints, Integer> p = getPaxosParticipants(metadata, key, consistencyLevel);
         List<InetAddress> liveEndpoints = ((WriteEndpoints)p.left).live();
         int requiredParticipants = ((Integer)p.right).intValue();
         ConsistencyLevel consistencyForCommitOrFetch = consistencyLevel == ConsistencyLevel.LOCAL_SERIAL?ConsistencyLevel.LOCAL_QUORUM:ConsistencyLevel.QUORUM;
         Flow<Pair<UUID, Integer>> repairPaxosFlow = beginAndRepairPaxosAsync(start, key, metadata, liveEndpoints, requiredParticipants, consistencyLevel, consistencyForCommitOrFetch, false, ctx.clientState).map((pair) -> {
            recordCasContention(((Integer)pair.right).intValue());
            return pair;
         }).onErrorResumeNext((e) -> {
            if(e instanceof WriteTimeoutException) {
               return Flow.error(new ReadTimeoutException(consistencyLevel, 0, consistencyLevel.blockFor(Keyspace.open(metadata.keyspace)), false));
            } else if(e instanceof WriteFailureException) {
               WriteFailureException wfe = (WriteFailureException)e;
               return Flow.error(new ReadFailureException(consistencyLevel, wfe.received, wfe.blockFor, false, wfe.failureReasonByEndpoint));
            } else {
               return Flow.error(e);
            }
         });
         return Threads.observeOn(repairPaxosFlow, TPC.bestTPCScheduler(), TPCTaskType.EXECUTE_STATEMENT).flatMap((unused) -> {
            return fetchRows(group.commands, ctx.withConsistency(consistencyForCommitOrFetch));
         }).doOnError((e) -> {
            if(e instanceof UnavailableException) {
               readMetrics.unavailables.mark();
               casReadMetrics.unavailables.mark();
               ((ClientRequestMetrics)readMetricsMap.get(consistencyLevel)).unavailables.mark();
            } else if(e instanceof ReadTimeoutException) {
               readMetrics.timeouts.mark();
               casReadMetrics.timeouts.mark();
               ((ClientRequestMetrics)readMetricsMap.get(consistencyLevel)).timeouts.mark();
            } else if(e instanceof ReadFailureException) {
               readMetrics.failures.mark();
               casReadMetrics.failures.mark();
               ((ClientRequestMetrics)readMetricsMap.get(consistencyLevel)).failures.mark();
            }

         }).doOnClose(() -> {
            long latency = recordLatency(group, ctx);
            casReadMetrics.addNano(latency);
         });
      }
   }

   private static Flow<FlowablePartition> readRegular(SinglePartitionReadCommand.Group group, ReadContext ctx) throws UnavailableException, ReadFailureException, ReadTimeoutException {
      Flow<FlowablePartition> result = fetchRows(group.commands, ctx);
      boolean enforceStrictLiveness = ((SinglePartitionReadCommand)group.commands.get(0)).metadata().enforceStrictLiveness();
      if(ctx.forContinuousPaging) {
         return result;
      } else {
         if(group.commands.size() > 1) {
            result = result.map((r) -> {
               return group.limits().truncateFiltered(r, group.nowInSec(), group.selectsFullPartition(), enforceStrictLiveness);
            });
         }

         return result.doOnError((e) -> {
            if(e instanceof UnavailableException) {
               readMetrics.unavailables.mark();
               ((ClientRequestMetrics)readMetricsMap.get(ctx.consistencyLevel)).unavailables.mark();
            } else if(e instanceof ReadTimeoutException) {
               readMetrics.timeouts.mark();
               ((ClientRequestMetrics)readMetricsMap.get(ctx.consistencyLevel)).timeouts.mark();
            } else if(e instanceof ReadFailureException) {
               readMetrics.failures.mark();
               ((ClientRequestMetrics)readMetricsMap.get(ctx.consistencyLevel)).failures.mark();
            }

         }).doOnClose(() -> {
            recordLatency(group, ctx);
         });
      }
   }

   private static long recordLatency(SinglePartitionReadCommand.Group group, ReadContext ctx) {
      long latency = System.nanoTime() - ctx.queryStartNanos;
      readMetrics.addNano(latency);
      ((ClientRequestMetrics)readMetricsMap.get(ctx.consistencyLevel)).addNano(latency);
      Iterator var4 = group.commands.iterator();

      while(var4.hasNext()) {
         ReadCommand command = (ReadCommand)var4.next();
         Keyspace.openAndGetStore(command.metadata()).metric.coordinatorReadLatency.update(latency, TimeUnit.NANOSECONDS);
      }

      return latency;
   }

   private static Flow<FlowablePartition> fetchRows(List<SinglePartitionReadCommand> commands, ReadContext ctx) throws UnavailableException, ReadFailureException, ReadTimeoutException {
      return commands.size() == 1?(new StorageProxy.SinglePartitionReadLifecycle((SinglePartitionReadCommand)commands.get(0), ctx)).result():Flow.fromIterable(commands).flatMap((command) -> {
         return (new StorageProxy.SinglePartitionReadLifecycle(command, ctx)).result();
      });
   }

   public static void addLiveSortedEndpointsToList(Keyspace keyspace, RingPosition pos, ArrayList<InetAddress> liveEndpoints) {
      StorageService var10000 = StorageService.instance;
      StorageService.addLiveNaturalEndpointsToList(keyspace, pos, liveEndpoints);
      DatabaseDescriptor.getEndpointSnitch().sortByProximity(FBUtilities.getBroadcastAddress(), liveEndpoints);
   }

   public static ArrayList<InetAddress> getLiveSortedEndpoints(Keyspace keyspace, RingPosition pos) {
      ArrayList<InetAddress> liveEndpoints = new ArrayList();
      addLiveSortedEndpointsToList(keyspace, pos, liveEndpoints);
      return liveEndpoints;
   }

   private static ArrayList<InetAddress> intersection(List<InetAddress> l1, List<InetAddress> l2) {
      ArrayList<InetAddress> inter = new ArrayList(l1);
      inter.retainAll(l2);
      return inter;
   }

   private static float estimateResultsPerRange(PartitionRangeReadCommand command, Keyspace keyspace) {
      ColumnFamilyStore cfs = keyspace.getColumnFamilyStore(command.metadata().id);
      Index index = command.getIndex(cfs);
      float maxExpectedResults = index == null?command.limits().estimateTotalResults(cfs):(float)index.getEstimatedResultRows();
      return maxExpectedResults / (float)DatabaseDescriptor.getNumTokens() / (float)keyspace.getReplicationStrategy().getReplicationFactor();
   }

   private static void recordLatency(PartitionRangeReadCommand command, long start) {
      long latency = System.nanoTime() - start;
      rangeMetrics.addNano(latency);
      Keyspace.openAndGetStore(command.metadata()).metric.coordinatorScanLatency.update(latency, TimeUnit.NANOSECONDS);
   }

   private static void recordLatency(ConsistencyLevel cl, long start) {
      long latency = System.nanoTime() - start;
      writeMetrics.addNano(latency);
      ((ClientWriteRequestMetrics)writeMetricsMap.get(cl)).addNano(latency);
   }

   public static Flow<FlowablePartition> getRangeSlice(PartitionRangeReadCommand command, ReadContext ctx) {
      return ctx.forContinuousPaging && ctx.consistencyLevel.isSingleNode() && command.queriesOnlyLocalData()?getRangeSliceLocalContinuous(command, ctx):getRangeSliceRemote(command, ctx);
   }

   private static Flow<FlowablePartition> getRangeSliceRemote(PartitionRangeReadCommand command, ReadContext ctx) {
      checkNotBootstrappingOrSystemQuery(Collections.singletonList(command), new ClientRequestMetrics[]{rangeMetrics});
      Tracing.trace("Computing ranges to query");
      Keyspace keyspace = Keyspace.open(command.metadata().keyspace);
      StorageProxy.RangeIterator ranges = new StorageProxy.RangeIterator(command, keyspace, ctx);
      float resultsPerRange = estimateResultsPerRange(command, keyspace);
      resultsPerRange = (float)((double)resultsPerRange - (double)resultsPerRange * 0.1D);
      int concurrencyFactor = (double)resultsPerRange == 0.0D?1:Math.max(1, Math.min(ranges.rangeCount(), (int)Math.ceil((double)((float)command.limits().count() / resultsPerRange))));
      logger.trace("Estimated result rows per range: {}; requested rows: {}, ranges.size(): {}; concurrent range requests: {}", new Object[]{Float.valueOf(resultsPerRange), Integer.valueOf(command.limits().count()), Integer.valueOf(ranges.rangeCount()), Integer.valueOf(concurrencyFactor)});
      Tracing.trace("Submitting range requests on {} ranges with a concurrency of {} ({} rows per range expected)", new Object[]{Integer.valueOf(ranges.rangeCount()), Integer.valueOf(concurrencyFactor), Float.valueOf(resultsPerRange)});
      return command.withLimitsAndPostReconciliation((new StorageProxy.RangeCommandPartitions(ranges, command, concurrencyFactor, ctx)).partitions());
   }

   public static Flow<FlowablePartition> getRangeSliceLocalContinuous(PartitionRangeReadCommand command, ReadContext ctx) {
      assert ctx.consistencyLevel.isSingleNode();

      checkNotBootstrappingOrSystemQuery(Collections.singletonList(command), new ClientRequestMetrics[]{rangeMetrics});
      if(logger.isTraceEnabled()) {
         logger.trace("Querying local ranges {} for continuous paging", command);
      }

      return command.withLimitsAndPostReconciliation(FlowablePartitions.filter(command.executeLocally(), command.nowInSec())).doOnError((e) -> {
         rangeMetrics.failures.mark();
      });
   }

   public Map<String, List<String>> getSchemaVersions() {
      return describeSchemaVersions();
   }

   public static Map<String, List<String>> describeSchemaVersions() {
      String myVersion = Schema.instance.getVersion().toString();
      final Map<InetAddress, UUID> versions = new ConcurrentHashMap();
      Set<InetAddress> liveHosts = Gossiper.instance.getLiveMembers();
      final CountDownLatch latch = new CountDownLatch(liveHosts.size());
      MessageCallback<UUID> cb = new MessageCallback<UUID>() {
         public void onResponse(Response<UUID> message) {
            versions.put(message.from(), message.payload());
            latch.countDown();
         }

         public void onFailure(FailureResponse<UUID> message) {
         }
      };
      Iterator var5 = liveHosts.iterator();

      while(var5.hasNext()) {
         InetAddress endpoint = (InetAddress)var5.next();
         MessagingService.instance().send(Verbs.SCHEMA.VERSION.newRequest(endpoint, EmptyPayload.instance), cb);
      }

      try {
         latch.await(DatabaseDescriptor.getRpcTimeout(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException var12) {
         throw new AssertionError("This latch shouldn't have been interrupted.");
      }

      Map<String, List<String>> results = new HashMap();
      Iterable<InetAddress> allHosts = Iterables.concat(Gossiper.instance.getLiveMembers(), Gossiper.instance.getUnreachableMembers());

      Iterator var7;
      InetAddress host;
      String host;
      Object hosts;
      for(var7 = allHosts.iterator(); var7.hasNext(); ((List)hosts).add(host.getHostAddress())) {
         host = (InetAddress)var7.next();
         UUID version = (UUID)versions.get(host);
         host = version == null?"UNREACHABLE":version.toString();
         hosts = (List)results.get(host);
         if(hosts == null) {
            hosts = new ArrayList();
            results.put(host, hosts);
         }
      }

      if(results.get("UNREACHABLE") != null) {
         logger.debug("Hosts not in agreement. Didn't get a response from everybody: {}", StringUtils.join((Iterable)results.get("UNREACHABLE"), ","));
      }

      var7 = results.entrySet().iterator();

      while(true) {
         Entry entry;
         do {
            do {
               if(!var7.hasNext()) {
                  if(results.size() == 1) {
                     logger.debug("Schemas are in agreement.");
                  }

                  return results;
               }

               entry = (Entry)var7.next();
            } while(((String)entry.getKey()).equals("UNREACHABLE"));
         } while(((String)entry.getKey()).equals(myVersion));

         Iterator var16 = ((List)entry.getValue()).iterator();

         while(var16.hasNext()) {
            host = (String)var16.next();
            logger.debug("{} disagrees ({})", host, entry.getKey());
         }
      }
   }

   static <T extends RingPosition<T>> List<AbstractBounds<T>> getRestrictedRanges(AbstractBounds<T> queryRange) {
      if(queryRange instanceof Bounds && queryRange.left.equals(queryRange.right) && !queryRange.left.isMinimum()) {
         return Collections.singletonList(queryRange);
      } else {
         TokenMetadata tokenMetadata = StorageService.instance.getTokenMetadata();
         List<AbstractBounds<T>> ranges = new ArrayList();
         Iterator<Token> ringIter = TokenMetadata.ringIterator(tokenMetadata.sortedTokens(), queryRange.left.getToken(), true);
         AbstractBounds remainder = queryRange;

         while(ringIter.hasNext()) {
            Token upperBoundToken = (Token)ringIter.next();
            T upperBound = upperBoundToken.upperBound(queryRange.left.getClass());
            if(!remainder.left.equals(upperBound) && !remainder.contains(upperBound)) {
               break;
            }

            Pair<AbstractBounds<T>, AbstractBounds<T>> splits = remainder.split(upperBound);
            if(splits != null) {
               ranges.add(splits.left);
               remainder = (AbstractBounds)splits.right;
            }
         }

         ranges.add(remainder);
         return ranges;
      }
   }

   public boolean getHintedHandoffEnabled() {
      return DatabaseDescriptor.hintedHandoffEnabled();
   }

   public void setHintedHandoffEnabled(boolean b) {
      StorageService var2 = StorageService.instance;
      synchronized(StorageService.instance) {
         if(b) {
            StorageService.instance.checkServiceAllowedToStart("hinted handoff");
         }

         DatabaseDescriptor.setHintedHandoffEnabled(b);
      }
   }

   public void enableHintsForDC(String dc) {
      DatabaseDescriptor.enableHintsForDC(dc);
   }

   public void disableHintsForDC(String dc) {
      DatabaseDescriptor.disableHintsForDC(dc);
   }

   public Set<String> getHintedHandoffDisabledDCs() {
      return DatabaseDescriptor.hintedHandoffDisabledDCs();
   }

   public int getMaxHintWindow() {
      return DatabaseDescriptor.getMaxHintWindow();
   }

   public void setMaxHintWindow(int ms) {
      DatabaseDescriptor.setMaxHintWindow(ms);
   }

   public static boolean shouldHint(InetAddress ep) {
      if(DatabaseDescriptor.hintedHandoffEnabled()) {
         Set<String> disabledDCs = DatabaseDescriptor.hintedHandoffDisabledDCs();
         if(!disabledDCs.isEmpty()) {
            String dc = DatabaseDescriptor.getEndpointSnitch().getDatacenter(ep);
            if(disabledDCs.contains(dc)) {
               Tracing.trace("Not hinting {} since its data center {} has been disabled {}", new Object[]{ep, dc, disabledDCs});
               return false;
            }
         }

         boolean hintWindowExpired = Gossiper.instance.getEndpointDowntime(ep) > (long)DatabaseDescriptor.getMaxHintWindow();
         if(hintWindowExpired) {
            HintsService.instance.metrics.incrPastWindow(ep);
            Tracing.trace("Not hinting {} which has been down {} ms", ep, Long.valueOf(Gossiper.instance.getEndpointDowntime(ep)));
         }

         return !hintWindowExpired;
      } else {
         return false;
      }
   }

   public static void truncateBlocking(String keyspace, String cfname) throws UnavailableException, TimeoutException {
      logger.debug("Starting a blocking truncate operation on keyspace {}, CF {}", keyspace, cfname);
      if(isAnyStorageHostDown()) {
         logger.info("Cannot perform truncate, some hosts are down");
         int liveMembers = Gossiper.instance.getLiveMembers().size();
         throw new UnavailableException(ConsistencyLevel.ALL, liveMembers + Gossiper.instance.getUnreachableMembers().size(), liveMembers);
      } else {
         Set<InetAddress> allEndpoints = StorageService.instance.getLiveRingMembers(true);
         int blockFor = allEndpoints.size();
         TruncateResponseHandler responseHandler = new TruncateResponseHandler(blockFor);
         Tracing.trace("Enqueuing truncate messages to hosts {}", (Object)allEndpoints);
         Truncation truncation = new Truncation(keyspace, cfname);
         MessagingService.instance().send((Request.Dispatcher)Verbs.OPERATIONS.TRUNCATE.newDispatcher(allEndpoints, truncation), responseHandler);

         try {
            responseHandler.get();
         } catch (TimeoutException var7) {
            Tracing.trace("Timed out");
            throw var7;
         }
      }
   }

   private static boolean isAnyStorageHostDown() {
      return !Gossiper.instance.getUnreachableTokenOwners().isEmpty();
   }

   public long getTotalHints() {
      return StorageMetrics.totalHints.getCount();
   }

   public int getMaxHintsInProgress() {
      return maxHintsInProgress;
   }

   public void setMaxHintsInProgress(int qs) {
      maxHintsInProgress = qs;
   }

   public int getHintsInProgress() {
      return (int)StorageMetrics.totalHintsInProgress.getCount();
   }

   public void waitForHintsInProgress(int timeout, TimeUnit timeUnit) {
      long now = System.nanoTime();

      for(long end = now + TimeUnit.NANOSECONDS.convert((long)timeout, timeUnit); now < end; now = System.nanoTime()) {
         if(this.getHintsInProgress() <= 0) {
            return;
         }

         FBUtilities.sleepQuietly(10L);
      }

      if(this.getHintsInProgress() > 0) {
         logger.warn("Some hints were not written before shutdown.  This is not supposed to happen.  You should (a) run repair, and (b) file a bug report");
      }

   }

   private static AtomicInteger getHintsInProgressFor(InetAddress destination) {
      try {
         return (AtomicInteger)hintsInProgress.load(destination);
      } catch (Exception var2) {
         throw new AssertionError(var2);
      }
   }

   public static Future<Void> maybeSubmitHint(Mutation mutation, InetAddress target, WriteHandler handler) {
      return maybeSubmitHint(mutation, (Iterable)Collections.singleton(target), handler);
   }

   public static Future<Void> maybeSubmitHint(Mutation mutation, Iterable<InetAddress> candidates, WriteHandler handler) {
      int cap = candidates instanceof Collection?Math.min(8, (int)((float)((Collection)candidates).size() / 0.75F + 1.0F)):8;
      Set<InetAddress> targets = new HashSet(cap);
      Set<UUID> hostIds = new HashSet(cap);
      Iterator var6 = candidates.iterator();

      while(var6.hasNext()) {
         InetAddress target = (InetAddress)var6.next();
         if(shouldHint(target)) {
            UUID hostId = StorageService.instance.getHostIdForEndpoint(target);
            if(hostId != null) {
               targets.add(target);
               hostIds.add(hostId);
            } else {
               logger.debug("Discarding hint for endpoint not part of ring: {}", target);
            }
         }
      }

      if(targets.isEmpty()) {
         return Futures.immediateFuture((Object)null);
      } else {
         return submitHint(targets, Completable.defer(() -> {
            logger.trace("Adding hints for {}", targets);
            HintsService.instance.write((Iterable)hostIds, Hint.create(mutation, System.currentTimeMillis()));
            HintedHandoffMetrics var10001 = HintsService.instance.metrics;
            HintsService.instance.metrics.getClass();
            targets.forEach(var10001::incrCreatedHints);
            if(handler != null && handler.consistencyLevel() == ConsistencyLevel.ANY) {
               handler.onLocalResponse();
            }

            return Completable.complete();
         }));
      }
   }

   private static Future<Void> submitHint(Collection<InetAddress> targets, Completable hintsCompletable) {
      StorageMetrics.totalHintsInProgress.inc((long)targets.size());
      Iterator var2 = targets.iterator();

      while(var2.hasNext()) {
         InetAddress target = (InetAddress)var2.next();
         getHintsInProgressFor(target).incrementAndGet();
      }

      hintsCompletable = hintsCompletable.doOnTerminate(() -> {
         StorageMetrics.totalHintsInProgress.dec((long)targets.size());
         Iterator var1 = targets.iterator();

         while(var1.hasNext()) {
            InetAddress target = (InetAddress)var1.next();
            getHintsInProgressFor(target).decrementAndGet();
         }

      });
      hintsCompletable = hintsCompletable.subscribeOn(StageManager.getScheduler(Stage.BACKGROUND_IO));
      return TPCUtils.toFuture(hintsCompletable);
   }

   public Long getRpcTimeout() {
      return Long.valueOf(DatabaseDescriptor.getRpcTimeout());
   }

   public void setRpcTimeout(Long timeoutInMillis) {
      DatabaseDescriptor.setRpcTimeout(timeoutInMillis.longValue());
   }

   public Long getReadRpcTimeout() {
      return Long.valueOf(DatabaseDescriptor.getReadRpcTimeout());
   }

   public void setReadRpcTimeout(Long timeoutInMillis) {
      DatabaseDescriptor.setReadRpcTimeout(timeoutInMillis.longValue());
   }

   public Long getWriteRpcTimeout() {
      return Long.valueOf(DatabaseDescriptor.getWriteRpcTimeout());
   }

   public void setWriteRpcTimeout(Long timeoutInMillis) {
      DatabaseDescriptor.setWriteRpcTimeout(timeoutInMillis.longValue());
   }

   public long getCrossDCRttLatency() {
      return DatabaseDescriptor.getCrossDCRttLatency();
   }

   public void setCrossDCRttLatency(long latencyInMillis) {
      DatabaseDescriptor.setCrossDCRttLatency(latencyInMillis);
   }

   public Long getCounterWriteRpcTimeout() {
      return Long.valueOf(DatabaseDescriptor.getCounterWriteRpcTimeout());
   }

   public void setCounterWriteRpcTimeout(Long timeoutInMillis) {
      DatabaseDescriptor.setCounterWriteRpcTimeout(timeoutInMillis.longValue());
   }

   public Long getCasContentionTimeout() {
      return Long.valueOf(DatabaseDescriptor.getCasContentionTimeout());
   }

   public void setCasContentionTimeout(Long timeoutInMillis) {
      DatabaseDescriptor.setCasContentionTimeout(timeoutInMillis.longValue());
   }

   public Long getRangeRpcTimeout() {
      return Long.valueOf(DatabaseDescriptor.getRangeRpcTimeout());
   }

   public void setRangeRpcTimeout(Long timeoutInMillis) {
      DatabaseDescriptor.setRangeRpcTimeout(timeoutInMillis.longValue());
   }

   public Long getTruncateRpcTimeout() {
      return Long.valueOf(DatabaseDescriptor.getTruncateRpcTimeout());
   }

   public void setTruncateRpcTimeout(Long timeoutInMillis) {
      DatabaseDescriptor.setTruncateRpcTimeout(timeoutInMillis.longValue());
   }

   public Long getNativeTransportMaxConcurrentConnections() {
      return Long.valueOf(DatabaseDescriptor.getNativeTransportMaxConcurrentConnections());
   }

   public void setNativeTransportMaxConcurrentConnections(Long nativeTransportMaxConcurrentConnections) {
      DatabaseDescriptor.setNativeTransportMaxConcurrentConnections(nativeTransportMaxConcurrentConnections.longValue());
   }

   public Long getNativeTransportMaxConcurrentConnectionsPerIp() {
      return Long.valueOf(DatabaseDescriptor.getNativeTransportMaxConcurrentConnectionsPerIp());
   }

   public void setNativeTransportMaxConcurrentConnectionsPerIp(Long nativeTransportMaxConcurrentConnections) {
      DatabaseDescriptor.setNativeTransportMaxConcurrentConnectionsPerIp(nativeTransportMaxConcurrentConnections.longValue());
   }

   public void reloadTriggerClasses() {
      TriggerExecutor.instance.reloadClasses();
   }

   public long getReadRepairAttempted() {
      return ReadRepairMetrics.attempted.getCount();
   }

   public long getReadRepairRepairedBlocking() {
      return ReadRepairMetrics.repairedBlocking.getCount();
   }

   public long getReadRepairRepairedBackground() {
      return ReadRepairMetrics.repairedBackground.getCount();
   }

   public int getNumberOfTables() {
      return Schema.instance.getNumberOfTables();
   }

   public String getIdealConsistencyLevel() {
      return DatabaseDescriptor.getIdealConsistencyLevel().toString();
   }

   public String setIdealConsistencyLevel(String cl) {
      ConsistencyLevel original = DatabaseDescriptor.getIdealConsistencyLevel();
      ConsistencyLevel newCL = ConsistencyLevel.valueOf(cl.trim().toUpperCase());
      DatabaseDescriptor.setIdealConsistencyLevel(newCL);
      return String.format("Updating ideal consistency level new value: %s old value %s", new Object[]{newCL, original.toString()});
   }

   public int getOtcBacklogExpirationInterval() {
      return 0;
   }

   public void setOtcBacklogExpirationInterval(int intervalInMillis) {
   }

   static {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

      try {
         mbs.registerMBean(instance, new ObjectName("org.apache.cassandra.db:type=StorageProxy"));
      } catch (Exception var5) {
         throw new RuntimeException(var5);
      }

      HintsService.instance.registerMBean();
      HintedHandOffManager.instance.registerMBean();
      ConsistencyLevel[] var1 = ConsistencyLevel.values();
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         ConsistencyLevel level = var1[var3];
         readMetricsMap.put(level, new ClientRequestMetrics("Read-" + level.name()));
         writeMetricsMap.put(level, new ClientWriteRequestMetrics("Write-" + level.name()));
      }

   }

   private static class RangeCommandPartitions {
      private final Iterator<StorageProxy.RangeForQuery> ranges;
      private final int totalRangeCount;
      private final PartitionRangeReadCommand command;
      private final ReadContext ctx;
      private final long startTime;
      private DataLimits.Counter counter;
      private int concurrencyFactor;
      private int liveReturned;
      private int rangesQueried;

      public RangeCommandPartitions(StorageProxy.RangeIterator ranges, PartitionRangeReadCommand command, int concurrencyFactor, ReadContext ctx) {
         this.command = command;
         this.concurrencyFactor = concurrencyFactor;
         this.startTime = System.nanoTime();
         this.ranges = new StorageProxy.RangeMerger(ranges, ctx, null);
         this.totalRangeCount = ranges.rangeCount();
         this.ctx = ctx;
      }

      Flow<FlowablePartition> partitions() {
         Flow<FlowablePartition> partitions = this.nextBatch().concatWith(this::moreContents);
         if(!this.ctx.forContinuousPaging) {
            partitions = partitions.doOnClose(this::close);
         }

         return partitions;
      }

      private Flow<FlowablePartition> nextBatch() {
         Flow<FlowablePartition> batch = this.sendNextRequests();
         if(!this.ctx.forContinuousPaging) {
            batch = batch.doOnError(this::handleError);
         }

         return batch.doOnComplete(this::handleBatchCompleted);
      }

      private Flow<FlowablePartition> moreContents() {
         return !this.ranges.hasNext()?null:this.nextBatch();
      }

      private void handleError(Throwable e) {
         if(e instanceof UnavailableException) {
            StorageProxy.rangeMetrics.unavailables.mark();
         } else if(e instanceof ReadTimeoutException) {
            StorageProxy.rangeMetrics.timeouts.mark();
         } else if(e instanceof ReadFailureException) {
            StorageProxy.rangeMetrics.failures.mark();
         }

      }

      private void handleBatchCompleted() {
         this.liveReturned += this.counter.counted();
         this.updateConcurrencyFactor();
      }

      private void updateConcurrencyFactor() {
         if(this.liveReturned == 0) {
            this.concurrencyFactor = this.totalRangeCount - this.rangesQueried;
         } else {
            int remainingRows = this.command.limits().count() - this.liveReturned;
            float rowsPerRange = (float)this.liveReturned / (float)this.rangesQueried;
            this.concurrencyFactor = Math.max(1, Math.min(this.totalRangeCount - this.rangesQueried, Math.round((float)remainingRows / rowsPerRange)));
            StorageProxy.logger.trace("Didn't get enough response rows; actual rows per range: {}; remaining rows: {}, new concurrent requests: {}", new Object[]{Float.valueOf(rowsPerRange), Integer.valueOf(remainingRows), Integer.valueOf(this.concurrencyFactor)});
         }
      }

      private Flow<FlowablePartition> query(StorageProxy.RangeForQuery toQuery, boolean isFirst) {
         PartitionRangeReadCommand rangeCommand = this.command.forSubRange(toQuery.range, isFirst);
         ReadCallback<FlowablePartition> handler = ReadCallback.forInitialRead(rangeCommand, toQuery.filteredEndpoints, this.ctx);
         handler.assureSufficientLiveNodes();
         List<InetAddress> replicas = toQuery.filteredEndpoints;
         if(this.ctx.withDigests) {
            MessagingService.instance().send((Request)rangeCommand.requestTo((InetAddress)replicas.get(0)), handler);
            if(replicas.size() > 1) {
               ReadCommand digestCommand = rangeCommand.createDigestCommand(DigestVersion.forReplicas(replicas));
               MessagingService.instance().send((Request.Dispatcher)digestCommand.dispatcherTo(replicas.subList(1, replicas.size())), handler);
            }
         } else {
            MessagingService.instance().send((Request.Dispatcher)rangeCommand.dispatcherTo(replicas), handler);
         }

         return handler.result().onErrorResumeNext((e) -> {
            if(e instanceof RuntimeException && e.getCause() != null) {
               e = e.getCause();
            }

            return e instanceof DigestMismatchException?this.retryOnDigestMismatch(handler, (DigestMismatchException)e):Flow.error(e);
         });
      }

      Flow<FlowablePartition> retryOnDigestMismatch(ReadCallback<FlowablePartition> handler, DigestMismatchException ex) throws ReadFailureException, ReadTimeoutException {
         Tracing.trace("Digest mismatch: {}", (Object)ex);
         ReadCommand command = handler.command();
         Pair<ReadCallback<FlowablePartition>, Collection<InetAddress>> p = handler.forDigestMismatchRepair(handler.endpoints);
         ReadCallback<FlowablePartition> repairHandler = (ReadCallback)p.left;
         Collection<InetAddress> endpoints = (Collection)p.right;
         Tracing.trace("Enqueuing full data reads to {}", (Object)endpoints);
         MessagingService.instance().send((Request.Dispatcher)command.dispatcherTo(endpoints), repairHandler);
         return repairHandler.result();
      }

      private Flow<FlowablePartition> sendNextRequests() {
         if(StorageProxy.logger.isTraceEnabled()) {
            StorageProxy.logger.trace("Sending requests with concurrencyFactor {}", Integer.valueOf(this.concurrencyFactor));
         }

         List<Flow<FlowablePartition>> concurrentQueries = new ArrayList(this.concurrencyFactor);

         for(int i = 0; i < this.concurrencyFactor && this.ranges.hasNext(); ++i) {
            concurrentQueries.add(this.query((StorageProxy.RangeForQuery)this.ranges.next(), i == 0));
            ++this.rangesQueried;
         }

         Tracing.trace("Submitted {} concurrent range requests", (Object)Integer.valueOf(concurrentQueries.size()));
         this.counter = DataLimits.NONE.newCounter(this.command.nowInSec(), true, this.command.selectsFullPartition(), this.command.metadata().enforceStrictLiveness());
         return DataLimits.truncateFiltered(Flow.concat((Iterable)concurrentQueries), this.counter);
      }

      public void close() {
         StorageProxy.recordLatency(this.command, this.startTime);
      }
   }

   private static class RangeMerger extends AbstractIterator<StorageProxy.RangeForQuery> {
      private final PeekingIterator<StorageProxy.RangeForQuery> ranges;
      private final ReadContext params;

      private RangeMerger(Iterator<StorageProxy.RangeForQuery> iterator, ReadContext params) {
         this.params = params;
         this.ranges = Iterators.peekingIterator(iterator);
      }

      protected StorageProxy.RangeForQuery computeNext() {
         if(!this.ranges.hasNext()) {
            return (StorageProxy.RangeForQuery)this.endOfData();
         } else {
            StorageProxy.RangeForQuery current = (StorageProxy.RangeForQuery)this.ranges.next();
            Keyspace keyspace = this.params.keyspace;
            ConsistencyLevel consistency = this.params.consistencyLevel;

            while(this.ranges.hasNext() && !((PartitionPosition)current.range.right).isMinimum()) {
               StorageProxy.RangeForQuery next = (StorageProxy.RangeForQuery)this.ranges.peek();
               ArrayList<InetAddress> merged = StorageProxy.intersection(current.liveEndpoints, next.liveEndpoints);
               if(!consistency.isSufficientLiveNodes(keyspace, merged)) {
                  break;
               }

               List<InetAddress> filteredMerged = this.params.filterForQuery(merged);
               if(!DatabaseDescriptor.getEndpointSnitch().isWorthMergingForRangeQuery(filteredMerged, current.filteredEndpoints, next.filteredEndpoints)) {
                  break;
               }

               current = new StorageProxy.RangeForQuery(current.range.withNewRight(next.range.right), merged, filteredMerged);
               this.ranges.next();
            }

            return current;
         }
      }
   }

   private static class RangeIterator extends AbstractIterator<StorageProxy.RangeForQuery> {
      private final Keyspace keyspace;
      private final ReadContext params;
      private final Iterator<? extends AbstractBounds<PartitionPosition>> ranges;
      private final int rangeCount;

      public RangeIterator(PartitionRangeReadCommand command, Keyspace keyspace, ReadContext params) {
         this.keyspace = keyspace;
         this.params = params;
         List<? extends AbstractBounds<PartitionPosition>> l = keyspace.getReplicationStrategy() instanceof LocalStrategy?command.dataRange().keyRange().unwrap():StorageProxy.getRestrictedRanges(command.dataRange().keyRange());
         this.ranges = l.iterator();
         this.rangeCount = l.size();
      }

      public int rangeCount() {
         return this.rangeCount;
      }

      protected StorageProxy.RangeForQuery computeNext() {
         if(!this.ranges.hasNext()) {
            return (StorageProxy.RangeForQuery)this.endOfData();
         } else {
            AbstractBounds<PartitionPosition> range = (AbstractBounds)this.ranges.next();
            ArrayList<InetAddress> liveEndpoints = StorageProxy.getLiveSortedEndpoints(this.keyspace, range.right);
            return new StorageProxy.RangeForQuery(range, liveEndpoints, this.params.filterForQuery(liveEndpoints));
         }
      }
   }

   private static class RangeForQuery {
      public final AbstractBounds<PartitionPosition> range;
      public final List<InetAddress> liveEndpoints;
      public final List<InetAddress> filteredEndpoints;

      public RangeForQuery(AbstractBounds<PartitionPosition> range, List<InetAddress> liveEndpoints, List<InetAddress> filteredEndpoints) {
         this.range = range;
         this.liveEndpoints = liveEndpoints;
         this.filteredEndpoints = filteredEndpoints;
      }

      public String toString() {
         return String.format("[%s -> %s", new Object[]{this.range, this.filteredEndpoints});
      }
   }

   private static class SinglePartitionReadLifecycle {
      private final SinglePartitionReadCommand command;
      private final AbstractReadExecutor executor;
      private final ReadContext ctx;
      private ReadCallback<FlowablePartition> repairHandler;

      SinglePartitionReadLifecycle(SinglePartitionReadCommand command, ReadContext ctx) {
         this.command = command;
         this.executor = AbstractReadExecutor.getReadExecutor(command, ctx);
         this.ctx = ctx;
      }

      Completable doInitialQueries() {
         return this.executor.executeAsync();
      }

      Completable maybeTryAdditionalReplicas() {
         return this.executor.maybeTryAdditionalReplicas();
      }

      Flow<FlowablePartition> result() {
         return Flow.concat(Completable.concatArray(new CompletableSource[]{this.doInitialQueries(), this.maybeTryAdditionalReplicas()}), this.executor.handler.result()).onErrorResumeNext((e) -> {
            if(StorageProxy.logger.isTraceEnabled()) {
               StorageProxy.logger.trace("Got error {}/{}", e.getClass().getName(), e.getMessage());
            }

            if(e instanceof RuntimeException && e.getCause() != null) {
               e = e.getCause();
            }

            return e instanceof DigestMismatchException?this.retryOnDigestMismatch((DigestMismatchException)e):Flow.error(e);
         });
      }

      Flow<FlowablePartition> retryOnDigestMismatch(DigestMismatchException ex) throws ReadFailureException, ReadTimeoutException {
         Tracing.trace("Digest mismatch: {}", (Object)ex.getMessage());
         ReadRepairMetrics.repairedBlocking.mark();
         Pair<ReadCallback<FlowablePartition>, Collection<InetAddress>> p = this.executor.handler.forDigestMismatchRepair(this.executor.getContactedReplicas());
         this.repairHandler = (ReadCallback)p.left;
         Collection<InetAddress> contactedReplicas = (Collection)p.right;
         Tracing.trace("Enqueuing full data reads to {}", (Object)contactedReplicas);
         MessagingService.instance().send((Request.Dispatcher)this.command.dispatcherTo(contactedReplicas), this.repairHandler);
         return this.repairHandler.result().mapError((e) -> {
            if(StorageProxy.logger.isTraceEnabled()) {
               StorageProxy.logger.trace("Got error {}/{}", e.getClass().getName(), e.getMessage());
            }

            if(e instanceof RuntimeException && e.getCause() != null) {
               e = e.getCause();
            }

            if(e instanceof DigestMismatchException) {
               return new AssertionError(e);
            } else if(e instanceof ReadTimeoutException) {
               if(Tracing.isTracing()) {
                  Tracing.trace("Timed out waiting on digest mismatch repair requests");
               } else {
                  StorageProxy.logger.trace("Timed out waiting on digest mismatch repair requests");
               }

               int blockFor = this.ctx.consistencyLevel.blockFor(Keyspace.open(this.command.metadata().keyspace));
               return new ReadTimeoutException(this.ctx.consistencyLevel, blockFor - 1, blockFor, true);
            } else {
               return e;
            }
         });
      }
   }

   private static class MutationAndEndpoints {
      final Mutation mutation;
      final WriteEndpoints endpoints;

      MutationAndEndpoints(Mutation mutation, WriteEndpoints endpoints) {
         this.mutation = mutation;
         this.endpoints = endpoints;
      }
   }
}
