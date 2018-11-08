package org.apache.cassandra.db;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.io.ByteStreams;
import io.reactivex.Single;
import io.reactivex.schedulers.Schedulers;
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import javax.management.openmbean.OpenDataException;
import javax.management.openmbean.TabularData;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.functions.AggregateFcts;
import org.apache.cassandra.cql3.functions.BytesConversionFcts;
import org.apache.cassandra.cql3.functions.CastFcts;
import org.apache.cassandra.cql3.functions.OperationFcts;
import org.apache.cassandra.cql3.functions.TimeFcts;
import org.apache.cassandra.cql3.functions.UuidFcts;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.commitlog.CommitLogPosition;
import org.apache.cassandra.db.compaction.CompactionHistoryTabularData;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Rows;
import org.apache.cassandra.dht.BoundsVersion;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.LocalPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.RebufferingInputStream;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.metrics.RestorableMeter;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingVersion;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.schema.CompactionParams;
import org.apache.cassandra.schema.CompressionParams;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.service.paxos.PaxosState;
import org.apache.cassandra.streaming.StreamOperation;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MD5Digest;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.concurrent.ExecutableLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SystemKeyspace {
   private static final Logger logger = LoggerFactory.getLogger(SystemKeyspace.class);
   public static final LocalPartitioner BATCH_PARTITIONER;
   public static final String BATCHES = "batches";
   public static final String PAXOS = "paxos";
   public static final String BUILT_INDEXES = "IndexInfo";
   public static final String LOCAL = "local";
   public static final String PEERS = "peers";
   public static final String PEER_EVENTS = "peer_events";
   public static final String RANGE_XFERS = "range_xfers";
   public static final String COMPACTION_HISTORY = "compaction_history";
   public static final String SSTABLE_ACTIVITY = "sstable_activity";
   public static final String SIZE_ESTIMATES = "size_estimates";
   public static final String AVAILABLE_RANGES = "available_ranges";
   public static final String TRANSFERRED_RANGES = "transferred_ranges";
   public static final String VIEW_BUILDS_IN_PROGRESS = "view_builds_in_progress";
   public static final String BUILT_VIEWS = "built_views";
   public static final String PREPARED_STATEMENTS = "prepared_statements";
   public static final String REPAIRS = "repairs";
   public static final TableMetadata Batches;
   public static final TableMetadata Paxos;
   private static final TableMetadata BuiltIndexes;
   private static final TableMetadata Local;
   private static final TableMetadata Peers;
   private static final TableMetadata PeerEvents;
   private static final TableMetadata RangeXfers;
   private static final TableMetadata CompactionHistory;
   private static final TableMetadata SSTableActivity;
   private static final TableMetadata SizeEstimates;
   private static final TableMetadata AvailableRanges;
   private static final TableMetadata TransferredRanges;
   private static final TableMetadata ViewBuildsInProgress;
   private static final TableMetadata BuiltViews;
   private static final TableMetadata PreparedStatements;
   private static final TableMetadata Repairs;
   private static final ExecutableLock GLOBAL_LOCK;
   private static SystemKeyspace.StartupState startupState;
   private static UUID localHostId;
   private static volatile SystemKeyspace.BootstrapState bootstrapState;
   private static volatile ConcurrentMap<InetAddress, PeerInfo> peers;
   private static ConcurrentMap<TableId, Pair<CommitLogPosition, Long>> truncationRecords;

   private SystemKeyspace() {
   }

   private static TableMetadata.Builder parse(String table, String description, String cql) {
      return parse(table, description, cql, Collections.emptyList());
   }

   private static TableMetadata.Builder parse(String table, String description, String cql, Collection<UserType> types) {
      return CreateTableStatement.parse(String.format(cql, new Object[]{table}), "system", types).id(TableId.forSystemTable("system", table)).dcLocalReadRepairChance(0.0D).gcGraceSeconds(0).memtableFlushPeriod((int)TimeUnit.HOURS.toMillis(1L)).comment(description);
   }

   public static KeyspaceMetadata metadata() {
      return KeyspaceMetadata.create("system", KeyspaceParams.local(), tables(), Views.none(), Types.none(), functions());
   }

   private static Tables tables() {
      return Tables.of(new TableMetadata[]{BuiltIndexes, Batches, Paxos, Local, Peers, PeerEvents, RangeXfers, CompactionHistory, SSTableActivity, SizeEstimates, AvailableRanges, TransferredRanges, ViewBuildsInProgress, BuiltViews, PreparedStatements, Repairs});
   }

   private static Functions functions() {
      return Functions.builder().add((Iterable)UuidFcts.all()).add((Iterable)TimeFcts.all()).add((Iterable)BytesConversionFcts.all()).add((Iterable)AggregateFcts.all()).add((Iterable)CastFcts.all()).add((Iterable)OperationFcts.all()).build();
   }

   public static List<String> readableSystemResources() {
      return Arrays.asList(new String[]{"local", "peers", "size_estimates", "available_ranges"});
   }

   public static void beginStartupBlocking() {
      TPCUtils.withLockBlocking(GLOBAL_LOCK, () -> {
         if(startupState != SystemKeyspace.StartupState.NONE) {
            return null;
         } else {
            peers = (ConcurrentMap)TPCUtils.blockingGet(readPeerInfo());
            TPCUtils.blockingAwait(persistLocalMetadata());
            truncationRecords = (ConcurrentMap)TPCUtils.blockingGet(readTruncationRecords());
            bootstrapState = (SystemKeyspace.BootstrapState)TPCUtils.blockingGet(loadBootstrapState());
            startupState = SystemKeyspace.StartupState.STARTED;
            return null;
         }
      });
   }

   public static void finishStartupBlocking() {
      TPCUtils.withLockBlocking(GLOBAL_LOCK, () -> {
         if(startupState == SystemKeyspace.StartupState.COMPLETED) {
            return null;
         } else {
            SchemaKeyspace.saveSystemKeyspacesSchema();
            peers = (ConcurrentMap)TPCUtils.blockingGet(readPeerInfo());
            truncationRecords = (ConcurrentMap)TPCUtils.blockingGet(readTruncationRecords());
            bootstrapState = (SystemKeyspace.BootstrapState)TPCUtils.blockingGet(loadBootstrapState());
            startupState = SystemKeyspace.StartupState.COMPLETED;
            return null;
         }
      });
   }

   @VisibleForTesting
   public static void resetStartupBlocking() {
      TPCUtils.withLockBlocking(GLOBAL_LOCK, () -> {
         peers = null;
         truncationRecords = null;
         bootstrapState = null;
         startupState = SystemKeyspace.StartupState.NONE;
         return null;
      });
   }

   private static void checkPeersCache() {
      if(peers == null) {
         if(TPC.isTPCThread()) {
            throw new TPCUtils.WouldBlockException(String.format("Reading system peers would block %s, call startup methods first", new Object[]{Thread.currentThread().getName()}));
         } else {
            TPCUtils.withLockBlocking(GLOBAL_LOCK, () -> {
               if(peers == null) {
                  peers = (ConcurrentMap)TPCUtils.blockingGet(readPeerInfo());
               }

               return null;
            });
         }
      }
   }

   private static void verify(boolean test, String error) {
      if(!test) {
         throw new IllegalStateException(error);
      }
   }

   public static CompletableFuture<Void> persistLocalMetadata() {
      String req = "INSERT INTO system.%s (key,cluster_name,release_version,cql_version,native_protocol_version,data_center,rack,partitioner,rpc_address,broadcast_address,listen_address,native_transport_address,native_transport_port,native_transport_port_ssl,storage_port,storage_port_ssl,jmx_port) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
      IEndpointSnitch snitch = DatabaseDescriptor.getEndpointSnitch();
      return TPCUtils.toFutureVoid(QueryProcessor.executeOnceInternal(String.format(req, new Object[]{"local"}), new Object[]{"local", DatabaseDescriptor.getClusterName(), FBUtilities.getReleaseVersionString(), QueryProcessor.CQL_VERSION.toString(), String.valueOf(ProtocolVersion.CURRENT.asInt()), snitch.getDatacenter(FBUtilities.getBroadcastAddress()), snitch.getRack(FBUtilities.getBroadcastAddress()), DatabaseDescriptor.getPartitioner().getClass().getName(), DatabaseDescriptor.getNativeTransportAddress(), FBUtilities.getBroadcastAddress(), FBUtilities.getLocalAddress(), DatabaseDescriptor.getNativeTransportAddress(), Integer.valueOf(DatabaseDescriptor.getNativeTransportPort()), Integer.valueOf(DatabaseDescriptor.getNativeTransportPortSSL()), Integer.valueOf(DatabaseDescriptor.getStoragePort()), Integer.valueOf(DatabaseDescriptor.getSSLStoragePort()), DatabaseDescriptor.getJMXPort().orElse((Object)null)}));
   }

   public static CompletableFuture<Void> updateCompactionHistory(String ksname, String cfname, long compactedAt, long bytesIn, long bytesOut, Map<Integer, Long> rowsMerged) {
      if(ksname.equals("system") && cfname.equals("compaction_history")) {
         return TPCUtils.completedFuture();
      } else {
         String req = "INSERT INTO system.%s (id, keyspace_name, columnfamily_name, compacted_at, bytes_in, bytes_out, rows_merged) VALUES (?, ?, ?, ?, ?, ?, ?)";
         return TPCUtils.toFutureVoid(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"compaction_history"}), new Object[]{UUIDGen.getTimeUUID(), ksname, cfname, ByteBufferUtil.bytes(compactedAt), Long.valueOf(bytesIn), Long.valueOf(bytesOut), rowsMerged}));
      }
   }

   public static CompletableFuture<TabularData> getCompactionHistory() {
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format("SELECT * from system.%s", new Object[]{"compaction_history"}), new Object[0])).thenApply((resultSet) -> {
         try {
            return CompactionHistoryTabularData.from(resultSet);
         } catch (OpenDataException var2) {
            throw new CompletionException(var2);
         }
      });
   }

   public static CompletableFuture<Boolean> isViewBuilt(String keyspaceName, String viewName) {
      String req = "SELECT view_name FROM %s.\"%s\" WHERE keyspace_name=? AND view_name=?";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"system", "built_views"}), new Object[]{keyspaceName, viewName})).thenApply((result) -> {
         return Boolean.valueOf(!result.isEmpty());
      });
   }

   public static CompletableFuture<Boolean> isViewStatusReplicated(String keyspaceName, String viewName) {
      String req = "SELECT status_replicated FROM %s.\"%s\" WHERE keyspace_name=? AND view_name=?";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"system", "built_views"}), new Object[]{keyspaceName, viewName})).thenApply((result) -> {
         if(result.isEmpty()) {
            return Boolean.valueOf(false);
         } else {
            UntypedResultSet.Row row = result.one();
            return Boolean.valueOf(row.has("status_replicated") && row.getBoolean("status_replicated"));
         }
      });
   }

   public static CompletableFuture<Void> setViewBuilt(String keyspaceName, String viewName, boolean replicated) {
      return isViewBuilt(keyspaceName, viewName).thenCompose((built) -> {
         return built.booleanValue()?isViewStatusReplicated(keyspaceName, viewName).thenCompose((replicatedResult) -> {
            return replicatedResult.booleanValue() == replicated?TPCUtils.completedFuture():doSetViewBuilt(keyspaceName, viewName, replicated);
         }):doSetViewBuilt(keyspaceName, viewName, replicated);
      });
   }

   private static CompletableFuture<Void> doSetViewBuilt(String keyspaceName, String viewName, boolean replicated) {
      String req = "INSERT INTO %s.\"%s\" (keyspace_name, view_name, status_replicated) VALUES (?, ?, ?)";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"system", "built_views"}), new Object[]{keyspaceName, viewName, Boolean.valueOf(replicated)})).thenCompose((resultSet) -> {
         return forceFlush("built_views");
      });
   }

   public static CompletableFuture<Void> setViewRemoved(String keyspaceName, String viewName) {
      String buildReq = "DELETE FROM %s.%s WHERE keyspace_name = ? AND view_name = ?";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(buildReq, new Object[]{"system", "view_builds_in_progress"}), new Object[]{keyspaceName, viewName})).thenCompose((r1) -> {
         return forceFlush("view_builds_in_progress");
      }).thenCompose((r2) -> {
         String builtReq = "DELETE FROM %s.\"%s\" WHERE keyspace_name = ? AND view_name = ? IF EXISTS";
         return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(builtReq, new Object[]{"system", "built_views"}), new Object[]{keyspaceName, viewName}));
      }).thenCompose((r3) -> {
         return forceFlush("built_views");
      });
   }

   public static CompletableFuture<Void> finishViewBuildStatus(String ksname, String viewName) {
      String req = "DELETE FROM system.%s WHERE keyspace_name = ? AND view_name = ?";
      return setViewBuilt(ksname, viewName, false).thenCompose((r1) -> {
         return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"view_builds_in_progress"}), new Object[]{ksname, viewName}));
      }).thenCompose((r2) -> {
         return forceFlush("view_builds_in_progress");
      });
   }

   public static CompletableFuture<Void> setViewBuiltReplicated(String ksname, String viewName) {
      return setViewBuilt(ksname, viewName, true);
   }

   public static CompletableFuture<Void> updateViewBuildStatus(String ksname, String viewName, Range<Token> range, Token lastToken, long keysBuilt) {
      String req = "INSERT INTO system.%s (keyspace_name, view_name, start_token, end_token, last_token, keys_built) VALUES (?, ?, ?, ?, ?, ?)";
      Token.TokenFactory factory = ViewBuildsInProgress.partitioner.getTokenFactory();
      return TPCUtils.toFutureVoid(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"view_builds_in_progress"}), new Object[]{ksname, viewName, factory.toString((Token)range.left), factory.toString((Token)range.right), factory.toString(lastToken), Long.valueOf(keysBuilt)}));
   }

   public static CompletableFuture<Map<Range<Token>, Pair<Token, Long>>> getViewBuildStatus(String ksname, String viewName) {
      String req = "SELECT start_token, end_token, last_token, keys_built FROM system.%s WHERE keyspace_name = ? AND view_name = ?";
      Token.TokenFactory factory = ViewBuildsInProgress.partitioner.getTokenFactory();
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"view_builds_in_progress"}), new Object[]{ksname, viewName})).thenApply((queryResultSet) -> {
         if(queryResultSet != null && !queryResultSet.isEmpty()) {
            Map<Range<Token>, Pair<Token, Long>> status = new HashMap();
            Iterator var3 = queryResultSet.iterator();

            while(var3.hasNext()) {
               UntypedResultSet.Row row = (UntypedResultSet.Row)var3.next();
               Token start = factory.fromString(row.getString("start_token"));
               Token end = factory.fromString(row.getString("end_token"));
               Range<Token> range = new Range(start, end);
               Token lastToken = row.has("last_token")?factory.fromString(row.getString("last_token")):null;
               long keysBuilt = row.has("keys_built")?row.getLong("keys_built"):0L;
               status.put(range, Pair.create(lastToken, Long.valueOf(keysBuilt)));
            }

            return status;
         } else {
            return Collections.emptyMap();
         }
      });
   }

   public static CompletableFuture<Void> saveTruncationRecord(ColumnFamilyStore cfs, long truncatedAt, CommitLogPosition position) {
      return TPCUtils.withLock(GLOBAL_LOCK, () -> {
         String req = "UPDATE system.%s SET truncated_at = truncated_at + ? WHERE key = '%s'";
         return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"local", "local"}), new Object[]{truncationAsMapEntry(cfs, truncatedAt, position)})).thenCompose((resultSet) -> {
            if(truncationRecords != null) {
               truncationRecords.put(cfs.metadata.id, Pair.create(position, Long.valueOf(truncatedAt)));
            }

            return forceFlush("local");
         });
      });
   }

   public static CompletableFuture<Void> maybeRemoveTruncationRecord(TableId id) {
      return TPCUtils.withLock(GLOBAL_LOCK, () -> {
         Pair<CommitLogPosition, Long> truncationRecord = (Pair)getTruncationRecords().get(id);
         return truncationRecord == null?TPCUtils.completedFuture():removeTruncationRecord(id);
      });
   }

   public static CompletableFuture<Void> removeTruncationRecord(TableId id) {
      String req = "DELETE truncated_at[?] from system.%s WHERE key = '%s'";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"local", "local"}), new Object[]{id.asUUID()})).thenCompose((resultSet) -> {
         if(truncationRecords != null) {
            truncationRecords.remove(id);
         }

         return forceFlush("local");
      });
   }

   private static Map<UUID, ByteBuffer> truncationAsMapEntry(ColumnFamilyStore cfs, long truncatedAt, CommitLogPosition position) {
      try {
         DataOutputBuffer out = (DataOutputBuffer)DataOutputBuffer.scratchBuffer.get();
         Throwable var5 = null;

         Map var6;
         try {
            CommitLogPosition.serializer.serialize((CommitLogPosition)position, out);
            out.writeLong(truncatedAt);
            var6 = Collections.singletonMap(cfs.metadata.id.asUUID(), out.asNewBuffer());
         } catch (Throwable var16) {
            var5 = var16;
            throw var16;
         } finally {
            if(out != null) {
               if(var5 != null) {
                  try {
                     out.close();
                  } catch (Throwable var15) {
                     var5.addSuppressed(var15);
                  }
               } else {
                  out.close();
               }
            }

         }

         return var6;
      } catch (IOException var18) {
         throw new RuntimeException(var18);
      }
   }

   public static long getTruncatedAt(TableId id) {
      Pair<CommitLogPosition, Long> record = getTruncationRecord(id);
      return record == null?-9223372036854775808L:((Long)record.right).longValue();
   }

   private static Pair<CommitLogPosition, Long> getTruncationRecord(TableId id) {
      return (Pair)getTruncationRecords().get(id);
   }

   private static Map<TableId, Pair<CommitLogPosition, Long>> getTruncationRecords() {
      verify(truncationRecords != null, "startup methods not yet called");
      return truncationRecords;
   }

   public static CompletableFuture<ConcurrentMap<TableId, Pair<CommitLogPosition, Long>>> readTruncationRecords() {
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format("SELECT truncated_at FROM system.%s WHERE key = '%s'", new Object[]{"local", "local"}), new Object[0])).thenApply((rows) -> {
         ConcurrentMap<TableId, Pair<CommitLogPosition, Long>> records = new ConcurrentHashMap();
         if(!rows.isEmpty() && rows.one().has("truncated_at")) {
            Map<UUID, ByteBuffer> map = rows.one().getMap("truncated_at", UUIDType.instance, BytesType.instance);
            Iterator var3 = map.entrySet().iterator();

            while(var3.hasNext()) {
               Entry<UUID, ByteBuffer> entry = (Entry)var3.next();
               records.put(TableId.fromUUID((UUID)entry.getKey()), truncationRecordFromBlob((ByteBuffer)entry.getValue()));
            }
         }

         return records;
      });
   }

   private static Pair<CommitLogPosition, Long> truncationRecordFromBlob(ByteBuffer bytes) {
      try {
         RebufferingInputStream in = new DataInputBuffer(bytes, true);
         Throwable var2 = null;

         Pair var3;
         try {
            var3 = Pair.create(CommitLogPosition.serializer.deserialize(in), Long.valueOf(in.available() > 0?in.readLong():-9223372036854775808L));
         } catch (Throwable var13) {
            var2 = var13;
            throw var13;
         } finally {
            if(in != null) {
               if(var2 != null) {
                  try {
                     in.close();
                  } catch (Throwable var12) {
                     var2.addSuppressed(var12);
                  }
               } else {
                  in.close();
               }
            }

         }

         return var3;
      } catch (IOException var15) {
         throw new RuntimeException(var15);
      }
   }

   public static CompletableFuture<Void> updateTokens(InetAddress ep, Collection<Token> tokens) {
      return TPCUtils.withLock(GLOBAL_LOCK, () -> {
         if(ep.equals(FBUtilities.getBroadcastAddress())) {
            return TPCUtils.completedFuture();
         } else {
            String req = "INSERT INTO system.%s (peer, tokens) VALUES (?, ?)";
            logger.debug("PEERS TOKENS for {} = {}", ep, tokensAsSet(tokens));
            return TPCUtils.toFutureVoid(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"peers"}), new Object[]{ep, tokensAsSet(tokens)}));
         }
      });
   }

   private static CompletableFuture<ConcurrentMap<InetAddress, PeerInfo>> readPeerInfo() {
      String req = "SELECT * from system.peers";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(req, new Object[0])).thenApply((results) -> {
         ConcurrentMap<InetAddress, PeerInfo> ret = new ConcurrentHashMap();
         Iterator var2 = results.iterator();

         while(var2.hasNext()) {
            UntypedResultSet.Row row = (UntypedResultSet.Row)var2.next();
            ret.put(row.getInetAddress("peer"), new PeerInfo(row));
         }

         return ret;
      });
   }

   public static CompletableFuture<Void> updatePreferredIP(InetAddress ep, InetAddress preferred_ip) {
      return TPCUtils.withLock(GLOBAL_LOCK, () -> {
         InetAddress current = getPreferredIPIfAvailable(ep);
         if(current == preferred_ip) {
            return TPCUtils.completedFuture();
         } else {
            String req = "INSERT INTO system.%s (peer, preferred_ip) VALUES (?, ?)";
            return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"peers"}), new Object[]{ep, preferred_ip})).thenCompose((r) -> {
               if(peers != null) {
                  ((PeerInfo)peers.computeIfAbsent(ep, (key) -> {
                     return new PeerInfo();
                  })).setPreferredIp(preferred_ip);
               }

               return forceFlush("peers");
            });
         }
      });
   }

   public static CompletableFuture<Void> updatePeerInfo(InetAddress ep, String columnName, Object value) {
      return TPCUtils.withLock(GLOBAL_LOCK, () -> {
         if(ep.equals(FBUtilities.getBroadcastAddress())) {
            return TPCUtils.completedFuture();
         } else {
            String req = "INSERT INTO system.%s (peer, %s) VALUES (?, ?)";
            return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"peers", columnName}), new Object[]{ep, value})).thenAccept((resultSet) -> {
               if(peers != null) {
                  ((PeerInfo)peers.computeIfAbsent(ep, (key) -> {
                     return new PeerInfo();
                  })).setValue(columnName, value);
               }

            });
         }
      });
   }

   public static CompletableFuture<UntypedResultSet> loadPeerInfo(InetAddress ep, String columnName) {
      String req = "SELECT %s FROM system.%s WHERE peer = '%s'";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{columnName, "peers", ep.getHostAddress()}), new Object[0]));
   }

   public static CompletableFuture<Void> updateHintsDropped(InetAddress ep, UUID timePeriod, int value) {
      return TPCUtils.withLock(GLOBAL_LOCK, () -> {
         String req = "UPDATE system.%s USING TTL 2592000 SET hints_dropped[ ? ] = ? WHERE peer = ?";
         return TPCUtils.toFutureVoid(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"peer_events"}), new Object[]{timePeriod, Integer.valueOf(value), ep}));
      });
   }

   public static CompletableFuture<Void> updateSchemaVersion(UUID version) {
      return updateLocalInfo("schema_version", version);
   }

   public static CompletableFuture<Void> updateLocalInfo(String columnName, Object value) {
      if(columnName.equals("host_id")) {
         if(!(value instanceof UUID)) {
            throw new IllegalArgumentException("Expected UUID for host_id column");
         } else {
            return setLocalHostId((UUID)value).thenAccept((uuid) -> {
            });
         }
      } else if(columnName.equals("bootstrapped")) {
         if(!(value instanceof SystemKeyspace.BootstrapState)) {
            throw new IllegalArgumentException("Expected BootstrapState for bootstrapped column");
         } else {
            return setBootstrapState((SystemKeyspace.BootstrapState)value).thenAccept((state) -> {
            });
         }
      } else if(columnName.equals("truncated_at")) {
         throw new IllegalArgumentException("Truncation records should be updated one by one via saveTruncationRecord");
      } else {
         return TPCUtils.withLock(GLOBAL_LOCK, () -> {
            String req = "INSERT INTO system.%s (key, %s) VALUES ('%s', ?)";
            return TPCUtils.toFutureVoid(QueryProcessor.executeInternalAsync(String.format("INSERT INTO system.%s (key, %s) VALUES ('%s', ?)", new Object[]{"local", columnName, "local"}), new Object[]{value}));
         });
      }
   }

   public static CompletableFuture<UntypedResultSet> loadLocalInfo(String columnName) {
      String req = "SELECT %s FROM system.%s WHERE key = '%s'";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{columnName, "local", "local"}), new Object[0]));
   }

   private static Set<String> tokensAsSet(Collection<Token> tokens) {
      if(tokens.isEmpty()) {
         return Collections.emptySet();
      } else {
         Token.TokenFactory factory = StorageService.instance.getTokenFactory();
         Set<String> s = new HashSet(tokens.size());
         Iterator var3 = tokens.iterator();

         while(var3.hasNext()) {
            Token tk = (Token)var3.next();
            s.add(factory.toString(tk));
         }

         return s;
      }
   }

   private static Collection<Token> deserializeTokens(Collection<String> tokensStrings) {
      Token.TokenFactory factory = StorageService.instance.getTokenFactory();
      List<Token> tokens = new ArrayList(tokensStrings.size());
      Iterator var3 = tokensStrings.iterator();

      while(var3.hasNext()) {
         String tk = (String)var3.next();
         tokens.add(factory.fromString(tk));
      }

      return tokens;
   }

   public static CompletableFuture<Void> removeEndpoint(InetAddress ep) {
      return TPCUtils.withLock(GLOBAL_LOCK, () -> {
         String req = "DELETE FROM system.%s WHERE peer = ?";
         return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"peers"}), new Object[]{ep})).thenCompose((resultSet) -> {
            if(peers != null) {
               peers.remove(ep);
            }

            return forceFlush("peers");
         });
      });
   }

   public static CompletableFuture<Void> updateTokens(Collection<Token> tokens) {
      verify(!tokens.isEmpty(), "removeEndpoint should be used instead");
      return TPCUtils.withLock(GLOBAL_LOCK, () -> {
         return getSavedTokens().thenCompose((savedTokens) -> {
            if(tokens.containsAll(savedTokens) && tokens.size() == savedTokens.size()) {
               return TPCUtils.completedFuture();
            } else {
               String req = "INSERT INTO system.%s (key, tokens) VALUES ('%s', ?)";
               return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"local", "local"}), new Object[]{tokensAsSet(tokens)})).thenCompose((resultSet) -> {
                  return forceFlush("local");
               });
            }
         });
      });
   }

   private static CompletableFuture<Void> forceFlush(String cfname) {
      return !DatabaseDescriptor.isUnsafeSystem()?Keyspace.open("system").getColumnFamilyStore(cfname).forceFlush().thenApply((pos) -> {
         return null;
      }):CompletableFuture.completedFuture((Object)null);
   }

   public static CompletableFuture<SetMultimap<InetAddress, Token>> loadTokens() {
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync("SELECT peer, tokens FROM system.peers", new Object[0])).thenApply((resultSet) -> {
         SetMultimap<InetAddress, Token> tokenMap = HashMultimap.create();
         Iterator var2 = resultSet.iterator();

         while(var2.hasNext()) {
            UntypedResultSet.Row row = (UntypedResultSet.Row)var2.next();
            InetAddress peer = row.getInetAddress("peer");
            if(row.has("tokens")) {
               tokenMap.putAll(peer, deserializeTokens(row.getSet("tokens", UTF8Type.instance)));
            }
         }

         return tokenMap;
      });
   }

   public static Map<InetAddress, UUID> getHostIds() {
      checkPeersCache();
      Map<InetAddress, UUID> hostIdMap = new HashMap();
      Iterator var1 = peers.entrySet().iterator();

      while(var1.hasNext()) {
         Entry<InetAddress, PeerInfo> entry = (Entry)var1.next();
         hostIdMap.put(entry.getKey(), ((PeerInfo)entry.getValue()).hostId);
      }

      return hostIdMap;
   }

   public static InetAddress getPreferredIP(InetAddress ep) {
      checkPeersCache();
      return getPreferredIPIfAvailable(ep);
   }

   private static InetAddress getPreferredIPIfAvailable(InetAddress ep) {
      PeerInfo info = peers == null?null:(PeerInfo)peers.get(ep);
      return info != null && info.preferredIp != null?info.preferredIp:ep;
   }

   public static Map<InetAddress, Map<String, String>> loadDcRackInfo() {
      checkPeersCache();
      Map<InetAddress, Map<String, String>> result = new HashMap();
      Iterator var1 = peers.entrySet().iterator();

      while(var1.hasNext()) {
         Entry<InetAddress, PeerInfo> entry = (Entry)var1.next();
         if(((PeerInfo)entry.getValue()).rack != null && ((PeerInfo)entry.getValue()).dc != null) {
            Map<String, String> dcRack = new HashMap();
            dcRack.put("data_center", ((PeerInfo)entry.getValue()).dc);
            dcRack.put("rack", ((PeerInfo)entry.getValue()).rack);
            result.put(entry.getKey(), dcRack);
         }
      }

      return result;
   }

   public static CassandraVersion getReleaseVersion(InetAddress ep) {
      checkPeersCache();
      if(FBUtilities.getBroadcastAddress().equals(ep)) {
         return new CassandraVersion(FBUtilities.getReleaseVersionString());
      } else {
         PeerInfo info = (PeerInfo)peers.get(ep);
         return info == null?null:info.version;
      }
   }

   public static UUID getSchemaVersion(InetAddress ep) {
      checkPeersCache();
      if(FBUtilities.getBroadcastAddress().equals(ep)) {
         return Schema.instance.getVersion();
      } else {
         PeerInfo info = (PeerInfo)peers.get(ep);
         return info == null?null:info.schemaVersion;
      }
   }

   public static CompletableFuture<Void> checkHealth() throws ConfigurationException {
      Keyspace keyspace;
      try {
         keyspace = Keyspace.open("system");
      } catch (AssertionError var3) {
         ConfigurationException ex = new ConfigurationException("Could not read system keyspace!");
         ex.initCause(var3);
         throw ex;
      }

      ColumnFamilyStore cfs = keyspace.getColumnFamilyStore("local");
      String req = "SELECT cluster_name FROM system.%s WHERE key='%s'";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"local", "local"}), new Object[0])).thenAccept((result) -> {
         if(!result.isEmpty() && result.one().has("cluster_name")) {
            String savedClusterName = result.one().getString("cluster_name");
            if(!DatabaseDescriptor.getClusterName().equals(savedClusterName)) {
               throw new ConfigurationException("Saved cluster name " + savedClusterName + " != configured name " + DatabaseDescriptor.getClusterName());
            }
         } else if(!cfs.getLiveSSTables().isEmpty()) {
            throw new ConfigurationException("Found system keyspace files, but they couldn't be loaded!");
         }
      });
   }

   public static CompletableFuture<Collection<Token>> getSavedTokens() {
      String req = "SELECT tokens FROM system.%s WHERE key='%s'";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"local", "local"}), new Object[0])).thenApply((result) -> {
         return (Collection)(!result.isEmpty() && result.one().has("tokens")?deserializeTokens(result.one().getSet("tokens", UTF8Type.instance)):Collections.emptyList());
      });
   }

   public static CompletableFuture<Integer> incrementAndGetGeneration() {
      String req = "SELECT gossip_generation FROM system.%s WHERE key='%s'";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"local", "local"}), new Object[0])).thenCompose((result) -> {
         int generation;
         if(!result.isEmpty() && result.one().has("gossip_generation")) {
            int storedGeneration = result.one().getInt("gossip_generation") + 1;
            int now = (int)(System.currentTimeMillis() / 1000L);
            if(storedGeneration >= now) {
               logger.warn("Using stored Gossip Generation {} as it is greater than current system time {}.  See CASSANDRA-3654 if you experience problems", Integer.valueOf(storedGeneration), Integer.valueOf(now));
               generation = storedGeneration;
            } else {
               generation = now;
            }
         } else {
            generation = (int)(System.currentTimeMillis() / 1000L);
         }

         String insert = "INSERT INTO system.%s (key, gossip_generation) VALUES ('%s', ?)";
         return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(insert, new Object[]{"local", "local"}), new Object[]{Integer.valueOf(generation)})).thenCompose((r) -> {
            return forceFlush("local");
         }).thenApply((r) -> {
            return Integer.valueOf(generation);
         });
      });
   }

   private static CompletableFuture<SystemKeyspace.BootstrapState> loadBootstrapState() {
      String req = "SELECT bootstrapped FROM system.%s WHERE key='%s'";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"local", "local"}), new Object[0])).thenApply((result) -> {
         return !result.isEmpty() && result.one().has("bootstrapped")?SystemKeyspace.BootstrapState.valueOf(result.one().getString("bootstrapped")):SystemKeyspace.BootstrapState.NEEDS_BOOTSTRAP;
      });
   }

   public static SystemKeyspace.BootstrapState getBootstrapState() {
      return bootstrapState;
   }

   public static boolean bootstrapComplete() {
      return getBootstrapState() == SystemKeyspace.BootstrapState.COMPLETED;
   }

   public static boolean bootstrapInProgress() {
      return getBootstrapState() == SystemKeyspace.BootstrapState.IN_PROGRESS;
   }

   public static boolean wasDecommissioned() {
      return getBootstrapState() == SystemKeyspace.BootstrapState.DECOMMISSIONED;
   }

   public static CompletableFuture<Void> setBootstrapState(SystemKeyspace.BootstrapState state) {
      logger.debug("Setting bootstrap state to {}", state.name());
      return TPCUtils.withLock(GLOBAL_LOCK, () -> {
         if(getBootstrapState() == state) {
            return TPCUtils.completedFuture();
         } else {
            String req = "INSERT INTO system.%s (key, bootstrapped) VALUES ('%s', ?)";
            return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"local", "local"}), new Object[]{state.name()})).thenCompose((resultSet) -> {
               bootstrapState = state;
               return forceFlush("local");
            });
         }
      });
   }

   public static CompletableFuture<Boolean> isIndexBuilt(String keyspaceName, String indexName) {
      String req = "SELECT index_name FROM %s.\"%s\" WHERE table_name=? AND index_name=?";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"system", "IndexInfo"}), new Object[]{keyspaceName, indexName})).thenApply((result) -> {
         return Boolean.valueOf(!result.isEmpty());
      });
   }

   public static CompletableFuture<Void> setIndexBuilt(String keyspaceName, String indexName) {
      String req = "INSERT INTO %s.\"%s\" (table_name, index_name) VALUES (?, ?) IF NOT EXISTS;";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"system", "IndexInfo"}), new Object[]{keyspaceName, indexName})).thenCompose((resultSet) -> {
         return forceFlush("IndexInfo");
      });
   }

   public static CompletableFuture<Void> setIndexRemoved(String keyspaceName, String indexName) {
      return isIndexBuilt(keyspaceName, indexName).thenCompose((built) -> {
         if(!built.booleanValue()) {
            return TPCUtils.completedFuture();
         } else {
            String req = "DELETE FROM %s.\"%s\" WHERE table_name = ? AND index_name = ? IF EXISTS";
            return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"system", "IndexInfo"}), new Object[]{keyspaceName, indexName})).thenCompose((resultSet) -> {
               return forceFlush("IndexInfo");
            });
         }
      });
   }

   public static CompletableFuture<List<String>> getBuiltIndexes(String keyspaceName) {
      String req = "SELECT table_name, index_name from %s.\"%s\" WHERE table_name=?";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"system", "IndexInfo"}), new Object[]{keyspaceName})).thenApply((results) -> {
         return (List)StreamSupport.stream(results.spliterator(), false).map((r) -> {
            return r.getString("index_name");
         }).collect(Collectors.toList());
      });
   }

   public static CompletableFuture<List<String>> getBuiltIndexes(String keyspaceName, Set<String> indexNames) {
      List<String> names = new ArrayList(indexNames);
      String req = "SELECT index_name from %s.\"%s\" WHERE table_name=? AND index_name IN ?";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"system", "IndexInfo"}), new Object[]{keyspaceName, names})).thenApply((results) -> {
         return (List)StreamSupport.stream(results.spliterator(), false).map((r) -> {
            return r.getString("index_name");
         }).collect(Collectors.toList());
      });
   }

   public static UUID getLocalHostId() {
      verify(localHostId != null, "startup methods not yet called");
      return localHostId;
   }

   public static CompletableFuture<UUID> setLocalHostId() {
      return localHostId == null?initialLocalHostId().thenCompose((hostId) -> {
         return setLocalHostId(hostId);
      }):TPCUtils.completedFuture(localHostId);
   }

   private static CompletableFuture<UUID> initialLocalHostId() {
      String req = "SELECT host_id FROM system.%s WHERE key='%s'";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"local", "local"}), new Object[0])).thenApply((result) -> {
         if(!result.isEmpty() && result.one().has("host_id")) {
            return result.one().getUUID("host_id");
         } else {
            UUID hostId = UUID.randomUUID();
            logger.warn("No host ID found, created {} (Note: This should happen exactly once per node).", hostId);
            return hostId;
         }
      });
   }

   public static CompletableFuture<UUID> setLocalHostId(UUID hostId) {
      return TPCUtils.withLock(GLOBAL_LOCK, () -> {
         UUID old = localHostId;
         localHostId = hostId;
         String req = "INSERT INTO system.%s (key, host_id) VALUES ('%s', ?)";
         return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"local", "local"}), new Object[]{hostId})).handle((result, error) -> {
            if(error != null) {
               logger.error("Failed to change local host id from {} to {}", new Object[]{localHostId, hostId, error});
               localHostId = old;
            }

            return localHostId;
         });
      });
   }

   public static CompletableFuture<String> getRack() {
      String req = "SELECT rack FROM system.%s WHERE key='%s'";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"local", "local"}), new Object[0])).thenApply((result) -> {
         return !result.isEmpty() && result.one().has("rack")?result.one().getString("rack"):null;
      });
   }

   public static CompletableFuture<String> getDatacenter() {
      String req = "SELECT data_center FROM system.%s WHERE key='%s'";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"local", "local"}), new Object[0])).thenApply((result) -> {
         return !result.isEmpty() && result.one().has("data_center")?result.one().getString("data_center"):null;
      });
   }

   public static CompletableFuture<PaxosState> loadPaxosState(DecoratedKey key, TableMetadata metadata, int nowInSec) {
      String req = "SELECT * FROM system.%s WHERE row_key = ? AND cf_id = ?";
      return TPCUtils.toFuture(QueryProcessor.executeInternalWithNow(nowInSec, System.nanoTime(), String.format(req, new Object[]{"paxos"}), new Object[]{key.getKey(), metadata.id.asUUID()})).thenApply((results) -> {
         if(results.isEmpty()) {
            return new PaxosState(key, metadata);
         } else {
            UntypedResultSet.Row row = results.one();
            Commit promised = row.has("in_progress_ballot")?new Commit(row.getUUID("in_progress_ballot"), new PartitionUpdate(metadata, key, metadata.regularAndStaticColumns(), 1)):Commit.emptyCommit(key, metadata);
            Commit accepted = row.has("proposal_version") && row.has("proposal")?new Commit(row.getUUID("proposal_ballot"), PartitionUpdate.fromBytes(row.getBytes("proposal"), getVersion(row, "proposal_version"))):Commit.emptyCommit(key, metadata);
            Commit mostRecent = row.has("most_recent_commit_version") && row.has("most_recent_commit")?new Commit(row.getUUID("most_recent_commit_at"), PartitionUpdate.fromBytes(row.getBytes("most_recent_commit"), getVersion(row, "most_recent_commit_version"))):Commit.emptyCommit(key, metadata);
            return new PaxosState(promised, accepted, mostRecent);
         }
      });
   }

   private static EncodingVersion getVersion(UntypedResultSet.Row row, String name) {
      int messagingVersion = row.getInt(name);
      MessagingVersion version = MessagingVersion.fromHandshakeVersion(messagingVersion);
      return ((WriteVerbs.WriteVersion)version.groupVersion(Verbs.Group.WRITES)).encodingVersion;
   }

   public static CompletableFuture<Void> savePaxosPromise(Commit promise) {
      String req = "UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET in_progress_ballot = ? WHERE row_key = ? AND cf_id = ?";
      return TPCUtils.toFutureVoid(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"paxos"}), new Object[]{Long.valueOf(UUIDGen.microsTimestamp(promise.ballot)), Integer.valueOf(paxosTtlSec(promise.update.metadata())), promise.ballot, promise.update.partitionKey().getKey(), promise.update.metadata().id.asUUID()}));
   }

   public static CompletableFuture<Void> savePaxosProposal(Commit proposal) {
      String req = "UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET proposal_ballot = ?, proposal = ?, proposal_version = ? WHERE row_key = ? AND cf_id = ?";
      return TPCUtils.toFutureVoid(QueryProcessor.executeInternalAsync(String.format(req, new Object[]{"paxos"}), new Object[]{Long.valueOf(UUIDGen.microsTimestamp(proposal.ballot)), Integer.valueOf(paxosTtlSec(proposal.update.metadata())), proposal.ballot, PartitionUpdate.toBytes(proposal.update, ((WriteVerbs.WriteVersion)MessagingService.current_version.groupVersion(Verbs.Group.WRITES)).encodingVersion), Integer.valueOf(MessagingService.current_version.protocolVersion().handshakeVersion), proposal.update.partitionKey().getKey(), proposal.update.metadata().id.asUUID()}));
   }

   public static int paxosTtlSec(TableMetadata metadata) {
      return Math.max(10800, metadata.params.gcGraceSeconds);
   }

   public static CompletableFuture<Void> savePaxosCommit(Commit commit) {
      String cql = "UPDATE system.%s USING TIMESTAMP ? AND TTL ? SET proposal_ballot = null, proposal = null, most_recent_commit_at = ?, most_recent_commit = ?, most_recent_commit_version = ? WHERE row_key = ? AND cf_id = ?";
      return TPCUtils.toFutureVoid(QueryProcessor.executeInternalAsync(String.format(cql, new Object[]{"paxos"}), new Object[]{Long.valueOf(UUIDGen.microsTimestamp(commit.ballot)), Integer.valueOf(paxosTtlSec(commit.update.metadata())), commit.ballot, PartitionUpdate.toBytes(commit.update, ((WriteVerbs.WriteVersion)MessagingService.current_version.groupVersion(Verbs.Group.WRITES)).encodingVersion), Integer.valueOf(MessagingService.current_version.protocolVersion().handshakeVersion), commit.update.partitionKey().getKey(), commit.update.metadata().id.asUUID()}));
   }

   public static CompletableFuture<RestorableMeter> getSSTableReadMeter(String keyspace, String table, int generation) {
      String cql = "SELECT * FROM system.%s WHERE keyspace_name=? and columnfamily_name=? and generation=?";
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(cql, new Object[]{"sstable_activity"}), new Object[]{keyspace, table, Integer.valueOf(generation)})).thenApply((results) -> {
         if(results.isEmpty()) {
            return new RestorableMeter();
         } else {
            UntypedResultSet.Row row = results.one();
            double m15rate = row.getDouble("rate_15m");
            double m120rate = row.getDouble("rate_120m");
            return new RestorableMeter(m15rate, m120rate);
         }
      });
   }

   public static CompletableFuture<Void> persistSSTableReadMeter(String keyspace, String table, int generation, RestorableMeter meter) {
      String cql = "INSERT INTO system.%s (keyspace_name, columnfamily_name, generation, rate_15m, rate_120m) VALUES (?, ?, ?, ?, ?) USING TTL 864000";
      return TPCUtils.toFutureVoid(QueryProcessor.executeInternalAsync(String.format(cql, new Object[]{"sstable_activity"}), new Object[]{keyspace, table, Integer.valueOf(generation), Double.valueOf(meter.fifteenMinuteRate()), Double.valueOf(meter.twoHourRate())}));
   }

   public static CompletableFuture<Void> clearSSTableReadMeter(String keyspace, String table, int generation) {
      String cql = "DELETE FROM system.%s WHERE keyspace_name=? AND columnfamily_name=? and generation=?";
      return TPCUtils.toFutureVoid(QueryProcessor.executeInternalAsync(String.format(cql, new Object[]{"sstable_activity"}), new Object[]{keyspace, table, Integer.valueOf(generation)}));
   }

   public static CompletableFuture<Void> updateSizeEstimates(String keyspace, String table, Map<Range<Token>, Pair<Long, Long>> estimates) {
      long timestamp = FBUtilities.timestampMicros();
      PartitionUpdate update = new PartitionUpdate(SizeEstimates, UTF8Type.instance.decompose(keyspace), SizeEstimates.regularAndStaticColumns(), estimates.size());
      Mutation mutation = new Mutation(update);
      int nowInSec = FBUtilities.nowInSeconds();
      update.add(new RangeTombstone(Slice.make(SizeEstimates.comparator, new Object[]{table}), new DeletionTime(timestamp - 1L, nowInSec)));
      Iterator var8 = estimates.entrySet().iterator();

      while(var8.hasNext()) {
         Entry<Range<Token>, Pair<Long, Long>> entry = (Entry)var8.next();
         Range<Token> range = (Range)entry.getKey();
         Pair<Long, Long> values = (Pair)entry.getValue();
         update.add(Rows.simpleBuilder(SizeEstimates, new Object[]{table, ((Token)range.left).toString(), ((Token)range.right).toString()}).timestamp(timestamp).add("partitions_count", values.left).add("mean_partition_size", values.right).build());
      }

      return TPCUtils.toFuture(mutation.applyAsync());
   }

   public static CompletableFuture<Void> clearSizeEstimates(String keyspace, String table) {
      String cql = String.format("DELETE FROM %s WHERE keyspace_name = ? AND table_name = ?", new Object[]{SizeEstimates.toString()});
      return TPCUtils.toFutureVoid(QueryProcessor.executeInternalAsync(cql, new Object[]{keyspace, table}));
   }

   public static CompletableFuture<Void> clearSizeEstimates(String keyspace) {
      String cql = String.format("DELETE FROM %s.%s WHERE keyspace_name = ?", new Object[]{"system", "size_estimates"});
      return TPCUtils.toFutureVoid(QueryProcessor.executeInternalAsync(cql, new Object[]{keyspace}));
   }

   public static synchronized CompletableFuture<SetMultimap<String, String>> getTablesWithSizeEstimates() {
      SetMultimap<String, String> keyspaceTableMap = HashMultimap.create();
      String cql = String.format("SELECT keyspace_name, table_name FROM %s.%s", new Object[]{"system", "size_estimates"});
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(cql, new Object[0])).thenApply((resultSet) -> {
         Iterator var2 = resultSet.iterator();

         while(var2.hasNext()) {
            UntypedResultSet.Row row = (UntypedResultSet.Row)var2.next();
            keyspaceTableMap.put(row.getString("keyspace_name"), row.getString("table_name"));
         }

         return keyspaceTableMap;
      });
   }

   public static CompletableFuture<Void> updateAvailableRanges(String keyspace, Collection<Range<Token>> completedRanges) {
      return TPCUtils.withLock(GLOBAL_LOCK, () -> {
         String cql = "UPDATE system.%s SET ranges = ranges + ? WHERE keyspace_name = ?";
         return TPCUtils.toFutureVoid(QueryProcessor.executeInternalAsync(String.format(cql, new Object[]{"available_ranges"}), new Object[]{rangesToUpdate(completedRanges), keyspace}));
      });
   }

   public static CompletableFuture<Set<Range<Token>>> getAvailableRanges(String keyspace, IPartitioner partitioner) {
      return TPCUtils.withLock(GLOBAL_LOCK, () -> {
         String query = "SELECT * FROM system.%s WHERE keyspace_name=?";
         return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(query, new Object[]{"available_ranges"}), new Object[]{keyspace})).thenApply((rs) -> {
            Set<Range<Token>> result = new HashSet();
            Iterator var3 = rs.iterator();

            while(var3.hasNext()) {
               UntypedResultSet.Row row = (UntypedResultSet.Row)var3.next();
               Set<ByteBuffer> rawRanges = row.getSet("ranges", BytesType.instance);
               Iterator var6 = rawRanges.iterator();

               while(var6.hasNext()) {
                  ByteBuffer rawRange = (ByteBuffer)var6.next();
                  result.add(byteBufferToRange(rawRange, partitioner));
               }
            }

            return ImmutableSet.copyOf(result);
         });
      });
   }

   public static CompletableFuture<Void> resetAvailableRanges(String keyspace) {
      String cql = "UPDATE system.%s SET ranges = null WHERE keyspace_name = ?";
      return TPCUtils.toFutureVoid(QueryProcessor.executeInternalAsync(String.format(cql, new Object[]{"available_ranges"}), new Object[]{keyspace}));
   }

   public static CompletableFuture<Void> resetAvailableRanges(String keyspace, Collection<Range<Token>> ranges) {
      String cql = "UPDATE system.%s SET ranges = ranges - ? WHERE keyspace_name = ?";
      return TPCUtils.toFutureVoid(QueryProcessor.executeInternalAsync(String.format(cql, new Object[]{"available_ranges"}), new Object[]{rangesToUpdate(ranges), keyspace}));
   }

   public static void resetAvailableRangesBlocking() {
      ColumnFamilyStore availableRanges = Keyspace.open("system").getColumnFamilyStore("available_ranges");
      availableRanges.truncateBlocking();
   }

   public static CompletableFuture<Void> updateTransferredRanges(StreamOperation streamOperation, InetAddress peer, String keyspace, Collection<Range<Token>> streamedRanges) {
      return TPCUtils.withLock(GLOBAL_LOCK, () -> {
         String cql = "UPDATE system.%s SET ranges = ranges + ? WHERE operation = ? AND peer = ? AND keyspace_name = ?";
         return TPCUtils.toFutureVoid(QueryProcessor.executeInternalAsync(String.format(cql, new Object[]{"transferred_ranges"}), new Object[]{rangesToUpdate(streamedRanges), streamOperation.getDescription(), peer, keyspace}));
      });
   }

   private static Set<ByteBuffer> rangesToUpdate(Collection<Range<Token>> ranges) {
      return (Set)ranges.stream().map(SystemKeyspace::rangeToBytes).collect(Collectors.toSet());
   }

   public static CompletableFuture<Map<InetAddress, Set<Range<Token>>>> getTransferredRanges(String description, String keyspace, IPartitioner partitioner) {
      return TPCUtils.withLock(GLOBAL_LOCK, () -> {
         String query = "SELECT * FROM system.%s WHERE operation = ? AND keyspace_name = ?";
         return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format(query, new Object[]{"transferred_ranges"}), new Object[]{description, keyspace})).thenApply((rs) -> {
            Map<InetAddress, Set<Range<Token>>> result = new HashMap();
            Iterator var3 = rs.iterator();

            while(var3.hasNext()) {
               UntypedResultSet.Row row = (UntypedResultSet.Row)var3.next();
               InetAddress peer = row.getInetAddress("peer");
               Set<ByteBuffer> rawRanges = row.getSet("ranges", BytesType.instance);
               Set<Range<Token>> ranges = Sets.newHashSetWithExpectedSize(rawRanges.size());
               Iterator var8 = rawRanges.iterator();

               while(var8.hasNext()) {
                  ByteBuffer rawRange = (ByteBuffer)var8.next();
                  ranges.add(byteBufferToRange(rawRange, partitioner));
               }

               result.put(peer, ranges);
            }

            return ImmutableMap.copyOf(result);
         });
      });
   }

   public static CompletableFuture<Void> snapshotOnVersionChange() {
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(String.format("SELECT release_version FROM %s.%s WHERE key='%s'", new Object[]{"system", "local", "local"}), new Object[0]).observeOn(Schedulers.io())).thenAccept((result) -> {
         String previous = result != null && !result.isEmpty() && result.one().has("release_version")?result.one().getString("release_version"):null;
         String current = FBUtilities.getReleaseVersionString();
         if(previous == null) {
            logger.info("No version in {}.{}. Current version is {}", new Object[]{"system", "local", current});
         } else if(current.equals(previous)) {
            logger.info("Detected current release version {} in {}.{}", new Object[]{current, "system", "local"});
         } else {
            logger.info("Detected version upgrade from {} to {}, snapshotting {} and {} keyspaces.", new Object[]{previous, current, "system", "system_schema"});
            String snapshotName = Keyspace.getTimestampedSnapshotName(String.format("upgrade-%s-%s", new Object[]{previous, current}));
            Keyspace ks = Keyspace.open("system");

            try {
               ks.snapshot(snapshotName, (String)null);
               ks = Keyspace.open("system_schema");
               ks.snapshot(snapshotName, (String)null);
            } catch (IOException var6) {
               throw new CompletionException(var6);
            }
         }

      });
   }

   private static ByteBuffer rangeToBytes(Range<Token> range) {
      try {
         DataOutputBuffer out = new DataOutputBuffer();
         Throwable var2 = null;

         ByteBuffer var3;
         try {
            Range.tokenSerializer.serialize(range, out, BoundsVersion.LEGACY);
            var3 = out.buffer();
         } catch (Throwable var13) {
            var2 = var13;
            throw var13;
         } finally {
            if(out != null) {
               if(var2 != null) {
                  try {
                     out.close();
                  } catch (Throwable var12) {
                     var2.addSuppressed(var12);
                  }
               } else {
                  out.close();
               }
            }

         }

         return var3;
      } catch (IOException var15) {
         throw new IOError(var15);
      }
   }

   private static Range<Token> byteBufferToRange(ByteBuffer rawRange, IPartitioner partitioner) {
      try {
         return (Range)Range.tokenSerializer.deserialize(ByteStreams.newDataInput(ByteBufferUtil.getArray(rawRange)), partitioner, BoundsVersion.LEGACY);
      } catch (IOException var3) {
         throw new IOError(var3);
      }
   }

   public static Single<UntypedResultSet> writePreparedStatement(String loggedKeyspace, MD5Digest key, String cql) {
      logger.debug("stored prepared statement for logged keyspace '{}': '{}'", loggedKeyspace, cql);
      return QueryProcessor.executeInternalAsync(String.format("INSERT INTO %s (logged_keyspace, prepared_id, query_string) VALUES (?, ?, ?)", new Object[]{PreparedStatements.toString()}), new Object[]{loggedKeyspace, key.byteBuffer(), cql});
   }

   public static CompletableFuture<Void> removePreparedStatement(MD5Digest key) {
      return TPCUtils.toFutureVoid(QueryProcessor.executeInternalAsync(String.format("DELETE FROM %s WHERE prepared_id = ?", new Object[]{PreparedStatements.toString()}), new Object[]{key.byteBuffer()}));
   }

   public static void resetPreparedStatementsBlocking() {
      ColumnFamilyStore preparedStatements = Keyspace.open("system").getColumnFamilyStore("prepared_statements");
      preparedStatements.truncateBlocking();
   }

   public static CompletableFuture<List<Pair<String, String>>> loadPreparedStatements() {
      String query = String.format("SELECT logged_keyspace, query_string FROM %s", new Object[]{PreparedStatements.toString()});
      return TPCUtils.toFuture(QueryProcessor.executeOnceInternal(query, new Object[0])).thenApply((resultSet) -> {
         List<Pair<String, String>> r = new ArrayList();
         Iterator var2 = resultSet.iterator();

         while(var2.hasNext()) {
            UntypedResultSet.Row row = (UntypedResultSet.Row)var2.next();
            r.add(Pair.create(row.has("logged_keyspace")?row.getString("logged_keyspace"):null, row.getString("query_string")));
         }

         return r;
      });
   }

   public static DecoratedKey decorateBatchKey(UUID id) {
      return BATCH_PARTITIONER.decorateKey(TimeUUIDType.instance.getSerializer().serialize(id));
   }

   static {
      BATCH_PARTITIONER = new LocalPartitioner(TimeUUIDType.instance);
      Batches = parse("batches", "batches awaiting replay", "CREATE TABLE %s (id timeuuid,mutations list<blob>,version int,PRIMARY KEY ((id)))").partitioner(BATCH_PARTITIONER).compaction(CompactionParams.scts(Collections.singletonMap("min_threshold", "2"))).compression(CompressionParams.forSystemTables()).build();
      Paxos = parse("paxos", "in-progress paxos proposals", "CREATE TABLE %s (row_key blob,cf_id UUID,in_progress_ballot timeuuid,most_recent_commit blob,most_recent_commit_at timeuuid,most_recent_commit_version int,proposal blob,proposal_ballot timeuuid,proposal_version int,PRIMARY KEY ((row_key), cf_id))").compaction(CompactionParams.lcs(Collections.emptyMap())).compression(CompressionParams.forSystemTables()).build();
      BuiltIndexes = parse("IndexInfo", "built column indexes", "CREATE TABLE \"%s\" (table_name text,index_name text,value blob,PRIMARY KEY ((table_name), index_name)) ").build();
      Local = parse("local", "information about the local node", "CREATE TABLE %s (key text,bootstrapped text,broadcast_address inet,cluster_name text,cql_version text,data_center text,gossip_generation int,host_id uuid,listen_address inet,native_protocol_version text,partitioner text,rack text,release_version text,rpc_address inet,schema_version uuid,tokens set<varchar>,truncated_at map<uuid, blob>,native_transport_address inet,native_transport_port int,native_transport_port_ssl int,storage_port int,storage_port_ssl int,jmx_port int,dse_version text,graph boolean,server_id text,workload text,workloads frozen<set<text>>,PRIMARY KEY ((key)))").recordDeprecatedSystemColumn("thrift_version", UTF8Type.instance).build();
      Peers = parse("peers", "information about known peers in the cluster", "CREATE TABLE %s (peer inet,data_center text,host_id uuid,preferred_ip inet,rack text,release_version text,rpc_address inet,schema_version uuid,tokens set<varchar>,native_transport_address inet,native_transport_port int,native_transport_port_ssl int,storage_port int,storage_port_ssl int,jmx_port int,dse_version text,graph boolean,server_id text,workload text,workloads frozen<set<text>>,PRIMARY KEY ((peer)))").build();
      PeerEvents = parse("peer_events", "events related to peers", "CREATE TABLE %s (peer inet,hints_dropped map<uuid, int>,PRIMARY KEY ((peer)))").build();
      RangeXfers = parse("range_xfers", "ranges requested for transfer", "CREATE TABLE %s (token_bytes blob,requested_at timestamp,PRIMARY KEY ((token_bytes)))").build();
      CompactionHistory = parse("compaction_history", "week-long compaction history", "CREATE TABLE %s (id uuid,bytes_in bigint,bytes_out bigint,columnfamily_name text,compacted_at timestamp,keyspace_name text,rows_merged map<int, bigint>,PRIMARY KEY ((id)))").defaultTimeToLive((int)TimeUnit.DAYS.toSeconds(7L)).build();
      SSTableActivity = parse("sstable_activity", "historic sstable read rates", "CREATE TABLE %s (keyspace_name text,columnfamily_name text,generation int,rate_120m double,rate_15m double,PRIMARY KEY ((keyspace_name, columnfamily_name, generation)))").build();
      SizeEstimates = parse("size_estimates", "per-table primary range size estimates", "CREATE TABLE %s (keyspace_name text,table_name text,range_start text,range_end text,mean_partition_size bigint,partitions_count bigint,PRIMARY KEY ((keyspace_name), table_name, range_start, range_end))").build();
      AvailableRanges = parse("available_ranges", "available keyspace/ranges during bootstrap/replace that are ready to be served", "CREATE TABLE %s (keyspace_name text,ranges set<blob>,PRIMARY KEY ((keyspace_name)))").build();
      TransferredRanges = parse("transferred_ranges", "record of transferred ranges for streaming operation", "CREATE TABLE %s (operation text,peer inet,keyspace_name text,ranges set<blob>,PRIMARY KEY ((operation, keyspace_name), peer))").build();
      ViewBuildsInProgress = parse("view_builds_in_progress", "views builds current progress", "CREATE TABLE %s (keyspace_name text,view_name text,start_token varchar,end_token varchar,last_token varchar,keys_built bigint,PRIMARY KEY ((keyspace_name), view_name, start_token, end_token))").build();
      BuiltViews = parse("built_views", "built views", "CREATE TABLE %s (keyspace_name text,view_name text,status_replicated boolean,PRIMARY KEY ((keyspace_name), view_name))").build();
      PreparedStatements = parse("prepared_statements", "prepared statements", "CREATE TABLE %s (prepared_id blob,logged_keyspace text,query_string text,PRIMARY KEY ((prepared_id)))").build();
      Repairs = parse("repairs", "repairs", "CREATE TABLE %s (parent_id timeuuid, started_at timestamp, last_update timestamp, repaired_at timestamp, state int, coordinator inet, participants set<inet>, ranges set<blob>, cfids set<uuid>, PRIMARY KEY (parent_id))").build();
      GLOBAL_LOCK = new ExecutableLock();
      startupState = SystemKeyspace.StartupState.NONE;
      localHostId = null;
      bootstrapState = SystemKeyspace.BootstrapState.NEEDS_BOOTSTRAP;
      peers = null;
      truncationRecords = null;
   }

   private static enum StartupState {
      NONE,
      STARTED,
      COMPLETED;

      private StartupState() {
      }
   }

   public static enum BootstrapState {
      NEEDS_BOOTSTRAP,
      COMPLETED,
      IN_PROGRESS,
      DECOMMISSIONED;

      private BootstrapState() {
      }
   }
}
