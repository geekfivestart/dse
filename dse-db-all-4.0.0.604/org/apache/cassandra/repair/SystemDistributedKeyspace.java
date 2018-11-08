package org.apache.cassandra.repair;

import com.datastax.bdp.db.nodesync.NodeSyncRecord;
import com.datastax.bdp.db.nodesync.Segment;
import com.datastax.bdp.db.nodesync.UserValidationProposer;
import com.datastax.bdp.db.nodesync.ValidationInfo;
import com.datastax.bdp.db.nodesync.ValidationMetrics;
import com.datastax.bdp.db.nodesync.ValidationOutcome;
import com.google.common.base.Joiner;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.CreateTypeStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.RequestTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.repair.messages.RepairOption;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Throwables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SystemDistributedKeyspace {
   private static final String NODESYNC_ERROR_IMPACT_MSG = "this won't prevent NodeSync but may lead to ranges being validated more often than necessary";
   private static final Logger logger = LoggerFactory.getLogger(SystemDistributedKeyspace.class);
   private static final NoSpamLogger noSpamLogger;
   public static final String REPAIR_HISTORY = "repair_history";
   public static final String PARENT_REPAIR_HISTORY = "parent_repair_history";
   public static final String VIEW_BUILD_STATUS = "view_build_status";
   public static final String NODESYNC_VALIDATION = "nodesync_validation";
   public static final String NODESYNC_STATUS = "nodesync_status";
   public static final String NODESYNC_METRICS = "nodesync_metrics";
   public static final String NODESYNC_USER_VALIDATIONS = "nodesync_user_validations";
   private static final TableMetadata RepairHistory;
   private static final TableMetadata ParentRepairHistory;
   private static final TableMetadata ViewBuildStatus;
   public static final UserType NodeSyncValidation;
   private static final String tokenType;
   private static final TableMetadata NodeSyncStatus;
   public static final UserType NodeSyncMetrics;
   private static final TableMetadata NodeSyncUserValidations;

   private SystemDistributedKeyspace() {
   }

   private static TableMetadata.Builder parse(String table, String description, String cql) {
      return parse(table, description, cql, Collections.emptyList());
   }

   private static TableMetadata.Builder parse(String table, String description, String cql, Collection<UserType> types) {
      return CreateTableStatement.parse(String.format(cql, new Object[]{table}), "system_distributed", types).id(TableId.forSystemTable("system_distributed", table)).dcLocalReadRepairChance(0.0D).comment(description);
   }

   private static UserType parseType(String name, String cql) {
      return CreateTypeStatement.parse(String.format(cql, new Object[]{name}), "system_distributed");
   }

   public static KeyspaceMetadata metadata() {
      return KeyspaceMetadata.create("system_distributed", KeyspaceParams.simple(3), Tables.of(new TableMetadata[]{RepairHistory, ParentRepairHistory, ViewBuildStatus, NodeSyncStatus, NodeSyncUserValidations}), Views.none(), types(), Functions.none());
   }

   private static Types types() {
      return Types.of(new UserType[]{NodeSyncValidation, NodeSyncMetrics});
   }

   public static void startParentRepair(UUID parent_id, String keyspaceName, String[] cfnames, RepairOption options) {
      Collection<Range<Token>> ranges = options.getRanges();
      String query = "INSERT INTO %s.%s (parent_id, keyspace_name, columnfamily_names, requested_ranges, started_at,          options) VALUES (%s,        '%s',          { '%s' },           { '%s' },          toTimestamp(now()), { %s })";
      String fmtQry = String.format(query, new Object[]{"system_distributed", "parent_repair_history", parent_id.toString(), keyspaceName, Joiner.on("','").join(cfnames), Joiner.on("','").join(ranges), toCQLMap(options.asMap(), new String[]{"ranges", "columnFamilies"})});
      processSilentBlocking(fmtQry, new String[0]);
   }

   private static String toCQLMap(Map<String, String> options, String... ignore) {
      Set<String> toIgnore = Sets.newHashSet(ignore);
      StringBuilder map = new StringBuilder();
      boolean first = true;
      Iterator var5 = options.entrySet().iterator();

      while(var5.hasNext()) {
         Entry<String, String> entry = (Entry)var5.next();
         if(!toIgnore.contains(entry.getKey())) {
            if(!first) {
               map.append(',');
            }

            first = false;
            map.append(String.format("'%s': '%s'", new Object[]{entry.getKey(), entry.getValue()}));
         }
      }

      return map.toString();
   }

   public static void failParentRepair(UUID parent_id, Throwable t) {
      String query = "UPDATE %s.%s SET finished_at = toTimestamp(now()), exception_message=?, exception_stacktrace=? WHERE parent_id=%s";
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      t.printStackTrace(pw);
      String fmtQuery = String.format(query, new Object[]{"system_distributed", "parent_repair_history", parent_id.toString()});
      String message = t.getMessage();
      processSilentBlocking(fmtQuery, new String[]{message != null?message:"", sw.toString()});
   }

   public static void successfulParentRepair(UUID parent_id, Collection<Range<Token>> successfulRanges) {
      String query = "UPDATE %s.%s SET finished_at = toTimestamp(now()), successful_ranges = {%s} WHERE parent_id=%s";
      String rangesAsString = successfulRanges.isEmpty()?"":String.format("'%s'", new Object[]{Joiner.on("','").join(successfulRanges)});
      String fmtQuery = String.format(query, new Object[]{"system_distributed", "parent_repair_history", rangesAsString, parent_id.toString()});
      processSilentBlocking(fmtQuery, new String[0]);
   }

   public static void startRepairs(UUID id, UUID parent_id, String keyspaceName, String[] cfnames, Collection<Range<Token>> ranges, Iterable<InetAddress> endpoints) {
      String coordinator = FBUtilities.getBroadcastAddress().getHostAddress();
      Set<String> participants = Sets.newHashSet(new String[]{coordinator});
      Iterator var8 = endpoints.iterator();

      while(var8.hasNext()) {
         InetAddress endpoint = (InetAddress)var8.next();
         participants.add(endpoint.getHostAddress());
      }

      String query = "INSERT INTO %s.%s (keyspace_name, columnfamily_name, id, parent_id, range_begin, range_end, coordinator, participants, status, started_at) VALUES (   '%s',          '%s',              %s, %s,        '%s',        '%s',      '%s',        { '%s' },     '%s',   toTimestamp(now()))";
      String[] var17 = cfnames;
      int var10 = cfnames.length;

      for(int var11 = 0; var11 < var10; ++var11) {
         String cfname = var17[var11];
         Iterator var13 = ranges.iterator();

         while(var13.hasNext()) {
            Range<Token> range = (Range)var13.next();
            String fmtQry = String.format(query, new Object[]{"system_distributed", "repair_history", keyspaceName, cfname, id.toString(), parent_id.toString(), ((Token)range.left).toString(), ((Token)range.right).toString(), coordinator, Joiner.on("', '").join(participants), SystemDistributedKeyspace.RepairState.STARTED.toString()});
            processSilentBlocking(fmtQry, new String[0]);
         }
      }

   }

   public static void failRepairs(UUID id, String keyspaceName, String[] cfnames, Throwable t) {
      String[] var4 = cfnames;
      int var5 = cfnames.length;

      for(int var6 = 0; var6 < var5; ++var6) {
         String cfname = var4[var6];
         failedRepairJob(id, keyspaceName, cfname, t);
      }

   }

   public static void successfulRepairJob(UUID id, String keyspaceName, String cfname) {
      String query = "UPDATE %s.%s SET status = '%s', finished_at = toTimestamp(now()) WHERE keyspace_name = '%s' AND columnfamily_name = '%s' AND id = %s";
      String fmtQuery = String.format(query, new Object[]{"system_distributed", "repair_history", SystemDistributedKeyspace.RepairState.SUCCESS.toString(), keyspaceName, cfname, id.toString()});
      processSilentBlocking(fmtQuery, new String[0]);
   }

   public static void failedRepairJob(UUID id, String keyspaceName, String cfname, Throwable t) {
      String query = "UPDATE %s.%s SET status = '%s', finished_at = toTimestamp(now()), exception_message=?, exception_stacktrace=? WHERE keyspace_name = '%s' AND columnfamily_name = '%s' AND id = %s";
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      t.printStackTrace(pw);
      String fmtQry = String.format(query, new Object[]{"system_distributed", "repair_history", SystemDistributedKeyspace.RepairState.FAILED.toString(), keyspaceName, cfname, id.toString()});
      processSilentBlocking(fmtQry, new String[]{t.getMessage(), sw.toString()});
   }

   public static CompletableFuture<Void> startViewBuild(String keyspace, String view, UUID hostId) {
      String query = "INSERT INTO %s.%s (keyspace_name, view_name, host_id, status) VALUES (?, ?, ?, ?)";
      return TPCUtils.toFutureVoid(QueryProcessor.process(String.format(query, new Object[]{"system_distributed", "view_build_status"}), ConsistencyLevel.ONE, Lists.newArrayList(new ByteBuffer[]{ByteBufferUtil.bytes(keyspace), ByteBufferUtil.bytes(view), ByteBufferUtil.bytes(hostId), ByteBufferUtil.bytes(SystemDistributedKeyspace.BuildStatus.STARTED.toString())})));
   }

   public static CompletableFuture<Void> successfulViewBuild(String keyspace, String view, UUID hostId) {
      String query = "UPDATE %s.%s SET status = ? WHERE keyspace_name = ? AND view_name = ? AND host_id = ?";
      return TPCUtils.toFutureVoid(QueryProcessor.process(String.format(query, new Object[]{"system_distributed", "view_build_status"}), ConsistencyLevel.ONE, Lists.newArrayList(new ByteBuffer[]{ByteBufferUtil.bytes(SystemDistributedKeyspace.BuildStatus.SUCCESS.toString()), ByteBufferUtil.bytes(keyspace), ByteBufferUtil.bytes(view), ByteBufferUtil.bytes(hostId)})));
   }

   public static CompletableFuture<Map<UUID, String>> viewStatus(String keyspace, String view) {
      String query = String.format("SELECT host_id, status FROM %s.%s WHERE keyspace_name = ? AND view_name = ?", new Object[]{"system_distributed", "view_build_status"});
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(query, new Object[]{ConsistencyLevel.ONE, keyspace, view})).handle((results, error) -> {
         if(error != null) {
            return Collections.emptyMap();
         } else {
            Map<UUID, String> status = new HashMap();
            Iterator var3 = results.iterator();

            while(var3.hasNext()) {
               UntypedResultSet.Row row = (UntypedResultSet.Row)var3.next();
               status.put(row.getUUID("host_id"), row.getString("status"));
            }

            return status;
         }
      });
   }

   public static CompletableFuture<Void> setViewRemoved(String keyspaceName, String viewName) {
      String buildReq = String.format("DELETE FROM %s.%s WHERE keyspace_name = ? AND view_name = ?", new Object[]{"system_distributed", "view_build_status"});
      return TPCUtils.toFuture(QueryProcessor.executeInternalAsync(buildReq, new Object[]{keyspaceName, viewName})).thenCompose((resultSet) -> {
         return forceFlush("view_build_status");
      });
   }

   private static void processSilentBlocking(String fmtQry, String... values) {
      try {
         List<ByteBuffer> valueList = new ArrayList();
         String[] var3 = values;
         int var4 = values.length;

         for(int var5 = 0; var5 < var4; ++var5) {
            String v = var3[var5];
            valueList.add(ByteBufferUtil.bytes(v));
         }

         TPCUtils.blockingGet(QueryProcessor.process(fmtQry, ConsistencyLevel.ONE, valueList));
      } catch (Throwable var7) {
         logger.error("Error executing query " + fmtQry, var7);
      }

   }

   private static <T> T withNodeSyncExceptionHandling(Callable<T> callable, T defaultOnError, String operationDescription) {
      try {
         return callable.call();
      } catch (Throwable var4) {
         logForNodeSyncOnError(var4, operationDescription);
         return defaultOnError;
      }
   }

   private static void logForNodeSyncOnError(Throwable error, String operationDescription) {
      Throwable cleaned = Throwables.unwrapped(error);
      if(cleaned instanceof UnavailableException) {
         noSpamLogger.warn("No replica available for {} ({}): {}", new Object[]{operationDescription, cleaned.getMessage(), "this won't prevent NodeSync but may lead to ranges being validated more often than necessary"});
      } else if(cleaned instanceof RequestTimeoutException) {
         noSpamLogger.warn("Timeout while {} ({}): {}", new Object[]{operationDescription, cleaned.getMessage(), "this won't prevent NodeSync but may lead to ranges being validated more often than necessary"});
      } else {
         logger.error(String.format("Unexpected error while %s: %s", new Object[]{operationDescription, "this won't prevent NodeSync but may lead to ranges being validated more often than necessary"}), error);
      }

   }

   public static List<NodeSyncRecord> nodeSyncRecords(Segment segment) {
      return nodeSyncRecords(segment.table, segment.range);
   }

   public static List<NodeSyncRecord> nodeSyncRecords(TableMetadata table, Range<Token> range) {
      assert !range.isTrulyWrapAround();

      logger.trace("Requesting NodeSync records for range {} of table {}", range, table);
      Token.TokenFactory tkf = table.partitioner.getTokenFactory();
      Callable<List<NodeSyncRecord>> callable = () -> {
         UntypedResultSet rows = queryNodeSyncRecords(table, range, tkf);
         List<NodeSyncRecord> records = new ArrayList();
         Iterator var5 = rows.iterator();

         while(var5.hasNext()) {
            UntypedResultSet.Row row = (UntypedResultSet.Row)var5.next();

            try {
               Token start = tkf.fromByteArray(row.getBytes("start_token"));
               Token end = tkf.fromByteArray(row.getBytes("end_token"));
               ValidationInfo lastSuccessfulValidation = row.has("last_successful_validation")?ValidationInfo.fromBytes(row.getBytes("last_successful_validation")):null;
               ValidationInfo lastUnsuccessfulValidation = row.has("last_unsuccessful_validation")?ValidationInfo.fromBytes(row.getBytes("last_unsuccessful_validation")):null;
               ValidationInfo lastValidation = lastUnsuccessfulValidation != null && (lastSuccessfulValidation == null || !lastSuccessfulValidation.isMoreRecentThan(lastUnsuccessfulValidation))?lastUnsuccessfulValidation:lastSuccessfulValidation;
               InetAddress lockedBy = row.has("locked_by")?row.getInetAddress("locked_by"):null;
               records.add(new NodeSyncRecord(new Segment(table, new Range(start, end)), lastValidation, lastSuccessfulValidation, lockedBy));
            } catch (RuntimeException var13) {
               noSpamLogger.warn("Unexpected error (msg: {}) reading NodeSync record: {}", new Object[]{var13.getMessage(), "this won't prevent NodeSync but may lead to ranges being validated more often than necessary"});
            }
         }

         return records;
      };
      return (List)withNodeSyncExceptionHandling(callable, Collections.emptyList(), "reading NodeSync records");
   }

   private static int rangeGroupFor(Token token) {
      int val = token.asByteComparableSource().next();

      assert val >= 0 : "Got END_OF_STREAM (" + val + ") as first byte of token";

      return val;
   }

   private static List<ByteBuffer> queriedGroups(Token start, Token end) {
      int startGroup = rangeGroupFor(start);
      int endGroup = end.isMinimum()?255:rangeGroupFor(end);

      assert startGroup <= endGroup : String.format("start=%s (group: %d), end=%s (group: %d)", new Object[]{start, Integer.valueOf(startGroup), end, Integer.valueOf(endGroup)});

      if(startGroup == endGroup) {
         return Collections.singletonList(ByteBufferUtil.bytes((byte)startGroup));
      } else {
         List<ByteBuffer> l = new ArrayList(endGroup - startGroup + 1);

         for(int i = startGroup; i <= endGroup; ++i) {
            l.add(ByteBufferUtil.bytes((byte)i));
         }

         return l;
      }
   }

   private static UntypedResultSet queryNodeSyncRecords(TableMetadata table, Range<Token> range, Token.TokenFactory tkf) {
      Token start = (Token)range.left;
      Token end = (Token)range.right;
      String qBase = "SELECT start_token, end_token, last_successful_validation, last_unsuccessful_validation, locked_by FROM %s.%s WHERE keyspace_name = ? AND table_name = ? AND range_group IN ? AND start_token >= ?";
      if(!end.isMinimum()) {
         qBase = qBase + " AND start_token < ?";
      }

      String q = String.format(qBase, new Object[]{"system_distributed", "nodesync_status"});
      List<ByteBuffer> groups = queriedGroups(start, end);
      ByteBuffer startBytes = tkf.toByteArray(start);
      ByteBuffer endBytes = end.isMinimum()?null:tkf.toByteArray(end);
      return end.isMinimum()?QueryProcessor.execute(q, ConsistencyLevel.ONE, new Object[]{table.keyspace, table.name, groups, startBytes}):QueryProcessor.execute(q, ConsistencyLevel.ONE, new Object[]{table.keyspace, table.name, groups, startBytes, endBytes});
   }

   public static void lockNodeSyncSegment(Segment segment, long timeout, TimeUnit timeoutUnit) {
      logger.trace("Locking NodeSync segment {}", segment);
      Token.TokenFactory tkf = segment.table.partitioner.getTokenFactory();
      String q = "INSERT INTO %s.%s (keyspace_name, table_name, range_group, start_token, end_token, locked_by) VALUES (?, ?, ?, ?, ?, ?) USING TTL ?";
      String query = String.format(q, new Object[]{"system_distributed", "nodesync_status"});
      withNodeSyncExceptionHandling(() -> {
         return QueryProcessor.execute(query, ConsistencyLevel.ONE, new Object[]{segment.table.keyspace, segment.table.name, ByteBufferUtil.bytes((byte)rangeGroupFor((Token)segment.range.left)), tkf.toByteArray((Token)segment.range.left), tkf.toByteArray((Token)segment.range.right), FBUtilities.getBroadcastAddress(), Integer.valueOf((int)timeoutUnit.toSeconds(timeout))});
      }, (Object)null, "recording ongoing NodeSync validation");
   }

   public static void forceReleaseNodeSyncSegmentLock(Segment segment) {
      logger.trace("Force releasing NodeSync segment {}", segment);
      Token.TokenFactory tkf = segment.table.partitioner.getTokenFactory();
      String q = "DELETE locked_by FROM %s.%s WHERE keyspace_name=? AND table_name=? AND range_group=? AND start_token=? AND end_token=?";
      String query = String.format(q, new Object[]{"system_distributed", "nodesync_status"});
      withNodeSyncExceptionHandling(() -> {
         return QueryProcessor.execute(query, ConsistencyLevel.ONE, new Object[]{segment.table.keyspace, segment.table.name, ByteBufferUtil.bytes((byte)rangeGroupFor((Token)segment.range.left)), tkf.toByteArray((Token)segment.range.left), tkf.toByteArray((Token)segment.range.right)});
      }, (Object)null, "releasing NodeSync lock");
   }

   public static void recordNodeSyncValidation(Segment segment, ValidationInfo info, boolean wasPreviousSuccessful) {
      logger.trace("Recording (and unlocking) NodeSync validation of segment {}: {}", segment, info);
      Token.TokenFactory tkf = segment.table.partitioner.getTokenFactory();
      String q = "INSERT INTO %s.%s (keyspace_name, table_name, range_group, start_token, end_token, last_successful_validation, last_unsuccessful_validation, locked_by) VALUES (?, ?, ?, ?, ?, ?, ?, null)";
      String query = String.format(q, new Object[]{"system_distributed", "nodesync_status"});
      ByteBuffer lastSuccessfulValidation;
      ByteBuffer lastUnsuccessfulValidation;
      if(info.wasSuccessful()) {
         lastSuccessfulValidation = info.toBytes();
         lastUnsuccessfulValidation = wasPreviousSuccessful?ByteBufferUtil.UNSET_BYTE_BUFFER:null;
      } else {
         lastSuccessfulValidation = ByteBufferUtil.UNSET_BYTE_BUFFER;
         lastUnsuccessfulValidation = info.toBytes();
      }

      withNodeSyncExceptionHandling(() -> {
         return QueryProcessor.execute(query, ConsistencyLevel.ONE, new Object[]{segment.table.keyspace, segment.table.name, ByteBufferUtil.bytes((byte)rangeGroupFor((Token)segment.range.left)), tkf.toByteArray((Token)segment.range.left), tkf.toByteArray((Token)segment.range.right), lastSuccessfulValidation, lastUnsuccessfulValidation});
      }, (Object)null, "recording NodeSync validation");
   }

   public static void recordNodeSyncUserValidation(UserValidationProposer proposer) {
      UserValidationProposer.Statistics statistics = proposer.statistics();
      List<Range<Token>> ranges = proposer.validatedRanges();
      Set<String> stringRanges = ranges == null?null:(Set)ranges.stream().map(Range::toString).collect(Collectors.toSet());
      ByteBuffer startTime = statistics.startTime() < 0L?null:ByteBufferUtil.bytes(statistics.startTime());
      UserValidationProposer.Status status = proposer.status();
      ByteBuffer endTime;
      switch(null.$SwitchMap$com$datastax$bdp$db$nodesync$UserValidationProposer$Status[status.ordinal()]) {
      case 1:
         endTime = null;
         break;
      case 2:
         endTime = ByteBufferUtil.bytes(statistics.endTime());
         break;
      default:
         endTime = ByteBufferUtil.bytes(System.currentTimeMillis());
      }

      ValidationMetrics metrics = statistics.metrics();
      String q = "INSERT INTO %s.%s (id,keyspace_name,table_name,node,status,validated_ranges,started_at,ended_at,segments_to_validate,segments_validated,outcomes, metrics) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
      String query = String.format(q, new Object[]{"system_distributed", "nodesync_user_validations"});
      withNodeSyncExceptionHandling(() -> {
         return QueryProcessor.execute(query, ConsistencyLevel.ONE, new Object[]{proposer.id().toString(), proposer.table().keyspace, proposer.table().name, DatabaseDescriptor.getListenAddress(), status.toString(), stringRanges, startTime, endTime, Long.valueOf(statistics.segmentsToValidate()), Long.valueOf(statistics.segmentValidated()), ValidationOutcome.toMap(statistics.getOutcomes()), metrics == null?null:metrics.toBytes()});
      }, (Object)null, "recording NodeSync user validation");
   }

   private static CompletableFuture<Void> forceFlush(String table) {
      return !DatabaseDescriptor.isUnsafeSystem()?Keyspace.open("system_distributed").getColumnFamilyStore(table).forceFlush().thenApply((pos) -> {
         return null;
      }):TPCUtils.completedFuture();
   }

   static {
      noSpamLogger = NoSpamLogger.getLogger(logger, 5L, TimeUnit.MINUTES);
      RepairHistory = parse("repair_history", "Repair history", "CREATE TABLE %s (keyspace_name text,columnfamily_name text,id timeuuid,parent_id timeuuid,range_begin text,range_end text,coordinator inet,participants set<inet>,exception_message text,exception_stacktrace text,status text,started_at timestamp,finished_at timestamp,PRIMARY KEY ((keyspace_name, columnfamily_name), id))").build();
      ParentRepairHistory = parse("parent_repair_history", "Repair history", "CREATE TABLE %s (parent_id timeuuid,keyspace_name text,columnfamily_names set<text>,started_at timestamp,finished_at timestamp,exception_message text,exception_stacktrace text,requested_ranges set<text>,successful_ranges set<text>,options map<text, text>,PRIMARY KEY (parent_id))").build();
      ViewBuildStatus = parse("view_build_status", "Materialized View build status", "CREATE TABLE %s (keyspace_name text,view_name text,host_id uuid,status text,PRIMARY KEY ((keyspace_name, view_name), host_id))").build();
      NodeSyncValidation = parseType("nodesync_validation", "CREATE TYPE %s (started_at timestamp,outcome tinyint,missing_nodes set<inet>)");
      tokenType = DatabaseDescriptor.getPartitioner().getTokenValidator().asCQL3Type().toString();
      NodeSyncStatus = parse("nodesync_status", "Tracks NodeSync recent validations", "CREATE TABLE %s (keyspace_name text,table_name text,range_group blob,start_token " + tokenType + ',' + "end_token " + tokenType + ',' + "last_successful_validation frozen<" + "nodesync_validation" + ">,last_unsuccessful_validation frozen<" + "nodesync_validation" + ">,locked_by inet,PRIMARY KEY ((keyspace_name, table_name, range_group), start_token, end_token))", Collections.singleton(NodeSyncValidation)).defaultTimeToLive((int)TimeUnit.DAYS.toSeconds(28L)).build();
      NodeSyncMetrics = parseType("nodesync_metrics", "CREATE TYPE %s (data_validated bigint,data_repaired bigint,objects_validated bigint,objects_repaired bigint,repair_data_sent bigint,repair_objects_sent bigint,pages_outcomes frozen<map<text, bigint>>)");
      NodeSyncUserValidations = parse("nodesync_user_validations", "NodeSync user-triggered validations status", "CREATE TABLE %s (id text,keyspace_name text static,table_name text static,node inet,status text,validated_ranges frozen<set<text>>,started_at timestamp,ended_at timestamp,segments_to_validate bigint,segments_validated bigint,outcomes frozen<map<text, bigint>>,metrics frozen<nodesync_metrics>,PRIMARY KEY (id, node))", Collections.singleton(NodeSyncMetrics)).defaultTimeToLive((int)TimeUnit.DAYS.toSeconds(1L)).build();
   }

   private static enum BuildStatus {
      UNKNOWN,
      STARTED,
      SUCCESS;

      private BuildStatus() {
      }
   }

   private static enum RepairState {
      STARTED,
      SUCCESS,
      FAILED;

      private RepairState() {
      }
   }
}
