package com.datastax.bdp.leasemanager;

import com.datastax.bdp.cassandra.cql3.StatementUtils;
import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.snitch.EndpointStateTracker;
import com.datastax.bdp.util.Addresses;
import com.datastax.bdp.util.QueryProcessorUtil;
import com.datastax.bdp.util.SchemaTool;
import com.google.common.collect.Lists;
import java.beans.ConstructorProperties;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement.Prepared;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.locator.AbstractReplicationStrategy;
import org.apache.cassandra.locator.NetworkTopologyStrategy;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.ResultMessage.Kind;
import org.apache.cassandra.transport.messages.ResultMessage.Rows;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LeaseMonitorCore {
   private static final Logger logger = LoggerFactory.getLogger(LeaseMonitorCore.class);
   public static final String UPDATE_REPLICATION_DOCS = "http://docs.datastax.com/en/cql/3.1/cql/cql_using/update_ks_rf_t.html";
   public static final String defaultKeyspace = "dse_leases";
   public static final String defaultTable = "leases";
   public static final String defaultLogTable = "logs";
   private static final String schema = "CREATE TABLE IF NOT EXISTS %s (name text, dc text, epoch bigint, holder inet, duration_ms int, PRIMARY KEY((name, dc)))";
   private static final String logSchema = "CREATE TABLE IF NOT EXISTS %s (name text, dc text, monitor inet, at timestamp, old_holder inet, new_holder inet, PRIMARY KEY((name, dc), monitor, at))";
   public static final int MIN_REC_LEASE_DURATION_MS = 20000;
   public static final int MAX_REC_LEASE_DURATION_MS = 300000;
   public final String keyspace;
   public final String table;
   public final String logTable;
   private final QueryState queryState;
   private final CQLStatement acquireLeaseQuery;
   private final CQLStatement renewLeaseQuery;
   private final CQLStatement readLockQuery;
   private final CQLStatement insertLogQuery;
   private final Prepared readLocalLocksQuery;
   private final Prepared readLocalLockQuery;

   LeaseMonitorCore() throws Exception {
      this("dse_leases", "leases", "logs");
   }

   LeaseMonitorCore(String keyspace, String table, String logTable) throws Exception {
      logger.debug(String.format("Creating %s and preparing statements . . .", new Object[]{QueryProcessorUtil.getFullTableName(keyspace, table)}));
      this.keyspace = keyspace;
      this.table = table;
      this.logTable = logTable;
      SchemaTool.maybeCreateOrUpdateKeyspace(this.getMetadata(), 0L);
      this.queryState = QueryState.forInternalCalls();
      this.acquireLeaseQuery = this.prepareStmt("UPDATE %s SET epoch = ?, holder = ? WHERE name = ? and dc = ? IF epoch = ?", QueryProcessorUtil.getFullTableName(keyspace, table));
      this.renewLeaseQuery = this.prepareStmt("UPDATE %s SET epoch = ? WHERE name = ? and dc = ? IF epoch = ?", QueryProcessorUtil.getFullTableName(keyspace, table));
      this.readLockQuery = this.prepareStmt("SELECT * FROM %s WHERE name = ? and dc = ?", QueryProcessorUtil.getFullTableName(keyspace, table));
      this.insertLogQuery = this.prepareStmt("INSERT INTO %s (name, dc, monitor, at, old_holder, new_holder) VALUES (?, ?, ?, ?, ?, ?) USING TTL " + DseConfig.getLeaseMetricsTtl(), QueryProcessorUtil.getFullTableName(keyspace, logTable));
      this.readLocalLocksQuery = QueryProcessor.getStatement(String.format("SELECT * FROM %s", new Object[]{QueryProcessorUtil.getFullTableName(keyspace, table)}), this.queryState);
      this.readLocalLockQuery = QueryProcessor.getStatement(String.format("SELECT * FROM %s WHERE name = ? and dc = ? ", new Object[]{QueryProcessorUtil.getFullTableName(keyspace, table)}), this.queryState);
   }

   private static TableMetadata compile(String keyspace, String table, String comment, String cql) {
      return CreateTableStatement.parse(String.format(cql, new Object[]{table}), keyspace).id(SchemaTool.tableIdForDseSystemTable(keyspace, table)).comment(comment).dcLocalReadRepairChance(0.0D).memtableFlushPeriod((int)TimeUnit.HOURS.toMillis(1L)).gcGraceSeconds((int)TimeUnit.DAYS.toSeconds(14L)).build();
   }

   KeyspaceMetadata getMetadata() {
      return KeyspaceMetadata.create(this.keyspace, KeyspaceParams.simple(1), Tables.of(new TableMetadata[]{compile(this.keyspace, this.table, "DSE Lease coordination", String.format("CREATE TABLE IF NOT EXISTS %s (name text, dc text, epoch bigint, holder inet, duration_ms int, PRIMARY KEY((name, dc)))", new Object[]{QueryProcessorUtil.getFullTableName(this.keyspace, this.table)})), compile(this.keyspace, this.logTable, "DSE Lease history", String.format("CREATE TABLE IF NOT EXISTS %s (name text, dc text, monitor inet, at timestamp, old_holder inet, new_holder inet, PRIMARY KEY((name, dc), monitor, at))", new Object[]{QueryProcessorUtil.getFullTableName(this.keyspace, this.logTable)}))}));
   }

   boolean createLease(String name, String dc, int duration_ms) throws Exception {
      this.verifyDcReplication(dc);
      if(duration_ms < 20000 || duration_ms > 300000) {
         logger.info(String.format("Creating lease %s.%s with duration %d outside the recommended range {%s, %s} ms. Leases with durations less than %d ms will perform poorly, and leases with durations longer than %d ms will sacrifice availability for no performance gain (if the machine fails, the lease will not pass to a new node until it expires).", new Object[]{name, dc, Integer.valueOf(duration_ms), Integer.valueOf(20000), Integer.valueOf(300000), Integer.valueOf(20000), Integer.valueOf(300000)}));
      }

      QueryProcessorUtil.LwtUntypedResultSet res = QueryProcessorUtil.executeLwt(String.format("INSERT INTO %s (name, dc, epoch, duration_ms) VALUES ('%s', '%s', 0, %d) IF NOT EXISTS", new Object[]{QueryProcessorUtil.getFullTableName(this.keyspace, this.table), name, dc, Integer.valueOf(duration_ms)}), consistencyLevelFromDc(dc), new Object[0]);
      boolean success = res.wasApplied();
      logger.info(String.format("%s %s.%s (duration: %dms)", new Object[]{success?"Created":"Failed to create", name, dc, Integer.valueOf(duration_ms)}));
      return success;
   }

   boolean acquireLease(String name, String dc, InetAddress leaseHolder, long epoch) throws Exception {
      ConsistencyLevel cl = consistencyLevelFromDc(dc);
      QueryOptions qo = QueryProcessorUtil.getLwtQueryOptions(cl, Lists.newArrayList(new ByteBuffer[]{ByteBufferUtil.bytes(epoch + 1L), ByteBufferUtil.bytes(leaseHolder), ByteBufferUtil.bytes(name), ByteBufferUtil.bytes(dc), ByteBufferUtil.bytes(epoch)}));
      UntypedResultSet res = this.process(this.acquireLeaseQuery, qo);
      boolean success = QueryProcessorUtil.wasLwtApplied(res);
      logger.info(String.format("%s %s.%s for node %s", new Object[]{success?"Acquired":"Failed to acquire", name, dc, formatInetAddress(leaseHolder)}));
      return success;
   }

   boolean renewLease(String name, String dc, long epoch) throws Exception {
      ConsistencyLevel cl = consistencyLevelFromDc(dc);
      QueryOptions qo = QueryProcessorUtil.getLwtQueryOptions(cl, Lists.newArrayList(new ByteBuffer[]{ByteBufferUtil.bytes(epoch + 1L), ByteBufferUtil.bytes(name), ByteBufferUtil.bytes(dc), ByteBufferUtil.bytes(epoch)}));
      UntypedResultSet res = this.process(this.renewLeaseQuery, qo);
      boolean success = QueryProcessorUtil.wasLwtApplied(res);
      logger.debug(String.format("%s %s.%s (epoch = %d)", new Object[]{success?"Renewed":"Failed to renew", name, dc, Long.valueOf(epoch)}));
      return success;
   }

   boolean disableLease(String name, String dc) throws Exception {
      QueryProcessorUtil.LwtUntypedResultSet res = QueryProcessorUtil.executeLwt(String.format("UPDATE %s SET epoch = %d WHERE name = '%s' AND dc = '%s' IF EXISTS", new Object[]{QueryProcessorUtil.getFullTableName(this.keyspace, this.table), Long.valueOf(-1L), name, dc}), consistencyLevelFromDc(dc), new Object[0]);
      boolean success = res.wasApplied();
      logger.info(String.format("%s %s.%s", new Object[]{success?"Disabled":"Failed to disable", name, dc}));
      return success;
   }

   boolean deleteLease(String name, String dc) throws Exception {
      QueryProcessorUtil.LwtUntypedResultSet res = QueryProcessorUtil.executeLwt(String.format("DELETE FROM %s WHERE name = '%s' AND dc = '%s' IF epoch = %d", new Object[]{QueryProcessorUtil.getFullTableName(this.keyspace, this.table), name, dc, Long.valueOf(-1L)}), consistencyLevelFromDc(dc), new Object[0]);
      boolean success = res.wasApplied();
      logger.info(String.format("%s lease %s.%s", new Object[]{success?"Deleted":"Failed to delete", name, dc}));
      return success;
   }

   boolean cleanupLease(String name, String dc) throws Exception {
      QueryProcessorUtil.LwtUntypedResultSet res = QueryProcessorUtil.executeLwt(String.format("DELETE FROM %s WHERE name = '%s' AND dc = '%s' IF EXISTS", new Object[]{QueryProcessorUtil.getFullTableName(this.keyspace, this.table), name, dc}), ConsistencyLevel.QUORUM, new Object[0]);
      boolean success = res.wasApplied();
      logger.info(String.format("%s lease %s.%s", new Object[]{success?"Cleaned up":"Failed to clean up", name, dc}));
      return success;
   }

   void logMonitorBeliefChange(String name, String dc, InetAddress monitor, long at, InetAddress oldHolder, InetAddress newHolder) throws Exception {
      this.process(this.insertLogQuery, QueryOptions.forInternalCalls(ConsistencyLevel.ONE, Lists.newArrayList(new ByteBuffer[]{ByteBufferUtil.bytes(name), ByteBufferUtil.bytes(dc), toByteBuffer(monitor), ByteBufferUtil.bytes(at), toByteBuffer(oldHolder), toByteBuffer(newHolder)})));
   }

   private static ByteBuffer toByteBuffer(InetAddress inetAddress) {
      return inetAddress == null?ByteBufferUtil.EMPTY_BYTE_BUFFER:ByteBufferUtil.bytes(inetAddress);
   }

   public static boolean isLeaseRowComplete(Row row) {
      return row.has("duration_ms");
   }

   LeaseMonitorCore.LeaseRow readLease(String name, String dc) throws Exception {
      ConsistencyLevel cl = consistencyLevelFromDc(dc);
      QueryOptions qo = QueryOptions.forInternalCalls(QueryProcessorUtil.getSerialCL(cl), Lists.newArrayList(new ByteBuffer[]{ByteBufferUtil.bytes(name), ByteBufferUtil.bytes(dc)}));
      UntypedResultSet res = this.process(this.readLockQuery, qo);
      LeaseMonitorCore.LeaseRow lease = this.getLeaseRowFromResultSet(res);
      logger.debug("Read lease: {}", lease);
      return lease;
   }

   Set<LeaseMonitorCore.LeaseRow> readLeases() throws Exception {
      UntypedResultSet res = QueryProcessorUtil.execute(String.format("SELECT * FROM %s", new Object[]{QueryProcessorUtil.getFullTableName(this.keyspace, this.table)}), ConsistencyLevel.QUORUM, new Object[0]);
      return (Set)StreamSupport.stream(res.spliterator(), false).filter(LeaseMonitorCore::isLeaseRowComplete).map(LeaseMonitorCore.LeaseRow::<init>).collect(Collectors.toSet());
   }

   Set<LeaseMonitorCore.LeaseId> readLocalLeases(String dc) throws Exception {
      QueryOptions qo = QueryOptions.forInternalCalls(Collections.emptyList());
      ResultMessage msg = (ResultMessage)TPCUtils.blockingGet(this.readLocalLocksQuery.statement.executeInternal(this.queryState, qo));
      Set<LeaseMonitorCore.LeaseId> localLeases = new HashSet();
      if(msg instanceof Rows) {
         Iterator var5 = UntypedResultSet.create(((Rows)msg).result).iterator();

         while(var5.hasNext()) {
            Row row = (Row)var5.next();
            if(isLeaseRowComplete(row) && this.inDc(dc, row.getString("dc"))) {
               localLeases.add(new LeaseMonitorCore.LeaseId(row.getString("name"), row.getString("dc")));
            }
         }
      }

      logger.debug("Local leases: {}", localLeases);
      return localLeases;
   }

   LeaseMonitorCore.LeaseRow readLocalLease(String name, String dc) throws Exception {
      QueryOptions qo = QueryOptions.forInternalCalls(Lists.newArrayList(new ByteBuffer[]{ByteBufferUtil.bytes(name), ByteBufferUtil.bytes(dc)}));
      ResultMessage msg = (ResultMessage)TPCUtils.blockingGet(this.readLocalLockQuery.statement.executeInternal(this.queryState, qo));
      LeaseMonitorCore.LeaseRow leaseRow = null;
      if(msg instanceof Rows) {
         leaseRow = this.getLeaseRowFromResultSet(UntypedResultSet.create(((Rows)msg).result));
      }

      logger.debug("Read local lease: {}", leaseRow);
      return leaseRow;
   }

   public LeaseMonitorCore.LeaseRow getLeaseRowFromResultSet(UntypedResultSet res) {
      LeaseMonitorCore.LeaseRow leaseRow = null;
      if(!res.isEmpty()) {
         Row row = res.one();
         if(isLeaseRowComplete(row)) {
            leaseRow = new LeaseMonitorCore.LeaseRow(row);
         }
      }

      return leaseRow;
   }

   private CQLStatement prepareStmt(String cql, String tableName) throws Exception {
      return StatementUtils.prepareStatementBlocking(String.format(cql, new Object[]{tableName}), this.queryState, "Error preparing CQL table info statement");
   }

   private UntypedResultSet process(CQLStatement statement, QueryOptions options) throws Exception {
      ResultMessage resultMessage = (ResultMessage)TPCUtils.blockingGet(statement.execute(this.queryState, options, System.nanoTime()));
      if(resultMessage.kind == Kind.ROWS) {
         return UntypedResultSet.create(((Rows)resultMessage).result);
      } else if(resultMessage.kind == Kind.VOID) {
         return null;
      } else {
         throw new RuntimeException("Unexpected result kind " + resultMessage.kind);
      }
   }

   public static String formatInetAddress(InetAddress addr) {
      return addr == null?"none":addr.getHostAddress();
   }

   public boolean inDc(String dc1, String dc2) {
      return dc1.equals(dc2) || isGlobal(dc1) || isGlobal(dc2);
   }

   private static ConsistencyLevel consistencyLevelFromDc(String dc) {
      return isGlobal(dc)?ConsistencyLevel.QUORUM:ConsistencyLevel.LOCAL_QUORUM;
   }

   public static boolean isGlobal(String dc) {
      return dc.equalsIgnoreCase("GlobalLock");
   }

   private void verifyDcReplication(String dc) throws UnavailableException {
      int totalRF = isGlobal(dc)?Keyspace.open(this.keyspace).getReplicationStrategy().getReplicationFactor():this.getDcRf(dc);
      if(totalRF == 0) {
         throw new UnavailableException(String.format("CF '%s.%s' has no replicas in dc '%s'.  Before you can create a lease in this datacenter, you'll need to increase the replication factor (to 3 if there are enough machines, otherwise 1).  Check the docs at %s for details.", new Object[]{this.keyspace, this.table, dc, "http://docs.datastax.com/en/cql/3.1/cql/cql_using/update_ks_rf_t.html"}), ConsistencyLevel.QUORUM, 3, 0);
      } else {
         if(totalRF < 3) {
            logger.warn(String.format("CF '%s.%s' has only %d replicas in dc '%s', less than the recommended minimum of 3.  A single node failure will render this lease unavailable.  You should increase the replication factor; check the docs at %s for details.", new Object[]{this.keyspace, this.table, Integer.valueOf(totalRF), dc, "http://docs.datastax.com/en/cql/3.1/cql/cql_using/update_ks_rf_t.html"}));
         }

      }
   }

   public int getDcRf(String dc) {
      AbstractReplicationStrategy replicationStrategy = Keyspace.open(this.keyspace).getReplicationStrategy();
      if(replicationStrategy instanceof NetworkTopologyStrategy) {
         return ((NetworkTopologyStrategy)replicationStrategy).getReplicationFactor(dc);
      } else if(replicationStrategy instanceof SimpleStrategy) {
         return replicationStrategy.getReplicationFactor();
      } else {
         throw new RuntimeException(String.format("Please change the replication strategy of %s from %s to NetworkTopologyStrategy.", new Object[]{this.keyspace, replicationStrategy.getClass().getCanonicalName()}));
      }
   }

   public int getLocalDcRf() {
      return this.getDcRf(EndpointStateTracker.instance.getDatacenter(Addresses.Internode.getBroadcastAddress()));
   }

   public static Set<LeaseMonitorCore.LeaseId> rowsToIds(Collection<LeaseMonitorCore.LeaseRow> rows) {
      return (Set)rows.stream().map((row) -> {
         return new LeaseMonitorCore.LeaseId(row.name, row.dc);
      }).collect(Collectors.toSet());
   }

   public static class LeaseRow extends LeaseMonitorCore.LeaseId {
      public final long epoch;
      public final InetAddress holder;
      public final int duration_ms;

      @ConstructorProperties({"name", "dc", "epoch", "holder", "duration_ms"})
      public LeaseRow(String name, String dc, long epoch, String holder, int duration_ms) throws Exception {
         this(name, dc, epoch, holder == null?null:InetAddress.getByName(holder), duration_ms);
      }

      public LeaseRow(String name, String dc, long epoch, InetAddress holder, int duration_ms) {
         super(name, dc);
         this.epoch = epoch;
         this.holder = holder;
         this.duration_ms = duration_ms;
      }

      public LeaseRow(Row r) {
         this(r.getString("name"), r.getString("dc"), r.getLong("epoch"), r.has("holder")?r.getInetAddress("holder"):null, r.getInt("duration_ms"));
      }

      public boolean equals(Object o) {
         if(o != null && o instanceof LeaseMonitorCore.LeaseRow) {
            LeaseMonitorCore.LeaseRow other = (LeaseMonitorCore.LeaseRow)o;
            return this.name.equals(other.name) && this.dc.equals(other.dc) && this.epoch == other.epoch && Objects.equals(this.holder, other.holder) && this.duration_ms == other.duration_ms;
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.name, this.dc, Long.valueOf(this.epoch), this.holder, Integer.valueOf(this.duration_ms)});
      }

      public String toString() {
         return String.format("name: %s, dc: %s, epoch: %d, holder: %s, duration_ms: %d", new Object[]{this.name, this.dc, Long.valueOf(this.epoch), LeaseMonitorCore.formatInetAddress(this.holder), Integer.valueOf(this.duration_ms)});
      }

      public long getEpoch() {
         return this.epoch;
      }

      public String getHolder() {
         return this.holder == null?null:this.holder.getHostAddress();
      }

      public int getDuration_ms() {
         return this.duration_ms;
      }
   }

   public static class LeaseId {
      public final String name;
      public final String dc;

      @ConstructorProperties({"name", "dc"})
      public LeaseId(String name, String dc) {
         this.name = name;
         this.dc = dc;
      }

      public boolean equals(Object o) {
         if(o != null && o instanceof LeaseMonitorCore.LeaseId) {
            LeaseMonitorCore.LeaseId other = (LeaseMonitorCore.LeaseId)o;
            return this.name.equals(other.name) && this.dc.equals(other.dc);
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.name, this.dc});
      }

      public String toString() {
         return this.name + "." + this.dc;
      }

      public String getName() {
         return this.name;
      }

      public String getDc() {
         return this.dc;
      }
   }
}
