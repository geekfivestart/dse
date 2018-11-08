package org.apache.cassandra.tracing;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public final class TraceKeyspace {
   public static final String SESSIONS = "sessions";
   public static final String EVENTS = "events";
   private static final TableMetadata Sessions = parse("sessions", "tracing sessions", "CREATE TABLE %s (session_id uuid,command text,client inet,coordinator inet,duration int,parameters map<text, text>,request text,started_at timestamp,PRIMARY KEY ((session_id)))");
   private static final TableMetadata Events = parse("events", "tracing events", "CREATE TABLE %s (session_id uuid,event_id timeuuid,activity text,source inet,source_elapsed int,thread text,PRIMARY KEY ((session_id), event_id))");

   private TraceKeyspace() {
   }

   private static TableMetadata parse(String table, String description, String cql) {
      return CreateTableStatement.parse(String.format(cql, new Object[]{table}), "system_traces").id(TableId.forSystemTable("system_traces", table)).dcLocalReadRepairChance(0.0D).gcGraceSeconds(0).memtableFlushPeriod((int)TimeUnit.HOURS.toMillis(1L)).comment(description).build();
   }

   public static KeyspaceMetadata metadata() {
      return KeyspaceMetadata.create("system_traces", KeyspaceParams.simple(2), Tables.of(new TableMetadata[]{Sessions, Events}));
   }

   static Mutation makeStartSessionMutation(ByteBuffer sessionId, InetAddress client, Map<String, String> parameters, String request, long startedAt, String command, int ttl) {
      PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(Sessions, new Object[]{sessionId});
      builder.row(new Object[0]).ttl(ttl).add("client", client).add("coordinator", FBUtilities.getBroadcastAddress()).add("request", request).add("started_at", new Date(startedAt)).add("command", command).appendAll("parameters", parameters);
      return builder.buildAsMutation();
   }

   static Mutation makeStopSessionMutation(ByteBuffer sessionId, int elapsed, int ttl) {
      PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(Sessions, new Object[]{sessionId});
      builder.row(new Object[0]).ttl(ttl).add("duration", Integer.valueOf(elapsed));
      return builder.buildAsMutation();
   }

   static Mutation makeEventMutation(ByteBuffer sessionId, String message, int elapsed, String threadName, int ttl) {
      PartitionUpdate.SimpleBuilder builder = PartitionUpdate.simpleBuilder(Events, new Object[]{sessionId});
      Row.SimpleBuilder rowBuilder = builder.row(new Object[]{UUIDGen.getTimeUUID()}).ttl(ttl);
      rowBuilder.add("activity", message).add("source", FBUtilities.getBroadcastAddress()).add("thread", threadName);
      if(elapsed >= 0) {
         rowBuilder.add("source_elapsed", Integer.valueOf(elapsed));
      }

      return builder.buildAsMutation();
   }
}
