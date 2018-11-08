package com.datastax.bdp.db.audit;

import com.google.common.base.Strings;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.UUID;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public class AuditableEvent {
   private static final String HOST = "host:";
   private static final String SOURCE = "|source:";
   private static final String USER = "|user:";
   private static final String AUTHENTICATED = "|authenticated:";
   private static final String TIMESTAMP = "|timestamp:";
   private static final String CATEGORY = "|category:";
   private static final String TYPE = "|type:";
   private static final String BATCH = "|batch:";
   private static final String KS = "|ks:";
   private static final String CF = "|cf:";
   private static final String OPERATION = "|operation:";
   private static final String CONSISTENCY_LEVEL = "|consistency level:";
   private final AuditableEventType type;
   private final UserRolesAndPermissions user;
   private final String source;
   private final InetAddress host;
   private final UUID uid;
   private final long timestamp;
   private final String keyspace;
   private final String columnFamily;
   private final UUID batch;
   private final String operation;
   private final ConsistencyLevel consistencyLevel;
   public static final ConsistencyLevel NO_CL = null;
   public static String UNKNOWN_SOURCE = "/0.0.0.0";

   public AuditableEvent(UserRolesAndPermissions user, AuditableEventType type, String source, UUID uid, UUID batchId, String keyspace, String table, String operation, ConsistencyLevel cl) {
      this.type = type;
      this.user = user;
      this.source = source;
      this.host = FBUtilities.getBroadcastAddress();
      this.uid = uid;
      this.timestamp = UUIDGen.unixTimestamp(uid);
      this.keyspace = keyspace;
      this.columnFamily = table;
      this.batch = batchId;
      this.operation = operation;
      this.consistencyLevel = cl;
   }

   public AuditableEvent(QueryState state, AuditableEventType type, String operation) {
      this(state, type, (UUID)null, (String)null, (String)null, operation, (ConsistencyLevel)null);
   }

   public AuditableEvent(UserRolesAndPermissions user, AuditableEventType type, String source, String operation) {
      this(user, type, source, UUIDGen.getTimeUUID(), (UUID)null, (String)null, (String)null, operation, (ConsistencyLevel)null);
   }

   public AuditableEvent(QueryState queryState, AuditableEventType type, UUID batchId, String keyspace, String table, String operation, ConsistencyLevel cl) {
      this(queryState.getUserRolesAndPermissions(), type, getEventSource(queryState.getClientState()), UUIDGen.getTimeUUID(), batchId, keyspace, table, operation, cl);
   }

   public AuditableEvent(AuditableEvent event, AuditableEventType type, String operation) {
      this.type = type;
      this.user = event.user;
      this.source = event.source;
      this.host = event.host;
      this.uid = UUIDGen.getTimeUUID();
      this.timestamp = event.timestamp;
      this.keyspace = event.keyspace;
      this.columnFamily = event.columnFamily;
      this.batch = event.batch;
      this.operation = operation;
      this.consistencyLevel = event.consistencyLevel;
   }

   public AuditableEventType getType() {
      return this.type;
   }

   public String getUser() {
      return this.user.getName();
   }

   public String getAuthenticated() {
      return this.user.getAuthenticatedName();
   }

   public String getSource() {
      return this.source;
   }

   public InetAddress getHost() {
      return this.host;
   }

   public UUID getUid() {
      return this.uid;
   }

   public long getTimestamp() {
      return this.timestamp;
   }

   public String getOperation() {
      return this.operation;
   }

   public ConsistencyLevel getConsistencyLevel() {
      return this.consistencyLevel;
   }

   public UUID getBatchId() {
      return this.batch;
   }

   public String getColumnFamily() {
      return this.columnFamily;
   }

   public String getKeyspace() {
      return this.keyspace;
   }

   public boolean userHasRole(RoleResource role) {
      return this.user.hasRole(role);
   }

   public String toString() {
      StringBuilder builder = (new StringBuilder(400)).append("host:").append(this.host).append("|source:").append(this.source).append("|user:").append(this.user.getName()).append("|authenticated:").append(this.user.getAuthenticatedName()).append("|timestamp:").append(this.timestamp).append("|category:").append(this.type.getCategory()).append("|type:").append(this.type);
      if(this.batch != null) {
         builder.append("|batch:").append(this.batch);
      }

      if(!Strings.isNullOrEmpty(this.keyspace)) {
         builder.append("|ks:").append(this.keyspace);
      }

      if(!Strings.isNullOrEmpty(this.columnFamily)) {
         builder.append("|cf:").append(this.columnFamily);
      }

      if(!Strings.isNullOrEmpty(this.operation)) {
         builder.append("|operation:").append(this.operation);
      }

      if(this.consistencyLevel != null) {
         builder.append("|consistency level:").append(this.consistencyLevel);
      }

      return builder.toString();
   }

   private static String getEventSource(ClientState state) {
      SocketAddress sockAddress = state.getRemoteAddress();
      if(sockAddress instanceof InetSocketAddress) {
         InetSocketAddress inetSocketAddress = (InetSocketAddress)sockAddress;
         if(!inetSocketAddress.isUnresolved()) {
            return inetSocketAddress.getAddress().toString();
         }
      }

      return UNKNOWN_SOURCE;
   }
}
