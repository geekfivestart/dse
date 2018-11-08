package org.apache.cassandra.service;

import io.reactivex.Single;
import io.reactivex.functions.Function;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryHandler;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.transport.Connection;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ClientState {
   private static final Logger logger = LoggerFactory.getLogger(ClientState.class);
   public static final CassandraVersion DEFAULT_CQL_VERSION;
   private volatile AuthenticatedUser user;
   private volatile String keyspace;
   private volatile boolean sasiWarningIssued = false;
   private static final QueryHandler cqlQueryHandler;
   public final boolean isInternal;
   private final InetSocketAddress remoteAddress;
   public final Connection connection;
   private static final AtomicLong lastTimestampMicros;
   private String clientID;
   private String applicationName;
   private String applicationVersion;
   private String driverName;
   private String driverVersion;

   private ClientState() {
      this.isInternal = true;
      this.remoteAddress = null;
      this.connection = null;
   }

   private ClientState(AuthenticatedUser user) {
      this.isInternal = false;
      this.remoteAddress = null;
      this.connection = null;
      this.user = user;
   }

   protected ClientState(InetSocketAddress remoteAddress, Connection connection) {
      this.isInternal = false;
      this.remoteAddress = remoteAddress;
      this.connection = connection;
      if(!DatabaseDescriptor.getAuthenticator().requireAuthentication()) {
         this.user = AuthenticatedUser.ANONYMOUS_USER;
      }

   }

   protected ClientState(ClientState source) {
      this.isInternal = source.isInternal;
      this.remoteAddress = source.remoteAddress;
      this.connection = source.connection;
      this.user = source.user;
      this.keyspace = source.keyspace;
      this.sasiWarningIssued = source.sasiWarningIssued;
   }

   public static ClientState forInternalCalls() {
      return new ClientState();
   }

   public static ClientState forExternalCalls(AuthenticatedUser user) {
      return new ClientState(user);
   }

   public static ClientState forExternalCalls(SocketAddress remoteAddress, Connection connection) {
      return new ClientState((InetSocketAddress)remoteAddress, connection);
   }

   public ClientState cloneWithKeyspaceIfSet(String keyspace) {
      if(keyspace != null && !keyspace.equals(this.keyspace)) {
         ClientState clientState = new ClientState(this);
         clientState.setKeyspace(keyspace);
         return clientState;
      } else {
         return this;
      }
   }

   public String getClientID() {
      return this.clientID;
   }

   public void setClientID(String clientID) {
      this.clientID = clientID;
   }

   public String getApplicationName() {
      return this.applicationName;
   }

   public void setApplicationName(String applicationName) {
      this.applicationName = applicationName;
   }

   public String getApplicationVersion() {
      return this.applicationVersion;
   }

   public void setApplicationVersion(String applicationVersion) {
      this.applicationVersion = applicationVersion;
   }

   public String getDriverName() {
      return this.driverName;
   }

   public void setDriverName(String driverName) {
      this.driverName = driverName;
   }

   public String getDriverVersion() {
      return this.driverVersion;
   }

   public void setDriverVersion(String driverVersion) {
      this.driverVersion = driverVersion;
   }

   public long getTimestamp() {
      long last;
      long tstamp;
      do {
         long current = System.currentTimeMillis() * 1000L;
         last = lastTimestampMicros.get();
         tstamp = last >= current?last + 1L:current;
      } while(!lastTimestampMicros.compareAndSet(last, tstamp));

      return tstamp;
   }

   public long getTimestampForPaxos(long minTimestampToUse) {
      long last;
      long tstamp;
      do {
         long current = Math.max(System.currentTimeMillis() * 1000L, minTimestampToUse);
         last = lastTimestampMicros.get();
         tstamp = last >= current?last + 1L:current;
      } while(tstamp != minTimestampToUse && !lastTimestampMicros.compareAndSet(last, tstamp));

      return tstamp;
   }

   public static QueryHandler getCQLQueryHandler() {
      return cqlQueryHandler;
   }

   public InetSocketAddress getRemoteAddress() {
      return this.remoteAddress;
   }

   public String getRawKeyspace() {
      return this.keyspace;
   }

   public String getKeyspace() throws InvalidRequestException {
      if(this.keyspace == null) {
         throw new InvalidRequestException("No keyspace has been specified. USE a keyspace, or explicitly specify keyspace.tablename");
      } else {
         return this.keyspace;
      }
   }

   public void setKeyspace(String ks) throws InvalidRequestException {
      if(this.user != null && Schema.instance.getKeyspaceMetadata(ks) == null) {
         throw new InvalidRequestException("Keyspace '" + ks + "' does not exist");
      } else {
         this.keyspace = ks;
      }
   }

   public Single<ClientState> login(AuthenticatedUser user) throws AuthenticationException {
      if(user.isAnonymous()) {
         this.user = user;
         return Single.just(this);
      } else {
         return DatabaseDescriptor.getAuthManager().canLogin(user.getLoginRole()).map((r) -> {
            if(r.booleanValue()) {
               this.user = user;
               if("cassandra".equals(user.getName())) {
                  logger.warn("User '{}' logged in from {}. It is strongly recommended to create and use another user and grant it superuser capabilities and remove the default one. See https://docs.datastax.com/en/dse/6.0/dse-admin/datastax_enterprise/security/Auth/secCreateRootAccount.html", "cassandra", this.remoteAddress);
               }

               return this;
            } else {
               throw new AuthenticationException(String.format("%s is not permitted to log in", new Object[]{user.getName()}));
            }
         });
      }
   }

   public AuthenticatedUser getUser() {
      return this.user;
   }

   public boolean hasUser() {
      return this.user != null;
   }

   public static CassandraVersion[] getCQLSupportedVersion() {
      return new CassandraVersion[]{QueryProcessor.CQL_VERSION};
   }

   public boolean isSASIWarningIssued() {
      return this.sasiWarningIssued;
   }

   public void setSASIWarningIssued() {
      this.sasiWarningIssued = true;
   }

   static {
      DEFAULT_CQL_VERSION = QueryProcessor.CQL_VERSION;
      QueryHandler handler = QueryProcessor.instance;
      String customHandlerClass = System.getProperty("cassandra.custom_query_handler_class");
      if(customHandlerClass != null) {
         try {
            handler = (QueryHandler)FBUtilities.construct(customHandlerClass, "QueryHandler");
            logger.info("Using {} as query handler for native protocol queries (as requested with -Dcassandra.custom_query_handler_class)", customHandlerClass);
         } catch (Exception var3) {
            JVMStabilityInspector.inspectThrowable(var3);
            logger.info("Cannot use class {} as query handler ({}), ignoring by defaulting on normal query handling", customHandlerClass, var3.getMessage());
         }
      }

      cqlQueryHandler = (QueryHandler)handler;
      lastTimestampMicros = new AtomicLong(0L);
   }
}
