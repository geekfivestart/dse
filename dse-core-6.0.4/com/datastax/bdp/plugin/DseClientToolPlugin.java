package com.datastax.bdp.plugin;

import com.datastax.bdp.cassandra.auth.CassandraDelegationTokenIdentifier;
import com.datastax.bdp.cassandra.auth.CassandraDelegationTokenSecretManager;
import com.datastax.bdp.cassandra.auth.Credentials;
import com.datastax.bdp.cassandra.auth.negotiators.ProxyAuthenticatedUser;
import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.snitch.EndpointStateTracker;
import com.datastax.bdp.snitch.Workload;
import com.datastax.bdp.util.Addresses;
import com.datastax.bdp.util.MapBuilder;
import com.datastax.bdp.util.rpc.Rpc;
import com.datastax.bdp.util.rpc.RpcClientState;
import com.datastax.bdp.util.rpc.RpcParam;
import com.datastax.bdp.util.rpc.RpcRegistry;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.inject.ConfigurationException;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.AuthenticationException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
@DsePlugin(
   dependsOn = {}
)
public class DseClientToolPlugin extends AbstractPlugin {
   private static final Logger LOGGER = LoggerFactory.getLogger(DseClientToolPlugin.class);
   public static final String RPC_NAME = "DseClientTool";
   public static final String RPC_GET_SPARK_DATACENTERS = "getSparkDataCenters";
   public static final String RPC_GET_SPARK_MASTER_ADDRESS = "getSparkMasterAddress";
   public static final String RPC_GET_SPARK_METRICS_CONFIG = "getSparkMetricsConfig";
   public static final String RPC_GENERATE_DELEGATION_TOKEN = "generateDelegationToken";
   public static final String RPC_RENEW_DELEGATION_TOKEN = "renewDelegationToken";
   public static final String RPC_CANCEL_DELEGATION_TOKEN = "cancelDelegationToken";
   public static final String RPC_TRY_CONNECT = "tryConnect";
   public static final String RPC_GET_SHUFFLE_SERVICE_PORT = "getShuffleServicePort";
   public static final String RPC_CAN_LOGIN = "canLogin";
   public static final String RPC_CHECK_CREDENTIALS = "checkCredentials";
   public static final String RPC_RECONFIG_ALWAYS_ON_SQL = "reconfigAlwaysOnSql";
   public static final String RPC_GET_ALWAYS_ON_SQL_ADDRESS = "getAlwaysOnSqlAddress";
   public static final String RPC_IS_ALWAYS_ON_SQL_ACTIVE = "isAlwaysOnSqlActive";
   @Inject
   private Injector injector;
   @Inject
   private Provider<CassandraDelegationTokenSecretManager> tokenSecretManager;
   @Inject(
      optional = true
   )
   private GraphOLAPPluginMXBean gremlinServerPlugin;

   public DseClientToolPlugin() {
   }

   public void onActivate() {
      super.onActivate();
      RpcRegistry.register("DseClientTool", this);
   }

   public void onPreDeactivate() {
      RpcRegistry.unregister("DseClientTool");
      super.onPreDeactivate();
   }

   public boolean isEnabled() {
      return true;
   }

   @Rpc(
      name = "getSparkDataCenters",
      permission = CorePermission.EXECUTE,
      multiRow = true
   )
   public List<String> getSparkDataCenters() throws IOException {
      return Lists.newArrayList((Iterable)Sets.union((Set)Gossiper.instance.getLiveMembers(),
              (Set<InetAddress>)Gossiper.instance.getUnreachableMembers()).stream().
              filter(host -> EndpointStateTracker.instance.getWorkloads((InetAddress)host).contains((Object)Workload.Analytics)).
              map((Function<InetAddress,String>) EndpointStateTracker.instance::getDatacenter).collect(Collectors.toSet()));
   }

   @Rpc(
      name = "getSparkMasterAddress",
      permission = CorePermission.EXECUTE
   )
   public String getSparkMasterAddress() throws IOException {
      try {
         SparkPluginMXBean sparkPlugin = (SparkPluginMXBean)this.injector.getInstance(SparkPluginMXBean.class);
         return sparkPlugin.isActive()?sparkPlugin.getMasterAddress():"";
      } catch (ConfigurationException var2) {
         LOGGER.info("getSparkMasterAddress was requested but SparkPlugin is not registered ({})", var2.getMessage());
         return "";
      }
   }

   @Rpc(
      name = "getAnalyticsGraphServer",
      permission = CorePermission.EXECUTE
   )
   public Map<String, String> getAnalyticsGraphServer() throws IOException {
      HashMap result = new HashMap(2);

      try {
         if(this.gremlinServerPlugin != null && this.gremlinServerPlugin.isActive()) {
            Optional<String> endpoint = Optional.ofNullable(this.gremlinServerPlugin.getAnalyticsGraphServerIP());
            endpoint.ifPresent((ip) -> {
               result.put("ip", ip);
               result.put("location", String.format("%s:%d", new Object[]{Addresses.Client.getBroadcastAddressOf(ip).getHostAddress(), Integer.valueOf(this.gremlinServerPlugin.getPort())}));
            });
         }
      } catch (Exception var3) {
         LOGGER.info("getAnalyticsGraphServer failed with exception: {}", var3.getMessage());
      }

      return result;
   }

   @Rpc(
      name = "getSparkMetricsConfig",
      permission = CorePermission.EXECUTE
   )
   public Map<String, String> getSparkMetricsConfig() {
      try {
         PerformanceObjectsController.SparkApplicationInfoBean sparkAppInfoMBean = (PerformanceObjectsController.SparkApplicationInfoBean)this.injector.getInstance(PerformanceObjectsController.SparkApplicationInfoBean.class);
         return sparkAppInfoMBean.asSparkProperties();
      } catch (ConfigurationException var2) {
         LOGGER.info("getSparkMetricsConfig was requested but SparkApplicationInfoBean is not registered ({})", var2.getMessage());
         return new HashMap(0);
      }
   }

   @Rpc(
      name = "generateDelegationToken",
      permission = CorePermission.EXECUTE
   )
   public Map<String, ByteBuffer> generateDelegationToken(RpcClientState clientState, @RpcParam(name = "owner") String owner, @RpcParam(name = "renewer") String renewer) {
      if(!DseConfig.isKerberosEnabled()) {
         throw new SecurityException("Kerberos is not enabled in DSE");
      } else {
         String expectedOwner = clientState.user.getName();
         if(owner != null && !Objects.equals(expectedOwner, owner) && !((UserRolesAndPermissions)TPCUtils.blockingGet(DatabaseDescriptor.getAuthManager().getUserRolesAndPermissions(clientState.user))).isSuper()) {
            throw new UnauthorizedException("Invalid token owner");
         } else {
            String realUser = clientState.user.getName();
            if(clientState.user instanceof ProxyAuthenticatedUser) {
               realUser = ((ProxyAuthenticatedUser)clientState.user).authenticatedUser.getName();
            }

            CassandraDelegationTokenIdentifier id = new CassandraDelegationTokenIdentifier(new Text(expectedOwner), renewer != null?new Text(renewer):new Text(""), new Text(realUser));
            byte[] password = ((CassandraDelegationTokenSecretManager)this.tokenSecretManager.get()).createPassword(id);

            assert password != null && password.length > 0;

            return MapBuilder.<String,ByteBuffer>immutable().withKeys(new String[]{"id", "password"}).withValues(new ByteBuffer[]{ByteBuffer.wrap(id.getBytes()), ByteBuffer.wrap(password)}).build();
         }
      }
   }

   @Rpc(
      name = "renewDelegationToken",
      permission = CorePermission.EXECUTE
   )
   public long renewDelegationToken(RpcClientState clientState, @RpcParam(name = "tokenIdentifier") ByteBuffer tokenIdentifier) throws IOException {
      CassandraDelegationTokenIdentifier id = this.getTokenIdentifier(tokenIdentifier);
      return ((UserRolesAndPermissions)TPCUtils.blockingGet(DatabaseDescriptor.getAuthManager().getUserRolesAndPermissions(clientState.user))).isSuper()?((CassandraDelegationTokenSecretManager)this.tokenSecretManager.get()).renewToken(id):((CassandraDelegationTokenSecretManager)this.tokenSecretManager.get()).renewToken(id, clientState.user.getName());
   }

   @Rpc(
      name = "cancelDelegationToken",
      permission = CorePermission.EXECUTE
   )
   public void cancelDelegationToken(RpcClientState clientState, @RpcParam(name = "tokenIdentifier") ByteBuffer tokenIdentifier) throws IOException {
      CassandraDelegationTokenIdentifier id = this.getTokenIdentifier(tokenIdentifier);
      if(((UserRolesAndPermissions)TPCUtils.blockingGet(DatabaseDescriptor.getAuthManager().getUserRolesAndPermissions(clientState.user))).isSuper()) {
         ((CassandraDelegationTokenSecretManager)this.tokenSecretManager.get()).cancelToken(id);
      } else {
         ((CassandraDelegationTokenSecretManager)this.tokenSecretManager.get()).cancelToken(id, clientState.user.getName());
      }

   }

   @Rpc(
      name = "tryConnect",
      permission = CorePermission.EXECUTE
   )
   public boolean tryConnectFromRpcAddress(RpcClientState clientState, @RpcParam(name = "host") InetAddress host, @RpcParam(name = "port") int port) throws Exception {
      try {
         Socket socket;
         if(host.isAnyLocalAddress()) {
            LOGGER.info("Trying to connect to {}:{}", clientState.remoteAddress.getHostName(), Integer.valueOf(port));
            socket = new Socket(clientState.remoteAddress.getHostName(), port);
         } else {
            LOGGER.info("Trying to connect to {}:{}", host, Integer.valueOf(port));
            socket = new Socket(host, port);
         }

         socket.close();
         return true;
      } catch (Exception var5) {
         LOGGER.debug("Failed to test connection to client at {}:{}", host, Integer.valueOf(port));
         return false;
      }
   }

   @Rpc(
      name = "getShuffleServicePort",
      permission = CorePermission.EXECUTE
   )
   public int getShuffleServicePort(@RpcParam(name = "encrypted") boolean encrypted) throws Exception {
      return ((SparkPluginBean)this.injector.getInstance(SparkPluginBean.class)).getShuffleServicePort(encrypted);
   }

   @Rpc(
      name = "canLogin",
      permission = CorePermission.EXECUTE
   )
   public boolean canLogin(@RpcParam(name = "username") String username) throws Exception {
      return DatabaseDescriptor.getRoleManager().canLogin(RoleResource.role(username));
   }

   @Rpc(
      name = "checkCredentials",
      permission = CorePermission.EXECUTE
   )
   public boolean checkCredentials(@RpcParam(name = "username") String username, @RpcParam(name = "password") String password) throws Exception {
      if(DatabaseDescriptor.getAuthenticator().requireAuthentication()) {
         try {
            DatabaseDescriptor.getAuthenticator().legacyAuthenticate((new Credentials(username, password)).toMap());
            return true;
         } catch (AuthenticationException var4) {
            return false;
         }
      } else {
         return true;
      }
   }

   @Rpc(
      name = "reconfigAlwaysOnSql",
      permission = CorePermission.EXECUTE
   )
   public boolean reconfigAlwaysOnSql(@RpcParam(name = "nodeAddress") String nodeAddress) throws Exception {
      ((AlwaysOnSqlPluginMXBean)this.injector.getInstance(AlwaysOnSqlPluginMXBean.class)).reconfigureAlwaysOnSql(nodeAddress);
      return true;
   }

   private CassandraDelegationTokenIdentifier getTokenIdentifier(ByteBuffer identifier) throws IOException {
      CassandraDelegationTokenIdentifier id = new CassandraDelegationTokenIdentifier();
      id.readFields(new DataInputStream(ByteBufferUtil.inputStream(identifier)));
      return id;
   }

   @Rpc(
      name = "getAlwaysOnSqlAddress",
      permission = CorePermission.EXECUTE
   )
   public String getAlwaysOnSqlAddress() throws IOException {
      try {
         AlwaysOnSqlPluginMXBean alwaysOnSqlPlugin = (AlwaysOnSqlPluginMXBean)this.injector.getInstance(AlwaysOnSqlPluginMXBean.class);
         return alwaysOnSqlPlugin.isActive()?alwaysOnSqlPlugin.getServiceAddress():"";
      } catch (ConfigurationException var2) {
         LOGGER.info("getAlwaysOnSqlAddress was requested but AlwaysOnSqlPlugin is not registered ({})", var2.getMessage());
         return "";
      }
   }

   @Rpc(
      name = "isAlwaysOnSqlActive",
      permission = CorePermission.EXECUTE
   )
   public boolean isAlwaysOnSqlActive() throws IOException {
      try {
         AlwaysOnSqlPluginMXBean alwaysOnSqlPlugin = (AlwaysOnSqlPluginMXBean)this.injector.getInstance(AlwaysOnSqlPluginMXBean.class);
         return alwaysOnSqlPlugin.isActive();
      } catch (ConfigurationException var2) {
         return false;
      }
   }
}
