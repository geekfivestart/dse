package org.apache.cassandra.auth;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.UnmodifiableIterator;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.util.concurrent.Futures;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.concurrent.ScheduledExecutors;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.commons.lang3.StringUtils;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CassandraRoleManager implements IRoleManager {
   private static final Logger logger = LoggerFactory.getLogger(CassandraRoleManager.class);
   public static final String DEFAULT_SUPERUSER_NAME = "cassandra";
   public static final String DEFAULT_SUPERUSER_PASSWORD = "cassandra";
   static final String GENSALT_LOG2_ROUNDS_PROPERTY = "cassandra.auth_bcrypt_gensalt_log2_rounds";
   private static final int GENSALT_LOG2_ROUNDS = getGensaltLogRounds();
   private volatile Future<?> setupFuture;
   private SelectStatement loadRoleStatement;
   private SelectStatement checkRolesStatement;
   private final Set<IRoleManager.Option> supportedOptions;
   private final Set<IRoleManager.Option> alterableOptions;
   private volatile boolean isClusterReady = false;

   private static ImmutableSet<RoleResource> rolesFromRow(UntypedResultSet.Row row) {
      if(!row.has("member_of")) {
         return ImmutableSet.of();
      } else {
         Builder<RoleResource> builder = ImmutableSet.builder();
         Iterator var2 = row.getSet("member_of", UTF8Type.instance).iterator();

         while(var2.hasNext()) {
            String role = (String)var2.next();
            builder.add(RoleResource.role(role));
         }

         return builder.build();
      }
   }

   static int getGensaltLogRounds() {
      int rounds = Integer.getInteger("cassandra.auth_bcrypt_gensalt_log2_rounds", 10).intValue();
      if(rounds >= 4 && rounds <= 30) {
         if(rounds > 16) {
            logger.warn("Using a high number of hash rounds, as configured using -D{}={} will consume a lot of CPU and likely cause timeouts. Note that the parameter defines the logarithmic number of rounds: {} becomes 2^{} = {} rounds", new Object[]{"cassandra.auth_bcrypt_gensalt_log2_rounds", Integer.valueOf(rounds), Integer.valueOf(rounds), Integer.valueOf(rounds), Integer.valueOf(1 << rounds)});
         }

         return rounds;
      } else {
         throw new ConfigurationException(String.format("Bad value for system property -D%s=%d. Please use a value between 4 and 30 inclusively", new Object[]{"cassandra.auth_bcrypt_gensalt_log2_rounds", Integer.valueOf(rounds)}));
      }
   }

   public CassandraRoleManager() {
      this.supportedOptions = DatabaseDescriptor.getAuthenticator().isImplementationOf(PasswordAuthenticator.class)?ImmutableSet.of(IRoleManager.Option.LOGIN, IRoleManager.Option.SUPERUSER, IRoleManager.Option.PASSWORD):ImmutableSet.of(IRoleManager.Option.LOGIN, IRoleManager.Option.SUPERUSER);
      this.alterableOptions = DatabaseDescriptor.getAuthenticator().isImplementationOf(PasswordAuthenticator.class)?ImmutableSet.of(IRoleManager.Option.PASSWORD):ImmutableSet.of();
   }

   public Future<?> setup() {
      if(this.setupFuture != null) {
         return this.setupFuture;
      } else {
         this.loadRoleStatement = (SelectStatement)QueryProcessor.parseStatement(String.format("SELECT * FROM %s.%s WHERE role = ?", new Object[]{"system_auth", "roles"})).prepare().statement;
         this.checkRolesStatement = (SelectStatement)QueryProcessor.parseStatement(String.format("SELECT role FROM %s.%s WHERE role IN ?", new Object[]{"system_auth", "roles"})).prepare().statement;
         this.setupFuture = this.scheduleSetupTask();
         return this.setupFuture;
      }
   }

   public Set<IRoleManager.Option> supportedOptions() {
      return this.supportedOptions;
   }

   public Set<IRoleManager.Option> alterableOptions() {
      return this.alterableOptions;
   }

   public void createRole(AuthenticatedUser performer, RoleResource role, RoleOptions options) throws RequestValidationException, RequestExecutionException {
      String insertCql = options.getPassword().isPresent()?String.format("INSERT INTO %s.%s (role, is_superuser, can_login, salted_hash) VALUES ('%s', %s, %s, '%s')", new Object[]{"system_auth", "roles", escape(role.getRoleName()), options.getSuperuser().or(Boolean.valueOf(false)), options.getLogin().or(Boolean.valueOf(false)), escape(hashpw((String)options.getPassword().get()))}):String.format("INSERT INTO %s.%s (role, is_superuser, can_login) VALUES ('%s', %s, %s)", new Object[]{"system_auth", "roles", escape(role.getRoleName()), options.getSuperuser().or(Boolean.valueOf(false)), options.getLogin().or(Boolean.valueOf(false))});
      this.process(insertCql, consistencyForRole(role.getRoleName()));
   }

   public void dropRole(AuthenticatedUser performer, RoleResource role) throws RequestValidationException, RequestExecutionException {
      this.process(String.format("DELETE FROM %s.%s WHERE role = '%s'", new Object[]{"system_auth", "roles", escape(role.getRoleName())}), consistencyForRole(role.getRoleName()));
      this.removeAllMembers(role.getRoleName());
   }

   public void alterRole(AuthenticatedUser performer, RoleResource role, RoleOptions options) {
      String assignments = Joiner.on(',').join(Iterables.filter(this.optionsToAssignments(options.getOptions()), Objects::nonNull));
      if(!Strings.isNullOrEmpty(assignments)) {
         this.process(String.format("UPDATE %s.%s SET %s WHERE role = '%s'", new Object[]{"system_auth", "roles", assignments, escape(role.getRoleName())}), consistencyForRole(role.getRoleName()));
      }

   }

   public void grantRole(AuthenticatedUser performer, RoleResource role, RoleResource grantee) throws RequestValidationException, RequestExecutionException {
      if(this.getRoles(grantee, true).contains(role)) {
         throw new InvalidRequestException(String.format("%s is a member of %s", new Object[]{grantee.getRoleName(), role.getRoleName()}));
      } else if(this.getRoles(role, true).contains(grantee)) {
         throw new InvalidRequestException(String.format("%s is a member of %s", new Object[]{role.getRoleName(), grantee.getRoleName()}));
      } else {
         this.modifyRoleMembership(grantee.getRoleName(), role.getRoleName(), "+");
         this.process(String.format("INSERT INTO %s.%s (role, member) values ('%s', '%s')", new Object[]{"system_auth", "role_members", escape(role.getRoleName()), escape(grantee.getRoleName())}), consistencyForRole(role.getRoleName()));
      }
   }

   public void revokeRole(AuthenticatedUser performer, RoleResource role, RoleResource revokee) throws RequestValidationException, RequestExecutionException {
      if(!this.getRoles(revokee, false).contains(role)) {
         throw new InvalidRequestException(String.format("%s is not a member of %s", new Object[]{revokee.getRoleName(), role.getRoleName()}));
      } else {
         this.modifyRoleMembership(revokee.getRoleName(), role.getRoleName(), "-");
         this.process(String.format("DELETE FROM %s.%s WHERE role = '%s' and member = '%s'", new Object[]{"system_auth", "role_members", escape(role.getRoleName()), escape(revokee.getRoleName())}), consistencyForRole(role.getRoleName()));
      }
   }

   public Set<RoleResource> getRoles(RoleResource grantee, boolean includeInherited) throws RequestValidationException, RequestExecutionException {
      Set<RoleResource> roles = new HashSet();
      Role role = this.getRole(grantee);
      if(!role.equals(Role.NULL_ROLE)) {
         roles.add(RoleResource.role(role.name));
         this.collectRoles(role, roles, includeInherited);
      }

      return roles;
   }

   public Set<RoleResource> getRoleMemberOf(RoleResource role) {
      UntypedResultSet rows = this.process(String.format("SELECT member FROM %s.%s WHERE role = '%s'", new Object[]{"system_auth", "role_members", escape(role.getRoleName())}), consistencyForRole(role.getRoleName()));
      Set<RoleResource> memberOf = new HashSet();
      Iterator var4 = rows.iterator();

      while(var4.hasNext()) {
         UntypedResultSet.Row row = (UntypedResultSet.Row)var4.next();
         memberOf.add(RoleResource.role(row.getString("member")));
      }

      return memberOf;
   }

   public Set<RoleResource> getAllRoles() throws RequestValidationException, RequestExecutionException {
      UntypedResultSet rows = this.process(String.format("SELECT role FROM %s.%s", new Object[]{"system_auth", "roles"}), ConsistencyLevel.QUORUM);
      Iterable<RoleResource> roles = Iterables.transform(rows, (row) -> {
         return RoleResource.role(row.getString("role"));
      });
      return ImmutableSet.builder().addAll(roles).build();
   }

   public boolean isSuper(RoleResource role) {
      return this.getRole(role).isSuper;
   }

   public boolean canLogin(RoleResource role) {
      return this.getRole(role).canLogin;
   }

   public Map<String, String> getCustomOptions(RoleResource role) {
      return Collections.emptyMap();
   }

   public Role getRoleData(RoleResource role) {
      return this.getRole(role);
   }

   public Set<RoleResource> filterExistingRoleNames(List<String> roleNames) {
      List<ByteBuffer> params = Collections.singletonList(ListType.getInstance(UTF8Type.instance, false).getSerializer().serialize(roleNames));
      ResultMessage.Rows rows = (ResultMessage.Rows)TPCUtils.blockingGet(this.checkRolesStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(consistencyForRole("-"), params), System.nanoTime()));
      Set<RoleResource> roles = new HashSet(roleNames.size(), 1.0F);
      Iterator var5 = UntypedResultSet.create(rows.result).iterator();

      while(var5.hasNext()) {
         UntypedResultSet.Row row = (UntypedResultSet.Row)var5.next();
         roles.add(RoleResource.role(row.getString("role")));
      }

      return roles;
   }

   public boolean isExistingRole(RoleResource role) {
      return this.getRole(role) != Role.NULL_ROLE;
   }

   public Set<? extends IResource> protectedResources() {
      return ImmutableSet.of(DataResource.table("system_auth", "roles"), DataResource.table("system_auth", "role_members"));
   }

   public void validateConfiguration() throws ConfigurationException {
   }

   private static boolean needsDefaultRoleSetup() {
      if(StorageService.instance.getTokenMetadata().sortedTokens().isEmpty()) {
         return true;
      } else {
         try {
            return !hasExistingRoles();
         } catch (RequestExecutionException var1) {
            return true;
         }
      }
   }

   private static void setupDefaultRole() {
      if(StorageService.instance.getTokenMetadata().sortedTokens().isEmpty()) {
         throw new IllegalStateException("CassandraRoleManager skipped default role setup: no known tokens in ring");
      } else {
         try {
            if(!hasExistingRoles()) {
               QueryProcessor.processBlocking(String.format("INSERT INTO %s.%s (role, is_superuser, can_login, salted_hash) VALUES ('%s', true, true, '%s') USING TIMESTAMP 0", new Object[]{"system_auth", "roles", "cassandra", escape(hashpw("cassandra"))}), consistencyForRole("cassandra"));
               DatabaseDescriptor.getAuthManager().invalidateRoles(Collections.singleton(RoleResource.role("cassandra")));
               logger.info("Created default superuser role '{}'", "cassandra");
            }

         } catch (RequestExecutionException var1) {
            logger.warn("CassandraRoleManager skipped default role setup: some nodes were not ready");
            throw var1;
         }
      }
   }

   private static boolean hasExistingRoles() throws RequestExecutionException {
      String defaultSUQuery = String.format("SELECT * FROM %s.%s WHERE role = '%s'", new Object[]{"system_auth", "roles", "cassandra"});
      String allUsersQuery = String.format("SELECT * FROM %s.%s LIMIT 1", new Object[]{"system_auth", "roles"});
      return !QueryProcessor.processBlocking(defaultSUQuery, ConsistencyLevel.ONE).isEmpty() || !QueryProcessor.processBlocking(defaultSUQuery, ConsistencyLevel.QUORUM).isEmpty() || !QueryProcessor.processBlocking(allUsersQuery, ConsistencyLevel.QUORUM).isEmpty();
   }

   private Future<?> scheduleSetupTask() {
      if(!needsDefaultRoleSetup()) {
         this.isClusterReady = true;
         return Futures.immediateFuture((Object)null);
      } else {
         long setupDelay = Long.getLong("cassandra.superuser_setup_delay_ms", 10000L).longValue();
         if(setupDelay < 0L) {
            this.doSetupDefaultRole();
            return Futures.immediateFuture((Object)null);
         } else {
            return ScheduledExecutors.optionalTasks.schedule(this::doSetupDefaultRole, setupDelay, TimeUnit.MILLISECONDS);
         }
      }
   }

   private void doSetupDefaultRole() {
      this.isClusterReady = true;

      try {
         setupDefaultRole();
      } catch (Exception var2) {
         logger.info("Setup task failed with error, rescheduling", var2);
         this.scheduleSetupTask();
      }

   }

   private void collectRoles(Role role, Set<RoleResource> collected, boolean includeInherited) throws RequestValidationException, RequestExecutionException {
      UnmodifiableIterator var4 = role.memberOf.iterator();

      while(var4.hasNext()) {
         RoleResource memberOf = (RoleResource)var4.next();
         Role granted = this.getRole(memberOf);
         if(!granted.equals(Role.NULL_ROLE)) {
            collected.add(RoleResource.role(granted.name));
            if(includeInherited) {
               this.collectRoles(granted, collected, true);
            }
         }
      }

   }

   private Role getRole(RoleResource role) {
      Role internalUserRole = AuthenticatedUser.maybeGetInternalUserRole(role);
      if(internalUserRole != null) {
         return internalUserRole;
      } else {
         String roleName = role.getRoleName();
         ResultMessage.Rows rows = (ResultMessage.Rows)TPCUtils.blockingGet(this.loadRoleStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(consistencyForRole(roleName), Collections.singletonList(ByteBufferUtil.bytes(roleName))), System.nanoTime()));
         if(rows.result.isEmpty()) {
            return Role.NULL_ROLE;
         } else {
            UntypedResultSet.Row row = UntypedResultSet.create(rows.result).one();

            try {
               return new Role(row.getString("role"), rolesFromRow(row), row.getBoolean("is_superuser"), row.getBoolean("can_login"), ImmutableMap.of(), row.has("salted_hash")?row.getString("salted_hash"):"");
            } catch (NullPointerException var7) {
               logger.warn("An invalid value has been detected in the {} table for role {}. If you are unable to login, you may need to disable authentication and confirm that values in that table are accurate", "roles", row.getString("role"));
               throw new RuntimeException(String.format("Invalid metadata has been detected for role %s", new Object[]{row.getString("role")}), var7);
            }
         }
      }
   }

   private void modifyRoleMembership(String grantee, String role, String op) throws RequestExecutionException {
      this.process(String.format("UPDATE %s.%s SET member_of = member_of %s {'%s'} WHERE role = '%s'", new Object[]{"system_auth", "roles", op, escape(role), escape(grantee)}), consistencyForRole(grantee));
   }

   private void removeAllMembers(String role) throws RequestValidationException, RequestExecutionException {
      UntypedResultSet rows = this.process(String.format("SELECT member FROM %s.%s WHERE role = '%s'", new Object[]{"system_auth", "role_members", escape(role)}), consistencyForRole(role));
      if(!rows.isEmpty()) {
         Iterator var3 = rows.iterator();

         while(var3.hasNext()) {
            UntypedResultSet.Row row = (UntypedResultSet.Row)var3.next();
            this.modifyRoleMembership(row.getString("member"), role, "-");
         }

         this.process(String.format("DELETE FROM %s.%s WHERE role = '%s'", new Object[]{"system_auth", "role_members", escape(role)}), consistencyForRole(role));
      }
   }

   private Iterable<String> optionsToAssignments(Map<IRoleManager.Option, Object> options) {
      return Iterables.transform(options.entrySet(), (entry) -> {
         switch(null.$SwitchMap$org$apache$cassandra$auth$IRoleManager$Option[((IRoleManager.Option)entry.getKey()).ordinal()]) {
         case 1:
            return String.format("can_login = %s", new Object[]{entry.getValue()});
         case 2:
            return String.format("is_superuser = %s", new Object[]{entry.getValue()});
         case 3:
            return String.format("salted_hash = '%s'", new Object[]{escape(hashpw((String)entry.getValue()))});
         default:
            return null;
         }
      });
   }

   protected static ConsistencyLevel consistencyForRole(String role) {
      return role.equals("cassandra")?ConsistencyLevel.QUORUM:ConsistencyLevel.LOCAL_ONE;
   }

   private static String hashpw(String password) {
      return BCrypt.hashpw(password, BCrypt.gensalt(GENSALT_LOG2_ROUNDS));
   }

   private static String escape(String name) {
      return StringUtils.replace(name, "'", "''");
   }

   private UntypedResultSet process(String query, ConsistencyLevel consistencyLevel) throws RequestValidationException, RequestExecutionException {
      if(!this.isClusterReady) {
         throw new InvalidRequestException("Cannot process role related query as the role manager isn't yet setup. This is likely because some of nodes in the cluster are on version 2.1 or earlier. You need to upgrade all nodes to Cassandra 2.2 or more to use roles.");
      } else {
         return QueryProcessor.processBlocking(query, consistencyLevel);
      }
   }

   @VisibleForTesting
   public void setClusterReadyForTests() {
      this.isClusterReady = true;
   }
}
