package com.datastax.bdp.cassandra.auth;

import com.datastax.bdp.cassandra.cql3.StatementUtils;
import com.datastax.bdp.config.DseConfig;
import com.datastax.bdp.system.DseSecurityKeyspace;
import com.google.common.base.Optional;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.CassandraRoleManager;
import org.apache.cassandra.auth.Role;
import org.apache.cassandra.auth.RoleOptions;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.IRoleManager.Option;
import org.apache.cassandra.concurrent.TPCUtils;
import org.apache.cassandra.cql3.CQLStatement;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.cql3.UntypedResultSet.Row;
import org.apache.cassandra.cql3.statements.DeleteStatement;
import org.apache.cassandra.cql3.statements.SelectStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage.Rows;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DseRoleManager extends CassandraRoleManager {
   private static final Logger logger = LoggerFactory.getLogger(DseRoleManager.class);
   private static final Function<String, List<String>> getLdapRoles = initGroupCache();
   private Set<Option> supportedOptions;
   private Set<Option> alterableOptions;
   private SelectStatement loadOptionsStatement;
   private UpdateStatement insertOptionsStatement;
   private UpdateStatement updateOptionsStatement;
   private DeleteStatement deleteOptionsStatement;
   private MapType<String, String> optionsSerializer;
   private DseRoleManager.RoleManagement roleManagement;

   public DseRoleManager() {
   }

   public Future<?> setup() {
      try {
         Future<?> future = super.setup();
         this.roleManagement = DseRoleManager.RoleManagement.valueOf(DseConfig.getRoleManagement().toUpperCase());
         if(this.roleManagement == DseRoleManager.RoleManagement.LDAP) {
            LdapUtils.instance.setup();
         }

         DseSecurityKeyspace.maybeConfigureKeyspace();
         this.loadOptionsStatement = (SelectStatement)this.prepare("SELECT options from %s.%s WHERE role = ?");
         this.insertOptionsStatement = (UpdateStatement)this.prepare("INSERT INTO %s.%s (role, options) VALUES (?,?)");
         this.updateOptionsStatement = (UpdateStatement)this.prepare("UPDATE %s.%s SET options = ? WHERE role = ?");
         this.deleteOptionsStatement = (DeleteStatement)this.prepare("DELETE FROM %s.%s WHERE role = ?");
         this.optionsSerializer = MapType.getInstance(UTF8Type.instance, UTF8Type.instance, true);
         return future;
      } catch (Exception var2) {
         throw new AssertionError(var2);
      }
   }

   public void validateConfiguration() throws ConfigurationException {
      this.supportedOptions = DseConfig.isInternalAuthEnabled()?ImmutableSet.of(Option.LOGIN, Option.SUPERUSER, Option.PASSWORD, Option.OPTIONS):ImmutableSet.of(Option.LOGIN, Option.SUPERUSER, Option.OPTIONS);
      this.alterableOptions = DseConfig.isInternalAuthEnabled()?ImmutableSet.of(Option.PASSWORD, Option.OPTIONS):ImmutableSet.of(Option.OPTIONS);
      if(DseRoleManager.RoleManagement.valueOf(DseConfig.getRoleManagement().toUpperCase()) == DseRoleManager.RoleManagement.LDAP) {
         LdapUtils.instance.validateGroupConfiguration();
      }

   }

   public Set<Option> supportedOptions() {
      return this.supportedOptions;
   }

   public Set<Option> alterableOptions() {
      return this.alterableOptions;
   }

   public void createRole(AuthenticatedUser performer, RoleResource role, RoleOptions options) throws RequestValidationException, RequestExecutionException {
      super.createRole(performer, role, options);
      Optional<Map<String, String>> customOptions = options.getCustomOptions();
      if(customOptions.isPresent()) {
         Map<String, String> optionMap = (Map)customOptions.get();
         if(!optionMap.isEmpty()) {
            TPCUtils.blockingGet(this.insertOptionsStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(consistencyForRole(role.getRoleName()), Arrays.asList(new ByteBuffer[]{UTF8Type.instance.decompose(role.getRoleName()), this.optionsSerializer.decompose(customOptions.get())})), System.nanoTime()));
         }
      }

   }

   public void dropRole(AuthenticatedUser performer, RoleResource role) throws RequestValidationException, RequestExecutionException {
      super.dropRole(performer, role);
      TPCUtils.blockingGet(this.deleteOptionsStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(consistencyForRole(role.getRoleName()), Collections.singletonList(UTF8Type.instance.decompose(role.getRoleName()))), System.nanoTime()));
   }

   public void alterRole(AuthenticatedUser performer, RoleResource role, RoleOptions options) throws RequestValidationException, RequestExecutionException {
      super.alterRole(performer, role, options);
      Optional<Map<String, String>> customOptions = options.getCustomOptions();
      if(customOptions.isPresent()) {
         Map<String, String> optionMap = (Map)customOptions.get();
         if(optionMap.isEmpty()) {
            TPCUtils.blockingGet(this.deleteOptionsStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(consistencyForRole(role.getRoleName()), Collections.singletonList(UTF8Type.instance.decompose(role.getRoleName()))), System.nanoTime()));
         } else {
            TPCUtils.blockingGet(this.updateOptionsStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(consistencyForRole(role.getRoleName()), Arrays.asList(new ByteBuffer[]{this.optionsSerializer.decompose(optionMap), UTF8Type.instance.decompose(role.getRoleName())})), System.nanoTime()));
         }
      }

   }

   public void grantRole(AuthenticatedUser performer, RoleResource role, RoleResource grantee) throws RequestValidationException, RequestExecutionException {
      if(this.roleManagement == DseRoleManager.RoleManagement.INTERNAL) {
         super.grantRole(performer, role, grantee);
      } else {
         throw new InvalidRequestException("Granting roles is not supported");
      }
   }

   public void revokeRole(AuthenticatedUser performer, RoleResource role, RoleResource revokee) throws RequestValidationException, RequestExecutionException {
      if(this.roleManagement == DseRoleManager.RoleManagement.INTERNAL) {
         super.revokeRole(performer, role, revokee);
      } else {
         throw new InvalidRequestException("Revoking roles is not supported");
      }
   }

   public Set<RoleResource> getRoles(RoleResource grantee, boolean includeInherited) throws RequestValidationException, RequestExecutionException {
      Set roles;
      if(this.roleManagement == DseRoleManager.RoleManagement.INTERNAL) {
         roles = super.getRoles(grantee, includeInherited);
         if(logger.isTraceEnabled()) {
            logger.trace("[role-management] found the following roles for grantee: " + grantee + " - " + roles + " using internal mode");
         }

         return roles;
      } else {
         roles = this.getRolesLDAP(grantee);
         if(logger.isTraceEnabled()) {
            logger.trace("[role-management] found the following roles for grantee: " + grantee + " - " + roles + " using ldap mode");
         }

         return roles;
      }
   }

   public Set<RoleResource> getRoleMemberOf(RoleResource role) {
      return this.roleManagement == DseRoleManager.RoleManagement.INTERNAL?super.getRoleMemberOf(role):Collections.emptySet();
   }

   private Set<RoleResource> getRolesLDAP(RoleResource grantee) {
      return super.filterExistingRoleNames((List)getLdapRoles.apply(grantee.getRoleName()));
   }

   public boolean isExistingRole(RoleResource role) {
      return this.roleManagement == DseRoleManager.RoleManagement.INTERNAL?super.isExistingRole(role):!this.getRoles(role, true).isEmpty();
   }

   public boolean canLogin(RoleResource role) {
      if(this.roleManagement == DseRoleManager.RoleManagement.INTERNAL) {
         return super.canLogin(role);
      } else {
         Iterator var2 = this.getRoles(role, true).iterator();

         RoleResource internalRole;
         do {
            if(!var2.hasNext()) {
               return false;
            }

            internalRole = (RoleResource)var2.next();
         } while(!super.canLogin(internalRole));

         return true;
      }
   }

   public boolean transitiveRoleLogin() {
      return this.roleManagement == DseRoleManager.RoleManagement.LDAP;
   }

   public Map<String, String> getCustomOptions(RoleResource role) {
      return (Map)(this.roleManagement == DseRoleManager.RoleManagement.INTERNAL?super.getCustomOptions(role):this.getRoleData(role).options);
   }

   public Role getRoleData(RoleResource role) {
      Role internalUserRole = AuthenticatedUser.maybeGetInternalUserRole(role);
      if(internalUserRole != null) {
         return internalUserRole;
      } else {
         Role data = super.getRoleData(role);
         if(this.roleManagement == DseRoleManager.RoleManagement.LDAP) {
            data = data.withRoles(role, ImmutableSet.copyOf(this.getRolesIncludingGrantee(role, true)));
         }

         Rows rows = (Rows)TPCUtils.blockingGet(this.loadOptionsStatement.execute(QueryState.forInternalCalls(), QueryOptions.forInternalCalls(consistencyForRole(role.getRoleName()), Collections.singletonList(UTF8Type.instance.decompose(role.getRoleName()))), System.nanoTime()));
         if(rows.result.isEmpty()) {
            return data;
         } else {
            Row row = UntypedResultSet.create(rows.result).one();
            Map<String, String> options = row.has("options")?row.getMap("options", UTF8Type.instance, UTF8Type.instance):Collections.emptyMap();
            return data.withOptions(role, ImmutableMap.copyOf(options));
         }
      }
   }

   private static Function<String, List<String>> initGroupCache() {
      if(DseConfig.getLdapSearchValidity() <= 0) {
         return DseRoleManager::fetchUserGroups;
      } else {
         LoadingCache<String, List<String>> cache = CacheBuilder.newBuilder().expireAfterWrite((long)DseConfig.getLdapSearchValidity(), TimeUnit.SECONDS).build(CacheLoader.from(DseRoleManager::fetchUserGroups));
         return cache::getUnchecked;
      }
   }

   private static List<String> fetchUserGroups(String role) {
      List<String> r = LdapUtils.instance.fetchUserGroups(role);
      r.add(role);
      return r;
   }

   private CQLStatement prepare(String cql) {
      String query = String.format(cql, new Object[]{"dse_security", "role_options"});
      return StatementUtils.prepareStatementBlocking(query, QueryState.forInternalCalls(), "Error preparing \"" + query + "\"");
   }

   private static enum RoleManagement {
      INTERNAL,
      LDAP;

      private RoleManagement() {
      }
   }
}
