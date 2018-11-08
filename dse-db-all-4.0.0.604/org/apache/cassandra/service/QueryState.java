package org.apache.cassandra.service;

import java.net.InetAddress;
import org.apache.cassandra.auth.AuthenticatedUser;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.transport.Connection;

public class QueryState {
   private final ClientState clientState;
   private final int streamId;
   private final UserRolesAndPermissions userRolesAndPermissions;

   private QueryState(QueryState queryState, ClientState clientState, UserRolesAndPermissions userRolesAndPermissions) {
      this(clientState, queryState.streamId, userRolesAndPermissions);
   }

   public QueryState(ClientState clientState, UserRolesAndPermissions userRolesAndPermissions) {
      this(clientState, 0, userRolesAndPermissions);
   }

   public QueryState(ClientState clientState, int streamId, UserRolesAndPermissions userRolesAndPermissions) {
      this.clientState = clientState;
      this.streamId = streamId;
      this.userRolesAndPermissions = userRolesAndPermissions;
   }

   public static QueryState forInternalCalls() {
      return new QueryState(ClientState.forInternalCalls(), UserRolesAndPermissions.SYSTEM);
   }

   public ClientState getClientState() {
      return this.clientState;
   }

   public String getUserName() {
      return this.userRolesAndPermissions.getName();
   }

   public AuthenticatedUser getUser() {
      return this.clientState.getUser();
   }

   public boolean hasUser() {
      return this.clientState.hasUser();
   }

   public QueryState cloneWithKeyspaceIfSet(String keyspace) {
      ClientState clState = this.clientState.cloneWithKeyspaceIfSet(keyspace);
      return clState == this.clientState?this:new QueryState(this, clState, this.userRolesAndPermissions);
   }

   public long getTimestamp() {
      return this.clientState.getTimestamp();
   }

   public InetAddress getClientAddress() {
      return this.clientState.isInternal?null:this.clientState.getRemoteAddress().getAddress();
   }

   public int getStreamId() {
      return this.streamId;
   }

   public Connection getConnection() {
      return this.clientState.connection;
   }

   public boolean isOrdinaryUser() {
      return !this.userRolesAndPermissions.isSuper() && !this.userRolesAndPermissions.isSystem();
   }

   public boolean isSuper() {
      return this.userRolesAndPermissions.isSuper();
   }

   public boolean isSystem() {
      return this.userRolesAndPermissions.isSystem();
   }

   public void checkNotAnonymous() {
      this.userRolesAndPermissions.checkNotAnonymous();
   }

   public boolean hasDataPermission(DataResource resource, Permission perm) {
      return this.userRolesAndPermissions.hasDataPermission(resource, perm);
   }

   public final boolean hasFunctionPermission(FunctionResource resource, Permission perm) {
      return this.userRolesAndPermissions.hasFunctionPermission(resource, perm);
   }

   public final boolean hasPermission(IResource resource, Permission perm) {
      return this.userRolesAndPermissions.hasPermission(resource, perm);
   }

   public boolean hasGrantPermission(IResource resource, Permission perm) {
      return this.userRolesAndPermissions.hasGrantPermission(resource, perm);
   }

   public final void checkAllKeyspacesPermission(Permission perm) {
      this.userRolesAndPermissions.checkAllKeyspacesPermission(perm);
   }

   public final void checkKeyspacePermission(String keyspace, Permission perm) {
      this.userRolesAndPermissions.checkKeyspacePermission(keyspace, perm);
   }

   public final void checkTablePermission(String keyspace, String table, Permission perm) {
      this.userRolesAndPermissions.checkTablePermission(keyspace, table, perm);
   }

   public final void checkTablePermission(TableMetadataRef tableRef, Permission perm) {
      this.userRolesAndPermissions.checkTablePermission(tableRef, perm);
   }

   public final void checkTablePermission(TableMetadata table, Permission perm) {
      this.userRolesAndPermissions.checkTablePermission(table, perm);
   }

   public final void checkFunctionPermission(Function function, Permission permission) {
      this.userRolesAndPermissions.checkFunctionPermission(function, permission);
   }

   public final void checkFunctionPermission(FunctionResource resource, Permission perm) {
      this.userRolesAndPermissions.checkFunctionPermission(resource, perm);
   }

   public final void checkPermission(IResource resource, Permission perm) {
      this.userRolesAndPermissions.checkPermission(resource, perm);
   }

   public final boolean hasRole(RoleResource role) {
      return this.userRolesAndPermissions.hasRole(role);
   }

   public boolean hasRolePermission(RoleResource role, Permission perm) {
      return this.userRolesAndPermissions.hasRolePermission(role, perm);
   }

   public UserRolesAndPermissions getUserRolesAndPermissions() {
      return this.userRolesAndPermissions;
   }
}
