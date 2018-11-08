package org.apache.cassandra.auth;

import java.util.Map;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntSupplier;
import org.apache.cassandra.config.DatabaseDescriptor;

public class PermissionsCache extends AuthCache<RoleResource, Map<IResource, PermissionSets>> implements PermissionsCacheMBean {
   public PermissionsCache() {
      this((IAuthorizer)(DatabaseDescriptor.getAuthorizer() != null?DatabaseDescriptor.getAuthorizer():new AllowAllAuthorizer()));
   }

   public PermissionsCache(IAuthorizer authorizer) {
      super("PermissionsCache", DatabaseDescriptor::setPermissionsValidity, DatabaseDescriptor::getPermissionsValidity, DatabaseDescriptor::setPermissionsUpdateInterval, DatabaseDescriptor::getPermissionsUpdateInterval, DatabaseDescriptor::setPermissionsCacheMaxEntries, DatabaseDescriptor::getPermissionsCacheMaxEntries, DatabaseDescriptor::setPermissionsCacheInitialCapacity, DatabaseDescriptor::getPermissionsCacheInitialCapacity, authorizer::allPermissionSets, authorizer::allPermissionSetsMany, authorizer::requireAuthorization);
   }
}
