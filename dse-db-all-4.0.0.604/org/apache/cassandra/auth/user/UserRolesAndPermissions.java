package org.apache.cassandra.auth.user;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.function.Function;
import java.util.function.Supplier;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.QueryExp;
import org.apache.cassandra.auth.DataResource;
import org.apache.cassandra.auth.FunctionResource;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.JMXResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.PermissionSets;
import org.apache.cassandra.auth.Resources;
import org.apache.cassandra.auth.RoleResource;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnauthorizedException;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaConstants;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class UserRolesAndPermissions {
   private static final Logger logger = LoggerFactory.getLogger(UserRolesAndPermissions.class);
   public static final UserRolesAndPermissions UNKNOWN = new UserRolesAndPermissions("unkown", Collections.emptySet()) {
      public boolean hasJMXPermission(MBeanServer mbs, ObjectName object, Permission permission) {
         return false;
      }

      public boolean hasGrantPermission(IResource resource, Permission perm) {
         return false;
      }

      protected void checkPermissionOnResourceChain(IResource resource, Permission perm) {
         throw new UnauthorizedException("Unknown users are not authorized to perform this request");
      }

      protected boolean hasPermissionOnResourceChain(IResource resource, Permission perm) {
         return false;
      }

      public void additionalQueryPermission(IResource resource, PermissionSets permissionSets) {
      }

      public <R> R filterPermissions(Function<R, R> applicablePermissions, Supplier<R> initialState, UserRolesAndPermissions.RoleResourcePermissionFilter<R> aggregate) {
         return null;
      }
   };
   public static final UserRolesAndPermissions SYSTEM = new UserRolesAndPermissions("system", Collections.emptySet()) {
      public boolean hasGrantPermission(IResource resource, Permission perm) {
         return false;
      }

      public boolean hasDataPermission(DataResource resource, Permission perm) {
         return true;
      }

      public boolean isSystem() {
         return true;
      }

      protected void checkPermissionOnResourceChain(IResource resource, Permission perm) {
      }

      protected boolean hasPermissionOnResourceChain(IResource resource, Permission perm) {
         return true;
      }

      public boolean hasJMXPermission(MBeanServer mbs, ObjectName object, Permission permission) {
         return true;
      }

      public void additionalQueryPermission(IResource resource, PermissionSets permissionSets) {
      }
   };
   public static final UserRolesAndPermissions ANONYMOUS = new UserRolesAndPermissions("anonymous", Collections.emptySet()) {
      public void checkNotAnonymous() {
         throw new UnauthorizedException("Anonymous users are not authorized to perform this request");
      }

      public boolean hasGrantPermission(IResource resource, Permission perm) {
         return false;
      }

      protected boolean hasPermissionOnResourceChain(IResource resource, Permission perm) {
         return this.checkPermission(perm);
      }

      public boolean hasJMXPermission(MBeanServer mbs, ObjectName object, Permission permission) {
         return this.checkPermission(permission);
      }

      protected void checkPermissionOnResourceChain(IResource resource, Permission perm) {
         if(!this.checkPermission(perm)) {
            throw new UnauthorizedException("Anonymous users are not authorized to perform this request");
         }
      }

      private boolean checkPermission(Permission perm) {
         IAuthorizer authorizer = DatabaseDescriptor.getAuthorizer();
         IAuthorizer.TransitionalMode mode = authorizer.getTransitionalMode();
         return !mode.supportPermissionForAnonymous(perm)?false:(!authorizer.requireAuthorization()?true:!mode.enforcePermissionsAgainstAnonymous());
      }

      public void additionalQueryPermission(IResource resource, PermissionSets permissionSets) {
      }
   };
   public static final UserRolesAndPermissions INPROC = new UserRolesAndPermissions("dse_inproc_user", Collections.emptySet()) {
      public boolean isSuper() {
         return true;
      }

      public boolean isSystem() {
         return true;
      }

      public void checkNotAnonymous() {
         throw new UnauthorizedException("In-proc users are not authorized to perform this request");
      }

      public boolean hasGrantPermission(IResource resource, Permission perm) {
         return false;
      }

      protected boolean hasPermissionOnResourceChain(IResource resource, Permission perm) {
         return this.checkPermission(perm);
      }

      public boolean hasJMXPermission(MBeanServer mbs, ObjectName object, Permission permission) {
         return this.checkPermission(permission);
      }

      protected void checkPermissionOnResourceChain(IResource resource, Permission perm) {
         if(!this.checkPermission(perm)) {
            throw new UnauthorizedException("In-proc users are not authorized to perform this request");
         }
      }

      private boolean checkPermission(Permission perm) {
         return perm != CorePermission.AUTHORIZE;
      }

      public void additionalQueryPermission(IResource resource, PermissionSets permissionSets) {
      }
   };
   private final String name;
   private final String authenticatedName;
   private final Set<RoleResource> roles;

   private UserRolesAndPermissions(String name, String authenticatedName, Set<RoleResource> roles) {
      this.name = name;
      this.authenticatedName = authenticatedName;
      this.roles = roles;
   }

   private UserRolesAndPermissions(String name, Set<RoleResource> roles) {
      this.name = name;
      this.authenticatedName = name;
      this.roles = roles;
   }

   public static UserRolesAndPermissions newNormalUserRoles(String name, String authenticatedName, Set<RoleResource> roles) {
      return new UserRolesAndPermissions.NormalUserRoles(name, authenticatedName, roles);
   }

   public static UserRolesAndPermissions newNormalUserRolesAndPermissions(String name, String authenticatedName, Set<RoleResource> roles, Map<RoleResource, Map<IResource, PermissionSets>> permissions) {
      return new UserRolesAndPermissions.NormalUserWithPermissions(name, authenticatedName, roles, permissions);
   }

   public static UserRolesAndPermissions createSuperUserRolesAndPermissions(String name, String authenticatedName, Set<RoleResource> roles) {
      return new UserRolesAndPermissions.SuperUserRoleAndPermissions(name, authenticatedName, roles);
   }

   public final String getName() {
      return this.name;
   }

   public final String getAuthenticatedName() {
      return this.authenticatedName;
   }

   public boolean isSuper() {
      return false;
   }

   public boolean isSystem() {
      return false;
   }

   public void checkNotAnonymous() {
   }

   public boolean hasDataPermission(DataResource resource, Permission perm) {
      if(!DataResource.root().equals(resource)) {
         if(this.isSchemaModification(perm)) {
            String keyspace = resource.getKeyspace();

            try {
               this.preventSystemKSSchemaModification(resource, perm);
            } catch (UnauthorizedException var5) {
               return false;
            }

            if(SchemaConstants.isReplicatedSystemKeyspace(keyspace) && perm != CorePermission.ALTER && (perm != CorePermission.DROP || !Resources.isDroppable(resource))) {
               return false;
            }
         }

         if(perm == CorePermission.SELECT && Resources.isAlwaysReadable(resource)) {
            return true;
         }

         if(Resources.isProtected(resource) && this.isSchemaModification(perm)) {
            return false;
         }
      }

      return this.hasPermissionOnResourceChain(resource, perm);
   }

   public final boolean hasRolePermission(RoleResource resource, Permission perm) {
      return this.hasPermissionOnResourceChain(resource, perm);
   }

   public final boolean hasFunctionPermission(FunctionResource resource, Permission perm) {
      return resource.hasParent() && this.isNativeFunction(resource)?true:this.hasPermissionOnResourceChain(resource, perm);
   }

   public abstract boolean hasJMXPermission(MBeanServer var1, ObjectName var2, Permission var3);

   public final boolean hasPermission(IResource resource, Permission perm) {
      return resource instanceof DataResource?this.hasDataPermission((DataResource)resource, perm):(resource instanceof FunctionResource?this.hasFunctionPermission((FunctionResource)resource, perm):this.hasPermissionOnResourceChain(resource, perm));
   }

   public abstract boolean hasGrantPermission(IResource var1, Permission var2);

   public final void checkDataPermission(DataResource resource, Permission perm) {
      if(!DataResource.root().equals(resource)) {
         this.preventSystemKSSchemaModification(resource, perm);
         if(perm == CorePermission.SELECT && Resources.isAlwaysReadable(resource)) {
            return;
         }

         if(Resources.isProtected(resource) && this.isSchemaModification(perm)) {
            throw new UnauthorizedException(String.format("%s schema is protected", new Object[]{resource}));
         }
      }

      this.checkPermissionOnResourceChain(resource, perm);
   }

   private void preventSystemKSSchemaModification(DataResource resource, Permission perm) {
      String keyspace = resource.getKeyspace();
      validateKeyspace(keyspace);
      if(this.isSchemaModification(perm)) {
         if(SchemaConstants.isLocalSystemKeyspace(keyspace)) {
            throw new UnauthorizedException(keyspace + " keyspace is not user-modifiable.");
         } else if(SchemaConstants.isReplicatedSystemKeyspace(keyspace) && perm != CorePermission.ALTER && (perm != CorePermission.DROP || !Resources.isDroppable(resource))) {
            throw new UnauthorizedException(String.format("Cannot %s %s", new Object[]{perm, resource}));
         }
      }
   }

   private boolean isSchemaModification(Permission perm) {
      return perm == CorePermission.CREATE || perm == CorePermission.ALTER || perm == CorePermission.DROP;
   }

   public final void checkAllKeyspacesPermission(Permission perm) {
      this.checkDataPermission(DataResource.root(), perm);
   }

   public final void checkKeyspacePermission(String keyspace, Permission perm) {
      validateKeyspace(keyspace);
      this.checkDataPermission(DataResource.keyspace(keyspace), perm);
   }

   protected static void validateKeyspace(String keyspace) {
      if(keyspace == null) {
         throw new InvalidRequestException("You have not set a keyspace for this session");
      }
   }

   public final void checkTablePermission(String keyspace, String table, Permission perm) {
      Schema.instance.validateTable(keyspace, table);
      this.checkDataPermission(DataResource.table(keyspace, table), perm);
   }

   public final void checkTablePermission(TableMetadataRef tableRef, Permission perm) {
      this.checkTablePermission(tableRef.get(), perm);
   }

   public final void checkTablePermission(TableMetadata table, Permission perm) {
      this.checkDataPermission(table.resource, perm);
   }

   public final void checkFunctionPermission(org.apache.cassandra.cql3.functions.Function function, Permission perm) {
      if(!function.isNative()) {
         this.checkPermissionOnResourceChain(FunctionResource.function(function.name().keyspace, function.name().name, function.argTypes()), perm);
      }
   }

   public final void checkFunctionPermission(FunctionResource resource, Permission perm) {
      if(!resource.hasParent() || !this.isNativeFunction(resource)) {
         this.checkPermissionOnResourceChain(resource, perm);
      }
   }

   private boolean isNativeFunction(FunctionResource resource) {
      return resource.getKeyspace().equals("system");
   }

   public final void checkPermission(IResource resource, Permission perm) {
      if(resource instanceof DataResource) {
         this.checkDataPermission((DataResource)resource, perm);
      } else if(resource instanceof FunctionResource) {
         this.checkFunctionPermission((FunctionResource)resource, perm);
      } else {
         this.checkPermissionOnResourceChain(resource, perm);
      }

   }

   protected abstract void checkPermissionOnResourceChain(IResource var1, Permission var2);

   protected abstract boolean hasPermissionOnResourceChain(IResource var1, Permission var2);

   public abstract void additionalQueryPermission(IResource var1, PermissionSets var2);

   public final boolean hasRole(RoleResource role) {
      return this.roles.contains(role);
   }

   public final Set<RoleResource> getRoles() {
      return this.roles;
   }

   public <R> R filterPermissions(Function<R, R> applicablePermissions, Supplier<R> initialState, UserRolesAndPermissions.RoleResourcePermissionFilter<R> aggregate) {
      R state = initialState.get();
      state = applicablePermissions.apply(state);
      return state;
   }

   private static final class NormalUserRoles extends UserRolesAndPermissions {
      public NormalUserRoles(String name, String authenticatedName, Set<RoleResource> roles) {
         super(name, authenticatedName, roles, null);
      }

      public boolean hasGrantPermission(IResource resource, Permission perm) {
         return true;
      }

      protected void checkPermissionOnResourceChain(IResource resource, Permission perm) {
      }

      protected boolean hasPermissionOnResourceChain(IResource resource, Permission perm) {
         return true;
      }

      public boolean hasJMXPermission(MBeanServer mbs, ObjectName object, Permission permission) {
         return true;
      }

      public void additionalQueryPermission(IResource resource, PermissionSets permissionSets) {
         throw new UnsupportedOperationException();
      }
   }

   private static final class NormalUserWithPermissions extends UserRolesAndPermissions {
      private Map<RoleResource, Map<IResource, PermissionSets>> permissions;
      private Map<IResource, PermissionSets> additionalPermissions;

      public NormalUserWithPermissions(String name, String authenticatedName, Set<RoleResource> roles, Map<RoleResource, Map<IResource, PermissionSets>> permissions) {
         super(name, authenticatedName, roles, null);
         this.permissions = permissions;
      }

      public boolean hasGrantPermission(IResource resource, Permission perm) {
         Iterator var3 = this.getAllPermissionSetsFor(resource).iterator();

         PermissionSets permissions;
         do {
            if(!var3.hasNext()) {
               return false;
            }

            permissions = (PermissionSets)var3.next();
         } while(!permissions.grantables.contains(perm));

         return true;
      }

      protected void checkPermissionOnResourceChain(IResource resource, Permission perm) {
         IAuthorizer.TransitionalMode mode = DatabaseDescriptor.getAuthorizer().getTransitionalMode();
         if(!mode.supportPermission(perm)) {
            throw new UnauthorizedException(String.format("User %s has no %s permission on %s or any of its parents", new Object[]{this.getName(), perm, resource}));
         } else if(mode.enforcePermissionsOnAuthenticatedUser()) {
            boolean granted = false;
            Iterator var5 = this.getAllPermissionSetsFor(resource).iterator();

            PermissionSets permissions;
            do {
               if(!var5.hasNext()) {
                  if(!granted) {
                     throw new UnauthorizedException(String.format("User %s has no %s permission on %s or any of its parents", new Object[]{this.getName(), perm, resource}));
                  }

                  return;
               }

               permissions = (PermissionSets)var5.next();
               granted |= permissions.granted.contains(perm);
            } while(!permissions.restricted.contains(perm));

            throw new UnauthorizedException(String.format("Access for user %s on %s or any of its parents with %s permission is restricted", new Object[]{this.getName(), resource, perm}));
         }
      }

      private List<PermissionSets> getAllPermissionSetsFor(IResource resource) {
         List<? extends IResource> chain = Resources.chain(resource);
         List<PermissionSets> list = new ArrayList(chain.size() * (super.roles.size() + 1));
         Iterator var4 = super.roles.iterator();

         while(var4.hasNext()) {
            RoleResource roleResource = (RoleResource)var4.next();
            Iterator var6 = chain.iterator();

            while(var6.hasNext()) {
               IResource res = (IResource)var6.next();
               PermissionSets permissions = this.getPermissions(roleResource, res);
               if(permissions != null) {
                  list.add(permissions);
               }
            }
         }

         if(this.additionalPermissions != null) {
            var4 = chain.iterator();

            while(var4.hasNext()) {
               IResource res = (IResource)var4.next();
               PermissionSets permissions = (PermissionSets)this.additionalPermissions.get(res);
               if(permissions != null) {
                  list.add(permissions);
               }
            }
         }

         return list;
      }

      protected boolean hasPermissionOnResourceChain(IResource resource, Permission perm) {
         IAuthorizer.TransitionalMode mode = DatabaseDescriptor.getAuthorizer().getTransitionalMode();
         if(!mode.supportPermission(perm)) {
            return false;
         } else if(!mode.enforcePermissionsOnAuthenticatedUser()) {
            return true;
         } else {
            boolean granted = false;
            Iterator var5 = this.getAllPermissionSetsFor(resource).iterator();

            PermissionSets permissions;
            do {
               if(!var5.hasNext()) {
                  return granted;
               }

               permissions = (PermissionSets)var5.next();
               granted |= permissions.granted.contains(perm);
            } while(!permissions.restricted.contains(perm));

            return false;
         }
      }

      private PermissionSets getPermissions(RoleResource roleResource, IResource res) {
         Map<IResource, PermissionSets> map = (Map)this.permissions.get(roleResource);
         return map == null?null:(PermissionSets)map.get(res);
      }

      public boolean hasJMXPermission(MBeanServer mbs, ObjectName object, Permission permission) {
         IAuthorizer.TransitionalMode mode = DatabaseDescriptor.getAuthorizer().getTransitionalMode();
         if(!mode.supportPermission(permission)) {
            return false;
         } else if(!mode.enforcePermissionsOnAuthenticatedUser()) {
            return true;
         } else {
            Iterator var5 = this.permissions.values().iterator();

            while(var5.hasNext()) {
               Map<IResource, PermissionSets> permissionsPerResource = (Map)var5.next();
               PermissionSets rootPerms = (PermissionSets)permissionsPerResource.get(JMXResource.root());
               if(rootPerms != null) {
                  if(rootPerms.restricted.contains(permission)) {
                     return false;
                  }

                  if(rootPerms.granted.contains(permission)) {
                     return true;
                  }
               }
            }

            Set<JMXResource> permittedResources = this.collectPermittedJmxResources(permission);
            if(permittedResources.isEmpty()) {
               return false;
            } else {
               return object.isPattern()?this.checkPattern(mbs, object, permittedResources):this.checkExact(mbs, object, permittedResources);
            }
         }
      }

      private boolean checkPattern(MBeanServer mbs, ObjectName target, Set<JMXResource> permittedResources) {
         Set<ObjectName> targetNames = mbs.queryNames(target, (QueryExp)null);
         Iterator var5 = permittedResources.iterator();

         while(var5.hasNext()) {
            JMXResource resource = (JMXResource)var5.next();

            try {
               Set<ObjectName> matchingNames = mbs.queryNames(ObjectName.getInstance(resource.getObjectName()), (QueryExp)null);
               targetNames.removeAll(matchingNames);
               if(targetNames.isEmpty()) {
                  return true;
               }
            } catch (MalformedObjectNameException var8) {
               UserRolesAndPermissions.logger.warn("Permissions for JMX resource contains invalid ObjectName {}", resource.getObjectName());
            }
         }

         return false;
      }

      private boolean checkExact(MBeanServer mbs, ObjectName target, Set<JMXResource> permittedResources) {
         Iterator var4 = permittedResources.iterator();

         while(var4.hasNext()) {
            JMXResource resource = (JMXResource)var4.next();

            try {
               if(ObjectName.getInstance(resource.getObjectName()).apply(target)) {
                  return true;
               }
            } catch (MalformedObjectNameException var7) {
               UserRolesAndPermissions.logger.warn("Permissions for JMX resource contains invalid ObjectName {}", resource.getObjectName());
            }
         }

         UserRolesAndPermissions.logger.trace("Subject does not have sufficient permissions on target MBean {}", target);
         return false;
      }

      private Set<JMXResource> collectPermittedJmxResources(Permission permission) {
         Set<JMXResource> permittedResources = new HashSet();
         Iterator var3 = this.permissions.values().iterator();

         while(var3.hasNext()) {
            Map<IResource, PermissionSets> permissionsPerResource = (Map)var3.next();
            Iterator var5 = permissionsPerResource.entrySet().iterator();

            while(var5.hasNext()) {
               Entry<IResource, PermissionSets> resourcePermissionSets = (Entry)var5.next();
               if(resourcePermissionSets.getKey() instanceof JMXResource && ((PermissionSets)resourcePermissionSets.getValue()).hasEffectivePermission(permission)) {
                  permittedResources.add((JMXResource)resourcePermissionSets.getKey());
               }
            }
         }

         return permittedResources;
      }

      public void additionalQueryPermission(IResource resource, PermissionSets permissionSets) {
         if(this.additionalPermissions == null) {
            this.additionalPermissions = new HashMap();
         }

         PermissionSets previous = (PermissionSets)this.additionalPermissions.putIfAbsent(resource, permissionSets);

         assert previous == null;

      }

      public <R> R filterPermissions(Function<R, R> applicablePermissions, Supplier<R> initialState, UserRolesAndPermissions.RoleResourcePermissionFilter<R> aggregate) {
         R state = initialState.get();
         Iterator var5 = this.getRoles().iterator();

         while(true) {
            RoleResource roleResource;
            Map rolePerms;
            do {
               if(!var5.hasNext()) {
                  return state;
               }

               roleResource = (RoleResource)var5.next();
               rolePerms = (Map)this.permissions.get(roleResource);
            } while(rolePerms == null);

            Entry resourcePerms;
            for(Iterator var8 = rolePerms.entrySet().iterator(); var8.hasNext(); state = aggregate.apply(state, roleResource, (IResource)resourcePerms.getKey(), (PermissionSets)resourcePerms.getValue())) {
               resourcePerms = (Entry)var8.next();
            }
         }
      }
   }

   private static final class SuperUserRoleAndPermissions extends UserRolesAndPermissions {
      public SuperUserRoleAndPermissions(String name, String authenticatedName, Set<RoleResource> roles) {
         super(name, authenticatedName, roles, null);
      }

      public boolean isSuper() {
         return true;
      }

      public boolean hasGrantPermission(IResource resource, Permission perm) {
         return true;
      }

      protected void checkPermissionOnResourceChain(IResource resource, Permission perm) {
      }

      protected boolean hasPermissionOnResourceChain(IResource resource, Permission perm) {
         return true;
      }

      public boolean hasJMXPermission(MBeanServer mbs, ObjectName object, Permission permission) {
         return true;
      }

      public void additionalQueryPermission(IResource resource, PermissionSets permissionSets) {
      }
   }

   @FunctionalInterface
   public interface RoleResourcePermissionFilter<R> {
      R apply(R var1, RoleResource var2, IResource var3, PermissionSets var4);
   }
}
