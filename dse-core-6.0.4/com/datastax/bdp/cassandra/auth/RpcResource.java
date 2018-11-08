package com.datastax.bdp.cassandra.auth;

import com.datastax.bdp.util.rpc.RpcRegistry;
import com.google.common.base.Objects;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import java.util.Set;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.auth.permission.Permissions;
import org.apache.commons.lang3.StringUtils;

public class RpcResource implements IResource {
   private static final String ROOT_NAME = "rpc";
   private static final RpcResource ROOT_RESOURCE = new RpcResource();
   private static final Set<Permission> DEFAULT_PERMISSIONS;
   private final RpcResource.Level level;
   private final String object;
   private final String method;

   @Inject
   private RpcResource() {
      this.level = RpcResource.Level.ROOT;
      this.object = null;
      this.method = null;
   }

   private RpcResource(String object) {
      this.level = RpcResource.Level.OBJECT;
      this.object = object;
      this.method = null;
   }

   private RpcResource(String object, String method) {
      this.level = RpcResource.Level.METHOD;
      this.object = object;
      this.method = method;
   }

   public static RpcResource root() {
      return ROOT_RESOURCE;
   }

   public static RpcResource object(String object) {
      return new RpcResource(object);
   }

   public static RpcResource method(String object, String method) {
      return new RpcResource(object, method);
   }

   public String getName() {
      switch(null.$SwitchMap$com$datastax$bdp$cassandra$auth$RpcResource$Level[this.level.ordinal()]) {
      case 1:
         return "rpc";
      case 2:
         return String.format("%s/%s", new Object[]{"rpc", this.object});
      case 3:
         return String.format("%s/%s/%s", new Object[]{"rpc", this.object, this.method});
      default:
         throw new AssertionError();
      }
   }

   public IResource getParent() {
      switch(null.$SwitchMap$com$datastax$bdp$cassandra$auth$RpcResource$Level[this.level.ordinal()]) {
      case 2:
         return root();
      case 3:
         return object(this.object);
      default:
         throw new IllegalStateException("Root-level resource can't have a parent");
      }
   }

   public boolean hasParent() {
      return this.level != RpcResource.Level.ROOT;
   }

   public boolean exists() {
      switch(null.$SwitchMap$com$datastax$bdp$cassandra$auth$RpcResource$Level[this.level.ordinal()]) {
      case 1:
         return true;
      case 2:
         return RpcRegistry.objectExists(this.object);
      case 3:
         return RpcRegistry.methodExists(this.object, this.method);
      default:
         throw new AssertionError();
      }
   }

   public Set<Permission> applicablePermissions() {
      switch(null.$SwitchMap$com$datastax$bdp$cassandra$auth$RpcResource$Level[this.level.ordinal()]) {
      case 1:
         return Sets.union(DEFAULT_PERMISSIONS, RpcRegistry.getAllPermissions());
      case 2:
         return Sets.union(DEFAULT_PERMISSIONS, RpcRegistry.getObjectPermissions(this.object));
      case 3:
         return Sets.union(DEFAULT_PERMISSIONS, RpcRegistry.getMethodPermissions(this.object, this.method));
      default:
         throw new AssertionError();
      }
   }

   public String toString() {
      switch(null.$SwitchMap$com$datastax$bdp$cassandra$auth$RpcResource$Level[this.level.ordinal()]) {
      case 1:
         return "<all rpc>";
      case 2:
         return String.format("<rpc object %s>", new Object[]{this.object});
      case 3:
         return String.format("<rpc method %s.%s>", new Object[]{this.object, this.method});
      default:
         throw new AssertionError();
      }
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof RpcResource)) {
         return false;
      } else {
         RpcResource r = (RpcResource)o;
         return Objects.equal(this.level, r.level) && Objects.equal(this.object, r.object) && Objects.equal(this.method, r.method);
      }
   }

   public int hashCode() {
      return Objects.hashCode(new Object[]{this.level, this.object, this.method});
   }

   static {
      DEFAULT_PERMISSIONS = Permissions.immutableSetOf(new Permission[]{CorePermission.AUTHORIZE});
   }

   public static class Factory implements DseResourceFactory.Factory {
      public Factory() {
      }

      public boolean matches(String name) {
         return name.startsWith("rpc");
      }

      public RpcResource fromName(String name) {
         String[] parts = StringUtils.split(name, '/');
         if(parts[0].equals("rpc") && parts.length <= 3) {
            return parts.length == 1?RpcResource.ROOT_RESOURCE:(parts.length == 2?RpcResource.object(parts[1]):RpcResource.method(parts[1], parts[2]));
         } else {
            throw new IllegalArgumentException(String.format("%s is not a valid rpc resource name", new Object[]{name}));
         }
      }
   }

   static enum Level {
      ROOT,
      OBJECT,
      METHOD;

      private Level() {
      }
   }
}
