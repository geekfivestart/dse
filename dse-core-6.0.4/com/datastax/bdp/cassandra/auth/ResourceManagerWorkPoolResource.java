package com.datastax.bdp.cassandra.auth;

import com.datastax.bdp.ioc.DseInjector;
import com.diffplug.common.base.Suppliers;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.name.Named;
import java.util.Collections;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.cassandra.auth.IResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.commons.lang3.StringUtils;

public class ResourceManagerWorkPoolResource implements IResource {
   public static final String DEFAULT_WORK_POOL = "default";
   @Inject
   @Named("workPoolVerifier")
   private final Provider<Function<String, Boolean>> workPoolVerifier = null;
   private final Supplier<Void> ensureInitialized = Suppliers.memoize(() -> {
      DseInjector.get().injectMembers(this);
      return null;
   });
   public static final String PRE_6_ROOT_NAME = "any_work_pool";
   public static final String ROOT_NAME = "any_work_pool_in_any_dc";
   public static final ResourceManagerWorkPoolResource ROOT = new ResourceManagerWorkPoolResource();
   public static final Set<Permission> DEFAULT_PERMISSIONS;
   private final ResourceManagerWorkPoolResource.Level level;
   private final String dc;
   private final String workPool;

   private ResourceManagerWorkPoolResource() {
      this.level = ResourceManagerWorkPoolResource.Level.ANY_WORK_POOL;
      this.dc = null;
      this.workPool = null;
   }

   private ResourceManagerWorkPoolResource(String dc) {
      Preconditions.checkArgument(!dc.contains("."), "DataCenter name cannot contain special characters like dots");
      this.level = ResourceManagerWorkPoolResource.Level.ANY_WORK_POOL_IN_DC;
      this.dc = dc;
      this.workPool = null;
   }

   private ResourceManagerWorkPoolResource(String dc, String workPool) {
      Preconditions.checkArgument(!dc.contains("."), "DataCenter name cannot contain special characters like dots");
      Preconditions.checkArgument(!workPool.contains("."), "Workpool name cannot contain special characters like dots");
      this.level = ResourceManagerWorkPoolResource.Level.WORK_POOL;
      this.dc = dc;
      this.workPool = workPool;
   }

   public static ResourceManagerWorkPoolResource root() {
      return ROOT;
   }

   public static ResourceManagerWorkPoolResource dc(String dc) {
      return new ResourceManagerWorkPoolResource(dc);
   }

   public static ResourceManagerWorkPoolResource workPool(String dc, String workPool) {
      return new ResourceManagerWorkPoolResource(dc, workPool);
   }

   public static ResourceManagerWorkPoolResource workPoolFromSpec(String spec) {
      String dc = getDC(spec);
      String workPool = getWorkPool(spec);
      if(workPool == null) {
         throw new IllegalArgumentException("Invalid workpool");
      } else {
         return Objects.equal(workPool, "*")?dc(dc):workPool(dc, workPool);
      }
   }

   public static String getDC(String spec) {
      String[] parts = spec.split("\\.");
      Preconditions.checkArgument(parts.length <= 2, "Invalid workpool specification");
      return parts[0];
   }

   public static String getWorkPool(String spec) {
      String[] parts = spec.split("\\.");
      Preconditions.checkArgument(parts.length <= 2, "Invalid workpool specification");
      return parts.length == 1?null:parts[1];
   }

   public static String getSpec(String dc, String workPool) {
      return String.format("%s.%s", new Object[]{dc, workPool});
   }

   public String getName() {
      switch(null.$SwitchMap$com$datastax$bdp$cassandra$auth$ResourceManagerWorkPoolResource$Level[this.level.ordinal()]) {
      case 1:
         return "any_work_pool_in_any_dc";
      case 2:
         return String.format("%s/%s", new Object[]{"any_work_pool_in_any_dc", this.dc});
      case 3:
         return String.format("%s/%s/%s", new Object[]{"any_work_pool_in_any_dc", this.dc, this.workPool});
      default:
         throw new AssertionError();
      }
   }

   public IResource getParent() {
      switch(null.$SwitchMap$com$datastax$bdp$cassandra$auth$ResourceManagerWorkPoolResource$Level[this.level.ordinal()]) {
      case 2:
         return root();
      case 3:
         return dc(this.dc);
      default:
         throw new IllegalStateException("Root-level resource can't have a parent");
      }
   }

   public boolean hasParent() {
      return this.level != ResourceManagerWorkPoolResource.Level.ANY_WORK_POOL;
   }

   public boolean exists() {
      switch(null.$SwitchMap$com$datastax$bdp$cassandra$auth$ResourceManagerWorkPoolResource$Level[this.level.ordinal()]) {
      case 1:
         return true;
      case 2:
         this.ensureInitialized.get();
         return ((Boolean)((Function)this.workPoolVerifier.get()).apply(this.dc)).booleanValue();
      case 3:
         this.ensureInitialized.get();
         return ((Boolean)((Function)this.workPoolVerifier.get()).apply(getSpec(this.dc, this.workPool))).booleanValue();
      default:
         throw new AssertionError();
      }
   }

   public Set<Permission> applicablePermissions() {
      switch(null.$SwitchMap$com$datastax$bdp$cassandra$auth$ResourceManagerWorkPoolResource$Level[this.level.ordinal()]) {
      case 1:
         return DEFAULT_PERMISSIONS;
      case 2:
         return DEFAULT_PERMISSIONS;
      case 3:
         return DEFAULT_PERMISSIONS;
      default:
         throw new AssertionError();
      }
   }

   public String toString() {
      switch(null.$SwitchMap$com$datastax$bdp$cassandra$auth$ResourceManagerWorkPoolResource$Level[this.level.ordinal()]) {
      case 1:
         return "<any work pool>";
      case 2:
         return String.format("<any work pool in %s>", new Object[]{this.dc});
      case 3:
         return String.format("<work pool %s in %s>", new Object[]{this.workPool, this.dc});
      default:
         throw new AssertionError();
      }
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof ResourceManagerWorkPoolResource)) {
         return false;
      } else {
         ResourceManagerWorkPoolResource r = (ResourceManagerWorkPoolResource)o;
         return Objects.equal(this.level, r.level) && Objects.equal(this.workPool, r.workPool) && Objects.equal(this.dc, r.dc);
      }
   }

   public int hashCode() {
      return Objects.hashCode(new Object[]{this.level, this.dc, this.workPool});
   }

   static {
      DEFAULT_PERMISSIONS = Collections.unmodifiableSet(Sets.newHashSet(new CorePermission[]{CorePermission.CREATE, CorePermission.DESCRIBE, CorePermission.AUTHORIZE}));
   }

   public static class Factory implements DseResourceFactory.Factory {
      public Factory() {
      }

      public boolean matches(String name) {
         return name.startsWith("any_work_pool_in_any_dc") || name.startsWith("any_work_pool");
      }

      public ResourceManagerWorkPoolResource fromName(String name) {
         String[] parts = StringUtils.split(name, '/');
         Preconditions.checkArgument(parts.length > 0 && (Objects.equal(parts[0], "any_work_pool_in_any_dc") && parts.length <= 3 || Objects.equal(parts[0], "any_work_pool") && parts.length <= 2), "{} is not a valid work pool name", new Object[]{name});
         return parts.length == 1?ResourceManagerWorkPoolResource.ROOT:(parts.length == 2?(Objects.equal(parts[0], "any_work_pool")?ResourceManagerWorkPoolResource.workPool(parts[1], "default"):ResourceManagerWorkPoolResource.dc(parts[1])):ResourceManagerWorkPoolResource.workPool(parts[1], parts[2]));
      }
   }

   static enum Level {
      ANY_WORK_POOL,
      ANY_WORK_POOL_IN_DC,
      WORK_POOL;

      private Level() {
      }
   }
}
