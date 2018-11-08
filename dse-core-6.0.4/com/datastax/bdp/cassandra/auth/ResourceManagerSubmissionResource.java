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
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.commons.lang3.StringUtils;

public class ResourceManagerSubmissionResource implements IResource {
   @Inject
   @Named("workPoolVerifier")
   private final Provider<Function<String, Boolean>> workPoolVerifier = null;
   @Inject
   private final Provider<ResourceManagerSubmissionResource.SubmissionVerifier> submissionVerifier = null;
   private final Supplier<Void> ensureInitialized = Suppliers.memoize(() -> {
      DseInjector.get().injectMembers(this);
      return null;
   });
   public static final String PRE_6_ROOT_NAME = "any_submission";
   public static final String ROOT_NAME = "any_submission_in_any_workpool_in_any_dc";
   public static final ResourceManagerSubmissionResource ROOT = new ResourceManagerSubmissionResource();
   public static final Set<Permission> DEFAULT_PERMISSIONS;
   private final ResourceManagerSubmissionResource.Level level;
   private final String dc;
   private final String workPool;
   private final String submission;

   private ResourceManagerSubmissionResource() {
      this.level = ResourceManagerSubmissionResource.Level.ANY_SUBMISSION;
      this.dc = null;
      this.workPool = null;
      this.submission = null;
   }

   private ResourceManagerSubmissionResource(String dc, String workPool, String submission, ResourceManagerSubmissionResource.Level level) {
      this.level = level;
      this.dc = dc;
      this.workPool = workPool;
      this.submission = submission;
   }

   public static ResourceManagerSubmissionResource root() {
      return ROOT;
   }

   public static ResourceManagerSubmissionResource dc(String dc) {
      return new ResourceManagerSubmissionResource(dc, (String)null, (String)null, ResourceManagerSubmissionResource.Level.ANY_SUBMISSION_IN_DC);
   }

   public static ResourceManagerSubmissionResource workPool(String dc, String workPool) {
      return new ResourceManagerSubmissionResource(dc, workPool, (String)null, ResourceManagerSubmissionResource.Level.ANY_SUBMISSION_IN_WORKPOOL);
   }

   public static ResourceManagerSubmissionResource submission(String dc, String workPool, String submission) {
      return new ResourceManagerSubmissionResource(dc, workPool, submission, ResourceManagerSubmissionResource.Level.SUBMISSION_IN_WORKPOOL);
   }

   public static ResourceManagerSubmissionResource workPoolFromSpec(String spec) {
      String dc = ResourceManagerWorkPoolResource.getDC(spec);
      String workPool = ResourceManagerWorkPoolResource.getWorkPool(spec);
      if(workPool == null) {
         throw new IllegalArgumentException("Invalid workpool");
      } else {
         return Objects.equal(workPool, "*")?dc(dc):workPool(dc, workPool);
      }
   }

   public static ResourceManagerSubmissionResource submissionFromSpec(String workPoolSpec, String submission) {
      String dc = ResourceManagerWorkPoolResource.getDC(workPoolSpec);
      String workPool = ResourceManagerWorkPoolResource.getWorkPool(workPoolSpec);
      if(workPool != null && !Objects.equal(workPool, "*")) {
         return submission(dc, workPool, submission);
      } else {
         throw new IllegalArgumentException("Invalid workpool");
      }
   }

   public String getName() {
      switch(null.$SwitchMap$com$datastax$bdp$cassandra$auth$ResourceManagerSubmissionResource$Level[this.level.ordinal()]) {
      case 1:
         return "any_submission_in_any_workpool_in_any_dc";
      case 2:
         return String.format("%s/%s", new Object[]{"any_submission_in_any_workpool_in_any_dc", this.dc});
      case 3:
         return String.format("%s/%s/%s", new Object[]{"any_submission_in_any_workpool_in_any_dc", this.dc, this.workPool});
      case 4:
         return String.format("%s/%s/%s/%s", new Object[]{"any_submission_in_any_workpool_in_any_dc", this.dc, this.workPool, this.submission});
      default:
         throw new AssertionError();
      }
   }

   public IResource getParent() {
      switch(null.$SwitchMap$com$datastax$bdp$cassandra$auth$ResourceManagerSubmissionResource$Level[this.level.ordinal()]) {
      case 2:
         return root();
      case 3:
         return dc(this.dc);
      case 4:
         return workPool(this.dc, this.workPool);
      default:
         throw new IllegalStateException("Root-level resource can't have a parent");
      }
   }

   public boolean hasParent() {
      return this.level != ResourceManagerSubmissionResource.Level.ANY_SUBMISSION;
   }

   public boolean exists() {
      switch(null.$SwitchMap$com$datastax$bdp$cassandra$auth$ResourceManagerSubmissionResource$Level[this.level.ordinal()]) {
      case 1:
         return true;
      case 2:
         this.ensureInitialized.get();
         return ((Boolean)((Function)this.workPoolVerifier.get()).apply(this.dc)).booleanValue();
      case 3:
         this.ensureInitialized.get();
         return ((Boolean)((Function)this.workPoolVerifier.get()).apply(ResourceManagerWorkPoolResource.getSpec(this.dc, this.workPool))).booleanValue();
      case 4:
         this.ensureInitialized.get();
         return ((ResourceManagerSubmissionResource.SubmissionVerifier)this.submissionVerifier.get()).verifySubmission(this.dc, this.workPool, this.submission).booleanValue();
      default:
         throw new AssertionError();
      }
   }

   public Set<Permission> applicablePermissions() {
      switch(null.$SwitchMap$com$datastax$bdp$cassandra$auth$ResourceManagerSubmissionResource$Level[this.level.ordinal()]) {
      case 1:
         return DEFAULT_PERMISSIONS;
      case 2:
         return DEFAULT_PERMISSIONS;
      case 3:
         return DEFAULT_PERMISSIONS;
      case 4:
         return DEFAULT_PERMISSIONS;
      default:
         throw new AssertionError();
      }
   }

   public String toString() {
      switch(null.$SwitchMap$com$datastax$bdp$cassandra$auth$ResourceManagerSubmissionResource$Level[this.level.ordinal()]) {
      case 1:
         return "<any submission in any work pool>";
      case 2:
         return String.format("<any submission in any work pool in %s>", new Object[]{this.dc});
      case 3:
         return String.format("<any submission in work pool %s in %s>", new Object[]{this.workPool, this.dc});
      case 4:
         return String.format("<submission %s in work pool %s in %s>", new Object[]{this.submission, this.workPool, this.dc});
      default:
         throw new AssertionError();
      }
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof ResourceManagerSubmissionResource)) {
         return false;
      } else {
         ResourceManagerSubmissionResource r = (ResourceManagerSubmissionResource)o;
         return Objects.equal(this.level, r.level) && Objects.equal(this.dc, r.dc) && Objects.equal(this.workPool, r.workPool) && Objects.equal(this.submission, r.submission);
      }
   }

   public int hashCode() {
      return Objects.hashCode(new Object[]{this.level, this.dc, this.workPool, this.submission});
   }

   static {
      DEFAULT_PERMISSIONS = Collections.unmodifiableSet(Sets.newHashSet(new CorePermission[]{CorePermission.MODIFY, CorePermission.DESCRIBE, CorePermission.AUTHORIZE}));
   }

   public interface SubmissionVerifier {
      Boolean verifySubmission(String var1, String var2, String var3);

      Boolean checkAppExists(String var1, String var2, String var3, ConsistencyLevel var4);

      Boolean checkDriverExists(String var1, String var2, String var3, ConsistencyLevel var4);
   }

   public static class Factory implements DseResourceFactory.Factory {
      public Factory() {
      }

      public boolean matches(String name) {
         return name.startsWith("any_submission_in_any_workpool_in_any_dc") || name.startsWith("any_submission");
      }

      public ResourceManagerSubmissionResource fromName(String name) {
         String[] parts = StringUtils.split(name, '/');
         Preconditions.checkArgument(parts.length > 0 && (Objects.equal(parts[0], "any_submission_in_any_workpool_in_any_dc") && parts.length <= 4 || Objects.equal(parts[0], "any_submission") && parts.length <= 3), "{} is not a valid work pool name", new Object[]{name});
         return parts.length == 1?ResourceManagerSubmissionResource.ROOT:(parts.length == 2?(Objects.equal(parts[0], "any_submission")?ResourceManagerSubmissionResource.workPool(parts[1], "default"):ResourceManagerSubmissionResource.dc(parts[1])):(parts.length == 3?(Objects.equal(parts[0], "any_submission")?ResourceManagerSubmissionResource.submission(parts[1], "default", parts[2]):ResourceManagerSubmissionResource.workPool(parts[1], parts[2])):ResourceManagerSubmissionResource.submission(parts[1], parts[2], parts[3])));
      }
   }

   static enum Level {
      ANY_SUBMISSION,
      ANY_SUBMISSION_IN_DC,
      ANY_SUBMISSION_IN_WORKPOOL,
      SUBMISSION_IN_WORKPOOL;

      private Level() {
      }
   }
}
