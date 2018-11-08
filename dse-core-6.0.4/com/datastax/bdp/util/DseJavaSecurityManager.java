package com.datastax.bdp.util;

import java.security.AccessControlException;
import java.security.Permission;
import java.util.Optional;
import java.util.function.BiFunction;

public class DseJavaSecurityManager extends SecurityManager {
   private InheritableThreadLocal<BiFunction<Permission, Object, Boolean>> sentinel = new InheritableThreadLocal();
   private final Optional<SecurityManager> parentSecurityManager;

   public DseJavaSecurityManager() {
      this.parentSecurityManager = Optional.empty();
   }

   public DseJavaSecurityManager(SecurityManager parent) {
      this.parentSecurityManager = Optional.ofNullable(parent);
   }

   private void verify(Permission perm, Object context) {
      BiFunction<Permission, Object, Boolean> _sentinel = (BiFunction)this.sentinel.get();
      if(_sentinel != null && !((Boolean)_sentinel.apply(perm, context)).booleanValue()) {
         throw new AccessControlException("Missing permission " + perm, perm);
      }
   }

   public void checkPermission(Permission perm) {
      this.verify(perm, (Object)null);
      if(this.parentSecurityManager.isPresent()) {
         ((SecurityManager)this.parentSecurityManager.get()).checkPermission(perm);
      }

   }

   public void checkPermission(Permission perm, Object context) {
      this.verify(perm, context);
      if(this.parentSecurityManager.isPresent()) {
         ((SecurityManager)this.parentSecurityManager.get()).checkPermission(perm, context);
      }

   }

   public static void setSentinel(BiFunction<Permission, Object, Boolean> sentinel) {
      SecurityManager sm = System.getSecurityManager();
      if(sm instanceof DseJavaSecurityManager) {
         ((DseJavaSecurityManager)sm).sentinel.set(sentinel);
      } else {
         throw new IllegalStateException("No DseJavaSecurityManager in use");
      }
   }

   public static void removeSentinel() {
      SecurityManager sm = System.getSecurityManager();
      if(sm instanceof DseJavaSecurityManager) {
         ((DseJavaSecurityManager)sm).sentinel.remove();
      } else {
         throw new IllegalStateException("No DseJavaSecurityManager in use");
      }
   }
}
