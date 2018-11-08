package org.apache.cassandra.cql3.functions;

import ch.qos.logback.classic.LoggerContext;
import ch.qos.logback.classic.spi.TurboFilterList;
import ch.qos.logback.classic.turbo.ReconfigureOnChangeFilter;
import ch.qos.logback.classic.turbo.TurboFilter;
import io.netty.util.concurrent.FastThreadLocal;
import java.security.AccessControlException;
import java.security.AllPermission;
import java.security.CodeSource;
import java.security.Permission;
import java.security.PermissionCollection;
import java.security.Permissions;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.Collections;
import java.util.Enumeration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ThreadAwareSecurityManager extends SecurityManager {
   static final PermissionCollection noPermissions = new PermissionCollection() {
      public void add(Permission permission) {
         throw new UnsupportedOperationException();
      }

      public boolean implies(Permission permission) {
         return false;
      }

      public Enumeration<Permission> elements() {
         return Collections.emptyEnumeration();
      }
   };
   private static final RuntimePermission CHECK_MEMBER_ACCESS_PERMISSION = new RuntimePermission("accessDeclaredMembers");
   private static final RuntimePermission MODIFY_THREAD_PERMISSION = new RuntimePermission("modifyThread");
   private static final RuntimePermission MODIFY_THREADGROUP_PERMISSION = new RuntimePermission("modifyThreadGroup");
   private static volatile boolean installed;
   private static final FastThreadLocal<Boolean> initializedThread;

   public static void install() {
      if(!installed) {
         System.setSecurityManager(new ThreadAwareSecurityManager());
         Logger l = LoggerFactory.getLogger(ThreadAwareSecurityManager.class);
         ch.qos.logback.classic.Logger logbackLogger = (ch.qos.logback.classic.Logger)l;
         LoggerContext ctx = logbackLogger.getLoggerContext();
         TurboFilterList turboFilterList = ctx.getTurboFilterList();

         for(int i = 0; i < turboFilterList.size(); ++i) {
            TurboFilter turboFilter = (TurboFilter)turboFilterList.get(i);
            if(turboFilter instanceof ReconfigureOnChangeFilter) {
               ReconfigureOnChangeFilter reconfigureOnChangeFilter = (ReconfigureOnChangeFilter)turboFilter;
               turboFilterList.set(i, new ThreadAwareSecurityManager.SMAwareReconfigureOnChangeFilter(reconfigureOnChangeFilter));
               break;
            }
         }

         installed = true;
      }
   }

   private ThreadAwareSecurityManager() {
   }

   private static boolean isSecuredThread() {
      ThreadGroup tg = Thread.currentThread().getThreadGroup();
      if(!(tg instanceof SecurityThreadGroup)) {
         return false;
      } else {
         Boolean threadInitialized = (Boolean)initializedThread.get();
         if(threadInitialized == null) {
            initializedThread.set(Boolean.valueOf(false));
            ((SecurityThreadGroup)tg).initializeThread();
            initializedThread.set(Boolean.valueOf(true));
            threadInitialized = Boolean.valueOf(true);
         }

         return threadInitialized.booleanValue();
      }
   }

   public void checkAccess(Thread t) {
      if(isSecuredThread()) {
         throw new AccessControlException("access denied: " + MODIFY_THREAD_PERMISSION, MODIFY_THREAD_PERMISSION);
      } else {
         super.checkAccess(t);
      }
   }

   public void checkAccess(ThreadGroup g) {
      if(isSecuredThread()) {
         throw new AccessControlException("access denied: " + MODIFY_THREADGROUP_PERMISSION, MODIFY_THREADGROUP_PERMISSION);
      } else {
         super.checkAccess(g);
      }
   }

   public void checkPermission(Permission perm) {
      if(isSecuredThread()) {
         if(!CHECK_MEMBER_ACCESS_PERMISSION.equals(perm)) {
            super.checkPermission(perm);
         }
      }
   }

   public void checkPermission(Permission perm, Object context) {
      if(isSecuredThread()) {
         super.checkPermission(perm, context);
      }

   }

   public void checkPackageAccess(String pkg) {
      if(isSecuredThread()) {
         if(!((SecurityThreadGroup)Thread.currentThread().getThreadGroup()).isPackageAllowed(pkg)) {
            RuntimePermission perm = new RuntimePermission("accessClassInPackage." + pkg);
            throw new AccessControlException("access denied: " + perm, perm);
         } else {
            super.checkPackageAccess(pkg);
         }
      }
   }

   static {
      Policy.setPolicy(new Policy() {
         public PermissionCollection getPermissions(CodeSource codesource) {
            Permissions perms = new Permissions();
            if(codesource != null && codesource.getLocation() != null) {
               String var3 = codesource.getLocation().getProtocol();
               byte var4 = -1;
               switch(var3.hashCode()) {
               case 3143036:
                  if(var3.equals("file")) {
                     var4 = 0;
                  }
               default:
                  switch(var4) {
                  case 0:
                     perms.add(new AllPermission());
                     return perms;
                  default:
                     return perms;
                  }
               }
            } else {
               return perms;
            }
         }

         public PermissionCollection getPermissions(ProtectionDomain domain) {
            return this.getPermissions(domain.getCodeSource());
         }

         public boolean implies(ProtectionDomain domain, Permission permission) {
            CodeSource codesource = domain.getCodeSource();
            if(codesource != null && codesource.getLocation() != null) {
               String var4 = codesource.getLocation().getProtocol();
               byte var5 = -1;
               switch(var4.hashCode()) {
               case 3143036:
                  if(var4.equals("file")) {
                     var5 = 0;
                  }
               default:
                  switch(var5) {
                  case 0:
                     return true;
                  default:
                     return false;
                  }
               }
            } else {
               return false;
            }
         }
      });
      initializedThread = new FastThreadLocal();
   }

   private static class SMAwareReconfigureOnChangeFilter extends ReconfigureOnChangeFilter {
      SMAwareReconfigureOnChangeFilter(ReconfigureOnChangeFilter reconfigureOnChangeFilter) {
         this.setRefreshPeriod(reconfigureOnChangeFilter.getRefreshPeriod());
         this.setName(reconfigureOnChangeFilter.getName());
         this.setContext(reconfigureOnChangeFilter.getContext());
         if(reconfigureOnChangeFilter.isStarted()) {
            reconfigureOnChangeFilter.stop();
            this.start();
         }

      }

      protected boolean changeDetected(long now) {
         return ThreadAwareSecurityManager.isSecuredThread()?false:super.changeDetected(now);
      }
   }
}
