package org.apache.cassandra.auth.jmx;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.security.AccessControlContext;
import java.security.AccessController;
import java.security.Principal;
import java.util.Set;
import java.util.function.BooleanSupplier;
import java.util.function.Function;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.security.auth.Subject;
import org.apache.cassandra.auth.IAuthorizer;
import org.apache.cassandra.auth.JMXResource;
import org.apache.cassandra.auth.Permission;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.auth.user.UserRolesAndPermissions;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.StorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AuthorizationProxy implements InvocationHandler {
   private static final Logger logger = LoggerFactory.getLogger(AuthorizationProxy.class);
   private static final Set<String> MBEAN_SERVER_METHOD_WHITELIST = ImmutableSet.of("getDefaultDomain", "getDomains", "getMBeanCount", "hashCode", "queryMBeans", "queryNames", new String[]{"toString"});
   private static final Set<String> METHOD_BLACKLIST = ImmutableSet.of("createMBean", "deserialize", "getClassLoader", "getClassLoaderFor", "instantiate", "registerMBean", new String[]{"unregisterMBean"});
   private MBeanServer mbs;
   protected Function<String, UserRolesAndPermissions> userRolesAndPermissions = (name) -> {
      return name == null?UserRolesAndPermissions.SYSTEM:(UserRolesAndPermissions)DatabaseDescriptor.getAuthManager().getUserRolesAndPermissions(name, name).blockingGet();
   };
   protected BooleanSupplier isAuthzRequired;
   protected BooleanSupplier isAuthSetupComplete;

   public AuthorizationProxy() {
      IAuthorizer var10001 = DatabaseDescriptor.getAuthorizer();
      this.isAuthzRequired = var10001::requireAuthorization;
      StorageService var1 = StorageService.instance;
      StorageService.instance.getClass();
      this.isAuthSetupComplete = var1::isAuthSetupComplete;
   }

   public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
      String methodName = method.getName();
      if("getMBeanServer".equals(methodName)) {
         throw new SecurityException("Access denied");
      } else {
         AccessControlContext acc = AccessController.getContext();
         Subject subject = Subject.getSubject(acc);
         if("setMBeanServer".equals(methodName)) {
            if(subject != null) {
               throw new SecurityException("Access denied");
            } else if(args[0] == null) {
               throw new IllegalArgumentException("Null MBeanServer");
            } else if(this.mbs != null) {
               throw new IllegalArgumentException("MBeanServer already initialized");
            } else {
               this.mbs = (MBeanServer)args[0];
               return null;
            }
         } else if(this.authorize(subject, methodName, args)) {
            return this.invoke(method, args);
         } else {
            throw new SecurityException("Access Denied");
         }
      }
   }

   @VisibleForTesting
   boolean authorize(Subject subject, String methodName, Object[] args) {
      logger.trace("Authorizing JMX method invocation {} for {}", methodName, subject == null?"":subject.toString().replaceAll("\\n", " "));
      if(!this.isAuthSetupComplete.getAsBoolean()) {
         logger.trace("Auth setup is not complete, refusing access");
         return false;
      } else if(!this.isAuthzRequired.getAsBoolean()) {
         return true;
      } else if(subject == null) {
         return true;
      } else if(METHOD_BLACKLIST.contains(methodName)) {
         logger.trace("Access denied to blacklisted method {}", methodName);
         return false;
      } else {
         Set<Principal> principals = subject.getPrincipals();
         if(principals != null && !principals.isEmpty()) {
            String name = ((Principal)principals.iterator().next()).getName();
            UserRolesAndPermissions user = (UserRolesAndPermissions)this.userRolesAndPermissions.apply(name);
            return user.isSuper()?true:(args != null && args[0] instanceof ObjectName?this.authorizeMBeanMethod(user, methodName, args):this.authorizeMBeanServerMethod(user, methodName));
         } else {
            return false;
         }
      }
   }

   private boolean authorizeMBeanServerMethod(UserRolesAndPermissions user, String methodName) {
      logger.trace("JMX invocation of {} on MBeanServer requires permission {}", methodName, CorePermission.DESCRIBE);
      return MBEAN_SERVER_METHOD_WHITELIST.contains(methodName) && user.hasPermission(JMXResource.root(), CorePermission.DESCRIBE);
   }

   private boolean authorizeMBeanMethod(UserRolesAndPermissions user, String methodName, Object[] args) {
      ObjectName targetBean = (ObjectName)args[0];
      Permission requiredPermission = getRequiredPermission(methodName);
      if(null == requiredPermission) {
         return false;
      } else {
         logger.trace("JMX invocation of {} on {} requires permission {}", new Object[]{methodName, targetBean, requiredPermission});
         boolean hasJmxPermission = user.hasJMXPermission(this.mbs, targetBean, requiredPermission);
         if(!hasJmxPermission) {
            logger.trace("Subject does not have sufficient permissions on target MBean {}", targetBean);
         }

         return hasJmxPermission;
      }
   }

   private static Permission getRequiredPermission(String methodName) {
      byte var2 = -1;
      switch(methodName.hashCode()) {
      case -1479410370:
         if(methodName.equals("queryMBeans")) {
            var2 = 10;
         }
         break;
      case -1221670378:
         if(methodName.equals("isInstanceOf")) {
            var2 = 8;
         }
         break;
      case -1183693704:
         if(methodName.equals("invoke")) {
            var2 = 4;
         }
         break;
      case -184415392:
         if(methodName.equals("queryNames")) {
            var2 = 11;
         }
         break;
      case 3480550:
         if(methodName.equals("getAttribute")) {
            var2 = 0;
         }
         break;
      case 107897165:
         if(methodName.equals("getAttributes")) {
            var2 = 1;
         }
         break;
      case 147696667:
         if(methodName.equals("hashCode")) {
            var2 = 7;
         }
         break;
      case 301341516:
         if(methodName.equals("isRegistered")) {
            var2 = 9;
         }
         break;
      case 604276034:
         if(methodName.equals("getInstanceOf")) {
            var2 = 5;
         }
         break;
      case 882028377:
         if(methodName.equals("setAttributes")) {
            var2 = 3;
         }
         break;
      case 1552473178:
         if(methodName.equals("setAttribute")) {
            var2 = 2;
         }
         break;
      case 1932176725:
         if(methodName.equals("getMBeanInfo")) {
            var2 = 6;
         }
      }

      switch(var2) {
      case 0:
      case 1:
         return CorePermission.SELECT;
      case 2:
      case 3:
         return CorePermission.MODIFY;
      case 4:
         return CorePermission.EXECUTE;
      case 5:
      case 6:
      case 7:
      case 8:
      case 9:
      case 10:
      case 11:
         return CorePermission.DESCRIBE;
      default:
         logger.debug("Access denied, method name {} does not map to any defined permission", methodName);
         return null;
      }
   }

   private Object invoke(Method method, Object[] args) throws Throwable {
      try {
         return method.invoke(this.mbs, args);
      } catch (InvocationTargetException var5) {
         Throwable t = var5.getCause();
         throw t;
      }
   }
}
