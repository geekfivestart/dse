package com.datastax.bdp.util.rpc;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.cassandra.auth.Permission;

public class RpcRegistry {
   protected static final ConcurrentHashMap<String, RpcObject> objects = new ConcurrentHashMap();

   public RpcRegistry() {
   }

   public static void register(Object o) {
      register(o.getClass().getSimpleName(), o);
   }

   public static void register(String name, Object o) {
      if(objects.putIfAbsent(name, new RpcObject(name, o)) != null) {
         throw new AssertionError("Multiple assignments to " + name + "!");
      }
   }

   public static boolean unregister(String name) {
      return null != objects.remove(name);
   }

   public static Optional<RpcMethod> lookupMethod(String object, String method) {
      return objects.containsKey(object)?((RpcObject)objects.get(object)).lookupMethod(method):Optional.empty();
   }

   public static boolean objectExists(String object) {
      return objects.containsKey(object);
   }

   public static boolean methodExists(String object, String method) {
      return lookupMethod(object, method).isPresent();
   }

   public static Set<Permission> getAllPermissions() {
      Set<Permission> permissions = new HashSet();
      Iterator var1 = objects.values().iterator();

      while(var1.hasNext()) {
         RpcObject rpcObject = (RpcObject)var1.next();
         Iterator var3 = rpcObject.getMethods().iterator();

         while(var3.hasNext()) {
            RpcMethod rpcMethod = (RpcMethod)var3.next();
            permissions.add(rpcMethod.getPermission());
         }
      }

      return permissions;
   }

   public static Set<Permission> getObjectPermissions(String object) {
      Set<Permission> permissions = new HashSet();
      if(objects.containsKey(object)) {
         RpcObject rpcObject = (RpcObject)objects.get(object);
         Iterator var3 = rpcObject.getMethods().iterator();

         while(var3.hasNext()) {
            RpcMethod rpcMethod = (RpcMethod)var3.next();
            permissions.add(rpcMethod.getPermission());
         }
      }

      return permissions;
   }

   public static Set<Permission> getMethodPermissions(String object, String method) {
      Set<Permission> permissions = new HashSet();
      if(objects.containsKey(object)) {
         RpcMethod rpcMethod = ((RpcObject)objects.get(object)).getMethod(method);
         if(rpcMethod != null) {
            permissions.add(rpcMethod.getPermission());
         }
      }

      return permissions;
   }
}
