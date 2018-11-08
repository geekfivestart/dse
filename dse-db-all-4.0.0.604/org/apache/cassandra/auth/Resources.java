package org.apache.cassandra.auth;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import org.apache.cassandra.auth.resource.IResourceFactory;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.utils.Hex;

public final class Resources {
   private static final Set<IResource> READABLE_SYSTEM_RESOURCES = new HashSet();
   private static final Set<IResource> PROTECTED_AUTH_RESOURCES = new HashSet();
   private static final Set<IResource> DROPPABLE_SYSTEM_TABLES = new HashSet();
   private static IResourceFactory factory;
   /** @deprecated */
   @Deprecated
   public static final String ROOT = "cassandra";
   /** @deprecated */
   @Deprecated
   public static final String KEYSPACES = "keyspaces";

   public Resources() {
   }

   public static void addReadableSystemResource(IResource resource) {
      READABLE_SYSTEM_RESOURCES.add(resource);
   }

   public static void setResourceFactory(IResourceFactory f) {
      factory = f;
   }

   public static List<? extends IResource> chain(IResource resource) {
      ArrayList chain = new ArrayList();

      while(true) {
         chain.add(resource);
         if(!resource.hasParent()) {
            return chain;
         }

         resource = resource.getParent();
      }
   }

   public static IResource fromName(String name) {
      return factory.fromName(name);
   }

   public static boolean isAlwaysReadable(IResource resource) {
      return READABLE_SYSTEM_RESOURCES.contains(resource);
   }

   public static boolean isProtected(IResource resource) {
      return PROTECTED_AUTH_RESOURCES.contains(resource);
   }

   public static boolean isDroppable(IResource resource) {
      return DROPPABLE_SYSTEM_TABLES.contains(resource);
   }

   /** @deprecated */
   @Deprecated
   public static String toString(List<Object> resource) {
      StringBuilder buff = new StringBuilder();
      Iterator var2 = resource.iterator();

      while(var2.hasNext()) {
         Object component = var2.next();
         buff.append("/");
         if(component instanceof byte[]) {
            buff.append(Hex.bytesToHex((byte[])((byte[])component)));
         } else {
            buff.append(component);
         }
      }

      return buff.toString();
   }

   static {
      Iterator var0 = SystemKeyspace.readableSystemResources().iterator();

      while(var0.hasNext()) {
         String cf = (String)var0.next();
         READABLE_SYSTEM_RESOURCES.add(DataResource.table("system", cf));
      }

      SchemaKeyspace.ALL.forEach((table) -> {
         READABLE_SYSTEM_RESOURCES.add(DataResource.table("system_schema", table));
      });
      if(DatabaseDescriptor.isDaemonInitialized()) {
         PROTECTED_AUTH_RESOURCES.addAll(DatabaseDescriptor.getAuthenticator().protectedResources());
         PROTECTED_AUTH_RESOURCES.addAll(DatabaseDescriptor.getAuthorizer().protectedResources());
         PROTECTED_AUTH_RESOURCES.addAll(DatabaseDescriptor.getRoleManager().protectedResources());
      }

      DROPPABLE_SYSTEM_TABLES.add(DataResource.table("system_auth", "resource_role_permissons_index"));
      factory = new Resources.DefaultResourceFactory();
   }

   public static class DefaultResourceFactory implements IResourceFactory {
      public DefaultResourceFactory() {
      }

      public IResource fromName(String name) {
         if(name.startsWith(RoleResource.root().getName())) {
            return RoleResource.fromName(name);
         } else if(name.startsWith(DataResource.root().getName())) {
            return DataResource.fromName(name);
         } else if(name.startsWith(FunctionResource.root().getName())) {
            return FunctionResource.fromName(name);
         } else if(name.startsWith(JMXResource.root().getName())) {
            return JMXResource.fromName(name);
         } else {
            throw new IllegalArgumentException(String.format("Name %s is not valid for any resource type", new Object[]{name}));
         }
      }
   }
}
