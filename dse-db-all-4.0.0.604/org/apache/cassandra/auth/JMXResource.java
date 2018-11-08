package org.apache.cassandra.auth;

import java.lang.management.ManagementFactory;
import java.util.Objects;
import java.util.Set;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import javax.management.QueryExp;
import org.apache.cassandra.auth.permission.CorePermission;
import org.apache.cassandra.auth.permission.Permissions;
import org.apache.commons.lang3.StringUtils;

public class JMXResource implements IResource {
   private static final String ROOT_NAME = "mbean";
   private static final JMXResource ROOT_RESOURCE = new JMXResource();
   private final JMXResource.Level level;
   private final String name;
   private static final Set<Permission> JMX_PERMISSIONS;

   private JMXResource() {
      this.level = JMXResource.Level.ROOT;
      this.name = null;
   }

   private JMXResource(String name) {
      this.name = name;
      this.level = JMXResource.Level.MBEAN;
   }

   public static JMXResource mbean(String name) {
      return new JMXResource(name);
   }

   public static JMXResource fromName(String name) {
      String[] parts = StringUtils.split(name, '/');
      if(parts[0].equals("mbean") && parts.length <= 2) {
         return parts.length == 1?root():mbean(parts[1]);
      } else {
         throw new IllegalArgumentException(String.format("%s is not a valid JMX resource name", new Object[]{name}));
      }
   }

   public String getName() {
      if(this.level == JMXResource.Level.ROOT) {
         return "mbean";
      } else if(this.level == JMXResource.Level.MBEAN) {
         return String.format("%s/%s", new Object[]{"mbean", this.name});
      } else {
         throw new AssertionError();
      }
   }

   public String getObjectName() {
      if(this.level == JMXResource.Level.ROOT) {
         throw new IllegalStateException(String.format("%s JMX resource has no object name", new Object[]{this.level}));
      } else {
         return this.name;
      }
   }

   public static JMXResource root() {
      return ROOT_RESOURCE;
   }

   public IResource getParent() {
      if(this.level == JMXResource.Level.MBEAN) {
         return root();
      } else {
         throw new IllegalStateException("Root-level resource can't have a parent");
      }
   }

   public boolean hasParent() {
      return !this.level.equals(JMXResource.Level.ROOT);
   }

   public boolean exists() {
      if(!this.hasParent()) {
         return true;
      } else {
         MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

         try {
            return !mbs.queryNames(new ObjectName(this.name), (QueryExp)null).isEmpty();
         } catch (MalformedObjectNameException var3) {
            return false;
         } catch (NullPointerException var4) {
            return false;
         }
      }
   }

   public Set<Permission> applicablePermissions() {
      return JMX_PERMISSIONS;
   }

   public String toString() {
      return this.level == JMXResource.Level.ROOT?"<all mbeans>":String.format("<mbean %s>", new Object[]{this.name});
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof JMXResource)) {
         return false;
      } else {
         JMXResource j = (JMXResource)o;
         return Objects.equals(this.level, j.level) && Objects.equals(this.name, j.name);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.level, this.name});
   }

   static {
      JMX_PERMISSIONS = Permissions.immutableSetOf(new Permission[]{CorePermission.AUTHORIZE, CorePermission.DESCRIBE, CorePermission.EXECUTE, CorePermission.MODIFY, CorePermission.SELECT});
   }

   static enum Level {
      ROOT,
      MBEAN;

      private Level() {
      }
   }
}
