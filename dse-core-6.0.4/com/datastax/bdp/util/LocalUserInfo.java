package com.datastax.bdp.util;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LocalUserInfo {
   private static Logger logger = LoggerFactory.getLogger(LocalUserInfo.class);
   private Object system;
   private List<String> systemsToCheck = new ArrayList<String>() {
      {
         this.add("com.sun.security.auth.module.UnixSystem");
         this.add("com.sun.security.auth.module.SolarisSystem");
         this.add("org.apache.harmony.auth.module.UnixSystem");
         this.add("org.apache.harmony.auth.module.SolarisSystem");
      }
   };

   public LocalUserInfo() {
      Iterator var1 = this.systemsToCheck.iterator();

      while(var1.hasNext()) {
         String systemName = (String)var1.next();
         this.system = this.tryLoadInstance(systemName);
         if(this.system != null) {
            logger.debug("Using " + this.system + " to get username and groupname");
            break;
         }
      }

   }

   private Object tryLoadInstance(String className) {
      try {
         return Class.forName(className).newInstance();
      } catch (ClassNotFoundException var3) {
         return null;
      } catch (InstantiationException var4) {
         return null;
      } catch (IllegalAccessException var5) {
         return null;
      }
   }

   public String getCurrentUserName() {
      if(this.system == null) {
         return null;
      } else {
         try {
            return (String)this.system.getClass().getMethod("getUsername", new Class[0]).invoke(this.system, new Object[0]);
         } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException var2) {
            logger.error("Error getting current username", var2);
            return null;
         }
      }
   }

   public String getCurrentUserName(String defaultUserName) {
      String result = this.getCurrentUserName();
      return result == null?defaultUserName:result;
   }

   public Long getCurrentGroupId() {
      if(this.system == null) {
         return null;
      } else {
         try {
            return (Long)this.system.getClass().getMethod("getGid", new Class[0]).invoke(this.system, new Object[0]);
         } catch (InvocationTargetException | NoSuchMethodException | IllegalAccessException var2) {
            logger.error("Error getting current group id", var2);
            return null;
         }
      }
   }

   public String getCurrentGroupName() {
      Long gid = this.getCurrentGroupId();
      if(gid == null) {
         return null;
      } else {
         try {
            String gidStr = String.valueOf(gid);
            BufferedReader br = new BufferedReader(new FileReader("/etc/group"));

            String line;
            while((line = br.readLine()) != null) {
               if(!line.startsWith("#")) {
                  String[] fields = line.split(":");
                  if(fields.length >= 3 && gidStr.equals(fields[2])) {
                     return fields[0];
                  }
               }
            }

            return gidStr;
         } catch (IOException var6) {
            return null;
         }
      }
   }

   public String getCurrentGroupName(String defaultGroupName) {
      String result = this.getCurrentGroupName();
      return result == null?defaultGroupName:result;
   }
}
