package com.datastax.bdp.db.tools.nodesync;

import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.util.Scanner;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import javax.annotation.Nullable;
import org.apache.cassandra.tools.NodeProbe;

class NodeProbeBuilder {
   private final Metadata metadata;
   private String username;
   private String password;
   private String passwordFilePath;
   private boolean ssl;

   NodeProbeBuilder(Metadata metadata) {
      this.metadata = metadata;
   }

   NodeProbeBuilder withUsername(@Nullable String username) {
      this.username = username;
      return this;
   }

   NodeProbeBuilder withPassword(@Nullable String password) {
      this.password = password;
      return this;
   }

   NodeProbeBuilder withPasswordFilePath(@Nullable String passwordFilePath) {
      this.passwordFilePath = passwordFilePath;
      return this;
   }

   private String readUserPasswordFromFile() {
      String password = "";
      File passwordFile = new File(this.passwordFilePath);

      try {
         Scanner scanner = (new Scanner(passwordFile)).useDelimiter("\\s+");
         Throwable var4 = null;

         try {
            for(; scanner.hasNextLine(); scanner.nextLine()) {
               if(scanner.hasNext()) {
                  String jmxRole = scanner.next();
                  if(jmxRole.equals(this.username) && scanner.hasNext()) {
                     password = scanner.next();
                     break;
                  }
               }
            }
         } catch (Throwable var14) {
            var4 = var14;
            throw var14;
         } finally {
            if(scanner != null) {
               if(var4 != null) {
                  try {
                     scanner.close();
                  } catch (Throwable var13) {
                     var4.addSuppressed(var13);
                  }
               } else {
                  scanner.close();
               }
            }

         }

         return password;
      } catch (FileNotFoundException var16) {
         throw new NodeSyncException("JMX password file not found: " + this.passwordFilePath);
      }
   }

   NodeProbeBuilder withSSL(boolean ssl) {
      this.ssl = ssl;
      return this;
   }

   NodeProbe build(InetAddress address) {
      if(this.passwordFilePath != null) {
         this.password = this.readUserPasswordFromFile();
      }

      if(this.ssl) {
         System.setProperty("ssl.enable", "true");
      }

      String host = address.getHostAddress();
      int port = ((Integer)this.metadata.getAllHosts().stream().filter((h) -> {
         return h.getBroadcastAddress().equals(address);
      }).map(Host::getJmxPort).findFirst().orElseThrow(() -> {
         return new NodeSyncException("Unable to find node " + address);
      })).intValue();
      if(port <= 0) {
         throw new NodeSyncException(String.format("Unable to read the JMX port of node %s, this could be because JMX is not enabled in that node or it's running a version without NodeSync support", new Object[]{host}));
      } else {
         try {
            return this.username == null?new NodeProbe(host, port):new NodeProbe(host, port, this.username, this.password);
         } catch (SecurityException | IOException var5) {
            throw new NodeSyncException(String.format("JMX connection to %s:%d failed: %s", new Object[]{address, Integer.valueOf(port), var5.getMessage()}));
         }
      }
   }
}
