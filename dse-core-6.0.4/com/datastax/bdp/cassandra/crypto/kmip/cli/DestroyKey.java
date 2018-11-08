package com.datastax.bdp.cassandra.crypto.kmip.cli;

import com.datastax.bdp.cassandra.crypto.kmip.CloseableKmip;
import com.datastax.bdp.cassandra.crypto.kmip.KmipHost;
import java.io.IOException;

public class DestroyKey extends KmipCommand {
   public static final String COMMAND = "destroy";
   public static final String USAGE = String.format("Usage: %s %s <kmip_groupname> <key_id>", new Object[]{"managekmip", "destroy"});

   public DestroyKey() {
   }

   public String help() {
      return "Destroys the specified key. Once a key has been destroyed, it cannot be used to decrypt data.\n" + USAGE;
   }

   public boolean execute(KmipHost host, String[] args) throws IOException {
      if(args.length == 0) {
         System.out.println("Please include the key id to revoke");
         return false;
      } else {
         String keyId = args[0];
         if(!KmipCommands.acknowledgeRisks()) {
            System.out.print("Cancelled, exiting...");
            return false;
         } else {
            CloseableKmip kmip = host.getConnection();
            Throwable var5 = null;

            try {
               kmip.destroy(keyId);
               System.out.println("Done");
            } catch (Throwable var14) {
               var5 = var14;
               throw var14;
            } finally {
               if(kmip != null) {
                  if(var5 != null) {
                     try {
                        kmip.close();
                     } catch (Throwable var13) {
                        var5.addSuppressed(var13);
                     }
                  } else {
                     kmip.close();
                  }
               }

            }

            return true;
         }
      }
   }
}
