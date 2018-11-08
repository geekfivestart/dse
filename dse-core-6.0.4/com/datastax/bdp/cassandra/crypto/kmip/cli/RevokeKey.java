package com.datastax.bdp.cassandra.crypto.kmip.cli;

import com.cryptsoft.kmip.enm.RevocationReasonCode;
import com.datastax.bdp.cassandra.crypto.kmip.CloseableKmip;
import com.datastax.bdp.cassandra.crypto.kmip.KmipHost;
import java.io.IOException;
import java.util.Date;

public class RevokeKey extends KmipCommand {
   public static final String COMMAND = "revoke";
   public static final String USAGE = String.format("Usage: %s %s <kmip_groupname> <key_id>", new Object[]{"managekmip", "revoke"});
   private static RevocationReasonCode[] REASONS;

   public RevokeKey() {
   }

   public String help() {
      return "Revokes the given key. Once a key has been revoked, it cannot be used to decrypt data.\n" + USAGE;
   }

   RevocationReasonCode getReason() {
      System.out.println("Select reason for revokation:");

      int selection;
      for(selection = 0; selection < REASONS.length; ++selection) {
         System.out.println(String.format("  [%s] %s", new Object[]{Integer.valueOf(selection), REASONS[selection].name()}));
      }

      selection = Integer.parseInt(console.readLine("Reason (0 - %s): ", new Object[]{Integer.valueOf(REASONS.length - 1)}));
      if(selection >= 0 && selection < REASONS.length) {
         return REASONS[selection];
      } else {
         System.out.println(String.format("Selection must be between 0 and %s: ", new Object[]{Integer.valueOf(REASONS.length - 1)}));
         return null;
      }
   }

   String getMessage() {
      return console.readLine("Revokation message (optional): ", new Object[0]);
   }

   boolean confirm(String keyId, RevocationReasonCode reason, String message, Date time) {
      System.out.println(String.format("Revoking key %s", new Object[]{keyId}));
      System.out.println(String.format("  Reason: %s", new Object[]{reason.name()}));
      System.out.println(String.format("  Message: %s", new Object[]{message}));
      System.out.println(String.format("  Time: %s", new Object[]{time}));
      String response = console.readLine("Would you like to continue? yes / [no]  ", new Object[0]);
      System.out.println();
      return response.toLowerCase().equals("yes");
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
            RevocationReasonCode reason = this.getReason();
            if(reason == null) {
               System.out.print("No reason given, exiting...");
               return false;
            } else {
               String message = this.getMessage();
               Date time = new Date();
               if(!this.confirm(keyId, reason, message, time)) {
                  System.out.print("Cancelled, exiting...");
                  return false;
               } else {
                  CloseableKmip kmip = host.getConnection();
                  Throwable var8 = null;

                  try {
                     kmip.revoke(keyId, reason, message, new Date());
                  } catch (Throwable var17) {
                     var8 = var17;
                     throw var17;
                  } finally {
                     if(kmip != null) {
                        if(var8 != null) {
                           try {
                              kmip.close();
                           } catch (Throwable var16) {
                              var8.addSuppressed(var16);
                           }
                        } else {
                           kmip.close();
                        }
                     }

                  }

                  System.out.println("Done");
                  return true;
               }
            }
         }
      }
   }

   static {
      REASONS = new RevocationReasonCode[]{RevocationReasonCode.Unspecified, RevocationReasonCode.KeyCompromise, RevocationReasonCode.CACompromise, RevocationReasonCode.AffiliationChanged, RevocationReasonCode.Superseded, RevocationReasonCode.CessationOfOperation, RevocationReasonCode.PrivilegeWithdrawn};
   }
}
