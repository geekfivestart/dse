package com.datastax.bdp.cassandra.crypto.kmip.cli;

import com.datastax.bdp.cassandra.crypto.kmip.KmipHost;
import java.io.IOException;
import java.text.DateFormat;
import java.text.ParseException;
import java.util.Date;

public class ExpireKey extends KmipCommand {
   public static final String COMMAND = "expirekey";
   public static final String USAGE = String.format("Usage: %s %s <kmip_groupname> <key_id> [<time>]", new Object[]{"managekmip", "expirekey"});

   public ExpireKey() {
   }

   public String help() {
      return "Specifies a date after which no new data will be encrypted with a key, although data can be decrypted with it. If no expire time is given, the key is expired immediately\n" + USAGE;
   }

   public boolean execute(KmipHost host, String[] args) throws IOException {
      if(args.length != 1 && args.length != 2) {
         System.out.println(String.format("Invalid arguments for %s: %s", new Object[]{"expirekey", args}));
         return false;
      } else {
         String keyId = args[0];
         Date when;
         if(args.length == 2) {
            try {
               when = DateFormat.getInstance().parse(args[1]);
            } catch (ParseException var6) {
               System.out.println(String.format("Invalid date format: %s, %s", new Object[]{args[1], var6.getMessage()}));
               return false;
            }
         } else {
            when = new Date();
         }

         host.stopKey(keyId, when);
         System.out.println("Done");
         return true;
      }
   }
}
