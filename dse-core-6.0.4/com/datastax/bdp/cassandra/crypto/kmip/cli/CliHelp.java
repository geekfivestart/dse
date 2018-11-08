package com.datastax.bdp.cassandra.crypto.kmip.cli;

import com.datastax.bdp.cassandra.crypto.kmip.KmipHost;
import java.io.IOException;
import java.util.Iterator;

public class CliHelp extends KmipCommand {
   public static final String COMMAND = "help";

   public CliHelp() {
   }

   public String help() {
      return "prints help for the available commands";
   }

   public boolean execute(KmipHost host, String[] args) throws IOException {
      Iterator var3 = KmipCommands.getCommandNames().iterator();

      while(var3.hasNext()) {
         String cmd = (String)var3.next();
         System.out.println(cmd);
         String help = KmipCommands.getCommand(cmd).help();
         String[] var6 = help.split("\n");
         int var7 = var6.length;

         for(int var8 = 0; var8 < var7; ++var8) {
            String line = var6[var8];
            System.out.println("  " + line);
         }

         System.out.println();
      }

      return true;
   }
}
