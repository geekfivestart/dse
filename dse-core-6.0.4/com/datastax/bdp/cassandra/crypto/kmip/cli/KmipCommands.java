package com.datastax.bdp.cassandra.crypto.kmip.cli;

import com.google.common.collect.ImmutableMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class KmipCommands {
   public static final String MANAGE_KMIP = "managekmip";
   private static final String WARN_BOX = "***********\n* WARNING *\n***********";
   private static final String DATA_LOSS_WARNING = "Revoking/destroying keys will render any data encrypted with them unreadable.\nBefore revoking or destroying a key, the following procedure should be followed\nfor re-keying any data encrypted with this key:\n    * Set expiration of key using `dsetool managekmip expirekey <id>`\n    * After expiration time has passed, run `nodetool upgradesstables -a` on any tables\n       (including system tables) that may be using the key.";
   private static final Map<String, KmipCommand> commands = ImmutableMap.<String,KmipCommand>builder().put("help", new CliHelp()).put("list", new ListKeys()).put("expirekey", new ExpireKey()).put("revoke", new RevokeKey()).put("destroy", new DestroyKey()).build();
   private static final List<String> COMMAND_NAMES;

   public KmipCommands() {
   }

   public static List<String> getCommandNames() {
      return COMMAND_NAMES;
   }

   public static KmipCommand getCommand(String name) {
      return (KmipCommand)commands.get(name);
   }

   static boolean acknowledgeRisks() {
      System.out.println("\n***********\n* WARNING *\n***********");
      System.out.println("\nRevoking/destroying keys will render any data encrypted with them unreadable.\nBefore revoking or destroying a key, the following procedure should be followed\nfor re-keying any data encrypted with this key:\n    * Set expiration of key using `dsetool managekmip expirekey <id>`\n    * After expiration time has passed, run `nodetool upgradesstables -a` on any tables\n       (including system tables) that may be using the key.\n");
      String response = System.console().readLine("Would you like to continue? yes / [no]  ", new Object[0]);
      System.out.println();
      return response.toLowerCase().equals("yes");
   }

   static {
      List<String> NAMES = new ArrayList(commands.keySet());
      NAMES.remove("help");
      COMMAND_NAMES = NAMES;
   }
}
