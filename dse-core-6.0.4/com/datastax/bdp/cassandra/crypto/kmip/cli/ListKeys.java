package com.datastax.bdp.cassandra.crypto.kmip.cli;

import com.datastax.bdp.cassandra.crypto.KeyAccessException;
import com.datastax.bdp.cassandra.crypto.kmip.KmipHost;
import com.google.common.base.Joiner;
import java.io.IOException;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class ListKeys extends KmipCommand {
   public static final String COMMAND = "list";
   public static final String USAGE = String.format("Usage: %s %s <kmip_groupname> [namespace=<key namespace>]", new Object[]{"managekmip", "list"});
   private static final String ID = "ID";
   private static final String NAME = "Name";
   private static final String CIPHER = "Cipher";
   private static final String STATE = "State";
   private static final String ACTIVATION_DATE = "Activation Date";
   private static final String CREATION_DATE = "Creation Date";
   private static final String PROTECT_STOP_DATE = "Protect Stop Date";
   private static final String NAMESPACE = "Namespace";
   private static final String NA = "n/a";
   private static final Options options = new Options();

   public ListKeys() {
   }

   public String help() {
      return "Lists the keys on the given KMIP host\n" + USAGE;
   }

   private String fw(int size) {
      return "%" + size + "s";
   }

   private String formatDate(Date date) {
      return date == null?"n/a":date.toString();
   }

   private String maybeNA(String s) {
      return s != null?s:"n/a";
   }

   private String getFmtString(List<KmipHost.KeyAttrs> keys) {
      int maxId = "ID".length();
      int maxName = "Name".length();
      int maxCipher = "Cipher".length();
      int maxState = "State".length();
      int maxActive = "Activation Date".length();
      int maxCreate = "Creation Date".length();
      int maxProtect = "Protect Stop Date".length();
      int maxNamespace = "Namespace".length();

      KmipHost.KeyAttrs key;
      for(Iterator var10 = keys.iterator(); var10.hasNext(); maxNamespace = Math.max(maxNamespace, this.maybeNA(key.namespace).length())) {
         key = (KmipHost.KeyAttrs)var10.next();
         maxId = Math.max(maxId, key.id.length());
         maxName = Math.max(maxName, this.maybeNA(key.name).length());
         maxCipher = Math.max(maxCipher, key.cipherInfo.toString().length());
         maxState = Math.max(maxState, key.state.name().length());
         maxActive = Math.max(maxActive, this.formatDate(key.activationDate).length());
         maxCreate = Math.max(maxCreate, this.formatDate(key.creationDate).length());
         maxProtect = Math.max(maxProtect, this.formatDate(key.protectStopDate).length());
      }

      Joiner joiner = Joiner.on("   ");
      return joiner.join(this.fw(maxId), this.fw(maxName), new Object[]{this.fw(maxCipher), this.fw(maxState), this.fw(maxActive), this.fw(maxCreate), this.fw(maxProtect), this.fw(maxNamespace)});
   }

   private void printKey(String fmt, KmipHost.KeyAttrs key) {
      System.out.println(String.format(fmt, new Object[]{key.id, this.maybeNA(key.name), key.cipherInfo, key.state.name(), this.formatDate(key.activationDate), this.formatDate(key.creationDate), this.formatDate(key.protectStopDate), this.maybeNA(key.namespace)}));
   }

   public boolean execute(KmipHost host, String[] args) throws IOException {
      System.out.println("Keys on " + host.getHostName() + ":");

      try {
         CommandLineParser parser = new GnuParser();
         CommandLine cl = parser.parse(options, args);
         KmipHost.Options kmipOptions = new KmipHost.Options((String)null, cl.getOptionValue("namespace"));
         List<KmipHost.KeyAttrs> keys = host.listAll(kmipOptions);
         String fmt = this.getFmtString(keys);
         System.out.println(String.format(fmt, new Object[]{"ID", "Name", "Cipher", "State", "Activation Date", "Creation Date", "Protect Stop Date", "Namespace"}));
         Iterator var8 = keys.iterator();

         while(var8.hasNext()) {
            KmipHost.KeyAttrs key = (KmipHost.KeyAttrs)var8.next();
            this.printKey(fmt, key);
         }

         return true;
      } catch (KeyAccessException var10) {
         System.out.println("Error retrieving key data: " + var10.getMessage());
         return false;
      } catch (ParseException var11) {
         System.out.println("Error parsing args: " + var11.getMessage());
         System.out.println(USAGE);
         return false;
      }
   }

   static {
      options.addOption("ns", "namespace", true, "namespace to restrict listing to");
   }
}
