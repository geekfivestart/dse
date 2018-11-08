package com.datastax.bdp.cassandra.crypto.kmip.cli;

import com.datastax.bdp.cassandra.crypto.kmip.KmipHost;
import java.io.Console;
import java.io.IOException;

public abstract class KmipCommand {
   static final Console console = System.console();

   public KmipCommand() {
   }

   public abstract String help();

   public abstract boolean execute(KmipHost var1, String[] var2) throws IOException;
}
