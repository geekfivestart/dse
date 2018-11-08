package com.datastax.bdp.tools;

import com.datastax.bdp.server.DseDaemon;
import java.lang.reflect.Method;
import java.util.Arrays;

public class ShellClientWrapper {
   public ShellClientWrapper() {
   }

   public static void main(String[] args) throws Exception {
      if(args.length < 1) {
         throw new IllegalArgumentException("at least one argument (wrapped class name) required");
      } else {
         Method method = Class.forName(args[0]).getMethod("main", new Class[]{String[].class});
         DseDaemon.initDseClientMode();
         method.invoke((Object)null, new Object[]{Arrays.copyOfRange(args, 1, args.length)});
      }
   }
}
