package com.datastax.bdp.tools;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;
import java.util.Map.Entry;

public class CommandUtils {
   public static final Path DSE_HOME = Paths.get((String)Optional.ofNullable(System.getenv("DSE_HOME")).orElse(""), new String[0]).normalize().toAbsolutePath();

   public CommandUtils() {
   }

   public static String javaProp(String propName, String propValue) {
      return String.format("-D%s=%s", new Object[]{propName, propValue});
   }

   public static String mergeOpts(String... opts) {
      StringBuilder sb = new StringBuilder();
      if(opts.length > 0) {
         sb.append(" ");
      }

      String[] var2 = opts;
      int var3 = opts.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         String opt = var2[var4];
         sb.append(opt).append(" ");
      }

      return sb.toString();
   }

   public static String toCommandJavaProps(Map<String, String> opts) {
      String[] props = new String[opts.size()];
      int i = 0;

      Entry opt;
      for(Iterator var3 = opts.entrySet().iterator(); var3.hasNext(); props[i++] = javaProp((String)opt.getKey(), (String)opt.getValue())) {
         opt = (Entry)var3.next();
      }

      return mergeOpts(props);
   }

   public static String toAbsolutePath(String path, String defaultDir) {
      File pathFile = new File(path);
      if(!pathFile.isAbsolute()) {
         pathFile = new File(defaultDir, path);
      }

      return pathFile.getAbsolutePath();
   }

   public static String toAbsolutePath(String path) {
      Path p = Paths.get(path, new String[0]);
      return p.isAbsolute()?p.normalize().toString():DSE_HOME.resolve(path).normalize().toAbsolutePath().toString();
   }
}
