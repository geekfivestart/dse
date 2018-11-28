package com.datastax.bdp.tools;

import com.datastax.bdp.DseClientModule;
import com.datastax.driver.core.exceptions.InvalidQueryException;
import com.google.common.annotations.VisibleForTesting;
import com.google.inject.Guice;
import com.google.inject.Inject;
import com.google.inject.Module;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.UndeclaredThrowableException;
import java.rmi.ConnectException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.IntFunction;
import jline.TerminalFactory;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.WordUtils;
import org.reflections.Reflections;
import org.reflections.scanners.Scanner;

public class DseTool {
   private static final int OPTION_INDENT = 8;
   private static final int HELP_MARGINS = 3;
   private final Map<String, DseTool.Plugin> commands;
   @Inject
   private NodeJmxProxyPoolFactory nodeJmxPoolFactory;
   @Inject
   private DseToolArgumentParser parser;

   public DseTool() {
      this(Collections.unmodifiableMap(findPlugins(DseTool.Plugin.class)));
   }

   public DseTool(Map<String, DseTool.Plugin> commands) {
      this.commands = commands;
   }

   private void prettyPrintHelp(StringBuilder header) {
      List<String> keys = new ArrayList(this.commands.keySet());
      Collections.sort(keys);
      int maxLine = Math.max(91, Math.min(150, TerminalFactory.get().getWidth()));
      int maxLength = getMaxLength(keys);

      for(Iterator var5 = keys.iterator(); var5.hasNext(); header.append("\n")) {
         String key = (String)var5.next();
         Command command = (Command)this.commands.get(key);
         header.append(key).append(StringUtils.repeat(" ", maxLength - (key.length() + 3 - 3))).append(" - ");
         String[] var8 = command.getHelp().split("\n");
         int var9 = var8.length;

         for(int var10 = 0; var10 < var9; ++var10) {
            String paragraph = var8[var10];
            String[] lines = WordUtils.wrap(paragraph, maxLine - (maxLength + 3)).split("\n");
            header.append(lines[0]);

            for(int i = 1; i < lines.length; ++i) {
               header.append('\n').append(StringUtils.repeat(" ", maxLength + 3)).append(lines[i]);
            }
         }

         String options = command.getOptionsHelp();
         if(options == null) {
            header.append("\n");
         } else {
            (new DseTool.Options(options)).print(header, maxLine);
         }
      }

   }

   private static int getMaxLength(Iterable<String> keys) {
      int maxLength = 0;

      String key;
      for(Iterator var2 = keys.iterator(); var2.hasNext(); maxLength = Math.max(maxLength, key.length())) {
         key = (String)var2.next();
      }

      return maxLength;
   }

   public int run(String[] args) throws InstantiationException, IllegalAccessException {
      byte exitCode = 0;

      try {
         this.parser.parse(args, this.commands);
         this.run(this.parser);
      } catch (UndeclaredThrowableException var5) {
         System.err.println("One or more JMX beans have not been registered. Has this node finished starting up?");
         System.err.println(var5.getCause().toString());
         exitCode = 3;
      } catch (ParseException var6) {
         System.err.println(var6.getLocalizedMessage());
         this.printUsage(this.parser);
         exitCode = 1;
      } catch (AssertionError | InvalidQueryException | IllegalArgumentException var7) {
         System.err.println(var7.getLocalizedMessage());
         exitCode = 1;
      } catch (Throwable var8) {
         Throwable i = var8;

         while(true) {
            if(i == null) {
               var8.printStackTrace(System.err);
               exitCode = 2;
               break;
            }

            if(i instanceof ConnectException || i instanceof java.net.ConnectException) {
               System.err.println(i.getLocalizedMessage());
               return 2;
            }

            i = i.getCause();
         }
      }

      return exitCode;
   }

   @VisibleForTesting
   public Map<String, DseTool.Plugin> getCommands() {
      return this.commands;
   }

   public void run(DseToolArgumentParser parser) throws Exception {
      DseTool.Plugin p = parser.getCommandPlugin();
      if(p == null) {
         throw new ParseException("Unknown command: " + parser.getCommand());
      } else {
         if(p.isJMX()) {
            this.nodeJmxPoolFactory.setUsername(parser.getJmxUsername());
            this.nodeJmxPoolFactory.setPassword(parser.getJmxPassword());
            this.nodeJmxPoolFactory.setHost(parser.getJmxHostName());
            this.nodeJmxPoolFactory.setPort(parser.getJmxPort());
            if(p.isJMXMultinode()) {
               p.executeJMX(this.nodeJmxPoolFactory, new NodeProbe(parser.getJmxHostName(), parser.getJmxPort(), parser.getJmxUsername(), parser.getJmxPassword()), this.nodeJmxPoolFactory.get(), parser);
            } else {
               p.executeJMX(new NodeProbe(parser.getJmxHostName(), parser.getJmxPort(), parser.getJmxUsername(), parser.getJmxPassword()), this.nodeJmxPoolFactory.get(), parser);
            }
         } else {
            p.execute(parser);
         }

      }
   }

   public static <T extends Command> Map<String, T> findPlugins(Class<T> type) {
      TreeMap<String, T> result = new TreeMap();
      Reflections reflections = new Reflections("com.datastax.bdp.tools", new Scanner[0]);
      for (Class p : reflections.getSubTypesOf(type)) {
         try {
            T plugin = (T)p.getConstructor(new Class[0]).newInstance(new Object[0]);
            result.put(plugin.getName(), plugin);
         }
         catch (IllegalAccessException | InstantiationException | NoSuchMethodException e) {
            System.err.println("--- Unable to instantiate " + p.getName());
            e.printStackTrace(System.err);
         }
         catch (InvocationTargetException e) {
            System.err.println("--- Unable to instantiate " + p.getName());
            e.getTargetException().printStackTrace(System.err);
         }
      }
      return result;
   }

   public static void main(String[] args) throws Exception {
      DseTool dseTool = (DseTool)Guice.createInjector(new Module[]{new DseClientModule()}).getInstance(DseTool.class);
      int exitCode = dseTool.run(args);
      System.exit(exitCode);
   }

   private void printUsage(DseToolArgumentParser parser) {
      parser.printUsage();
      StringBuilder sb = new StringBuilder("\n");
      this.prettyPrintHelp(sb);
      System.out.println(sb.toString());
   }

   public abstract static class Plugin implements Command {
      public Plugin() {
      }

      public boolean isJMX() {
         return true;
      }

      public boolean isJMXMultinode() {
         return false;
      }

      public String getOptionsHelp() {
         return null;
      }

      public void executeJMX(NodeJmxProxyPoolFactory jmxFactory, NodeProbe np, NodeJmxProxyPool dnp, DseToolArgumentParser args) throws Exception {
         this.executeJMX(jmxFactory, np, dnp, args.getArguments());
      }

      public void executeJMX(NodeJmxProxyPoolFactory jmxFactory, NodeProbe np, NodeJmxProxyPool dnp, String[] args) throws Exception {
         throw new RuntimeException("Override one of the executeJMX methods");
      }

      public void executeJMX(NodeProbe np, NodeJmxProxyPool dnp, DseToolArgumentParser args) throws Exception {
         this.executeJMX(np, dnp, args.getArguments());
      }

      public void executeJMX(NodeProbe np, NodeJmxProxyPool dnp, String[] args) throws Exception {
         throw new RuntimeException("Override one of the executeJMX methods");
      }

      public void execute(DseToolArgumentParser args) throws Exception {
         this.execute(args.getArguments());
      }

      public void execute(String[] args) throws Exception {
         throw new RuntimeException("Override one of the execute methods");
      }
   }

   public static class Option {
      private final String key;
      private final String description;

      public Option(String key, String description) {
         this.description = description;
         this.key = key;
      }

      public String getDescription() {
         return this.description;
      }

      public String getKey() {
         return this.key;
      }
   }

   private static class Options {
      private final DseTool.Option[] options;

      private Options(String optionsString) {
         this.options = (DseTool.Option[])Arrays.asList(optionsString.split("\n")).stream().map((optionString) -> {
            String[] split = optionString.split("\t");
            return new DseTool.Option(split[0], split.length == 1?"":split[1]);
         }).toArray((x$0) -> {
            return new DseTool.Option[x$0];
         });
      }

      public void print(StringBuilder header, int maxLine) {
         header.append('\n');
         int maxKeyLength = this.getMaxLength();
         DseTool.Option[] var4 = this.options;
         int var5 = var4.length;

         for(int var6 = 0; var6 < var5; ++var6) {
            DseTool.Option option = var4[var6];
            header.append(StringUtils.repeat(" ", 8)).append(option.getKey()).append(StringUtils.repeat(" ", 3 + maxKeyLength - option.getKey().length()));
            String[] optionLines = WordUtils.wrap(option.getDescription(), maxLine - maxKeyLength - 3 - 8).split("\n");
            if(optionLines.length > 0) {
               header.append(optionLines[0]);

               for(int i = 1; i < optionLines.length; ++i) {
                  header.append('\n').append(StringUtils.repeat(" ", 8 + maxKeyLength + 3)).append(optionLines[i]);
               }
            }

            header.append('\n');
         }

      }

      private int getMaxLength() {
         int maxLength = 0;
         DseTool.Option[] var2 = this.options;
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            DseTool.Option option = var2[var4];
            maxLength = Math.max(maxLength, option.getKey().length());
         }

         return maxLength;
      }
   }
}
