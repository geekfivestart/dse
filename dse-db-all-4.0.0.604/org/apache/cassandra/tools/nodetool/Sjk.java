package org.apache.cassandra.tools.nodetool;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.ParameterDescription;
import com.beust.jcommander.Parameterized;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;
import javax.management.MBeanServerConnection;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.gridkit.jvmtool.JmxConnectionInfo;
import org.gridkit.jvmtool.cli.CommandLauncher;
import org.gridkit.jvmtool.cli.CommandLauncher.CmdRef;
import org.gridkit.jvmtool.cli.CommandLauncher.CommandAbortedError;

@Command(
   name = "sjk",
   description = "Run commands of 'Swiss Java Knife'. Run 'nodetool sjk --help' for more information."
)
public class Sjk extends NodeTool.NodeToolCmd {
   @Arguments(
      description = "Arguments passed as is to 'Swiss Army Knife'."
   )
   private List<String> args;
   private final Sjk.Wrapper wrapper = new Sjk.Wrapper();

   public Sjk() {
   }

   public void run() {
      this.wrapper.prepare(this.args != null?(String[])this.args.toArray(new String[0]):new String[]{"help"});
      if(!this.wrapper.requiresMbeanServerConn()) {
         this.wrapper.run((NodeProbe)null);
      } else {
         super.run();
      }

   }

   public void sequenceRun(NodeProbe probe) {
      this.wrapper.prepare(this.args != null?(String[])this.args.toArray(new String[0]):new String[]{"help"});
      if(!this.wrapper.run(probe)) {
         probe.failed();
      }

   }

   protected void execute(NodeProbe probe) {
      if(!this.wrapper.run(probe)) {
         probe.failed();
      }

   }

   public static class Wrapper extends CommandLauncher {
      boolean suppressSystemExit;
      private final Map<String, Runnable> commands = new HashMap();
      private JCommander parser;
      private Runnable cmd;

      public Wrapper() {
      }

      public void suppressSystemExit() {
         this.suppressSystemExit = true;
         super.suppressSystemExit();
      }

      public boolean start(String[] args) {
         throw new UnsupportedOperationException();
      }

      public void prepare(String[] args) {
         try {
            this.parser = new JCommander(this);
            this.addCommands();
            this.fixCommands();

            try {
               this.parser.parse(args);
            } catch (Exception var7) {
               this.failAndPrintUsage(new String[]{var7.toString()});
            }

            if(this.isHelp()) {
               String cmd = this.parser.getParsedCommand();
               if(cmd == null) {
                  this.parser.usage();
               } else {
                  this.parser.usage(cmd);
               }
            } else if(this.isListCommands()) {
               Iterator var10 = this.commands.keySet().iterator();

               while(var10.hasNext()) {
                  String cmd = (String)var10.next();
                  System.out.println(String.format("%8s - %s", new Object[]{cmd, this.parser.getCommandDescription(cmd)}));
               }
            } else {
               this.cmd = (Runnable)this.commands.get(this.parser.getParsedCommand());
               if(this.cmd == null) {
                  this.failAndPrintUsage(new String[0]);
               }
            }
         } catch (CommandAbortedError var8) {
            String[] var3 = var8.messages;
            int var4 = var3.length;

            for(int var5 = 0; var5 < var4; ++var5) {
               String m = var3[var5];
               this.logError(m);
            }

            if(this.isVerbose() && var8.getCause() != null) {
               this.logTrace(var8.getCause());
            }

            if(var8.printUsage && this.parser != null) {
               if(this.parser.getParsedCommand() != null) {
                  this.parser.usage(this.parser.getParsedCommand());
               } else {
                  this.parser.usage();
               }
            }
         } catch (Throwable var9) {
            var9.printStackTrace();
         }

      }

      public boolean run(NodeProbe probe) {
         try {
            this.setJmxConnInfo(probe);
            if(this.cmd != null) {
               this.cmd.run();
            }

            return true;
         } catch (CommandAbortedError var7) {
            String[] var3 = var7.messages;
            int var4 = var3.length;

            for(int var5 = 0; var5 < var4; ++var5) {
               String m = var3[var5];
               this.logError(m);
            }

            if(this.isVerbose() && var7.getCause() != null) {
               this.logTrace(var7.getCause());
            }

            if(var7.printUsage && this.parser != null) {
               if(this.parser.getParsedCommand() != null) {
                  this.parser.usage(this.parser.getParsedCommand());
               } else {
                  this.parser.usage();
               }
            }
         } catch (Throwable var8) {
            var8.printStackTrace();
         }

         return false;
      }

      private void setJmxConnInfo(final NodeProbe probe) throws IllegalAccessException {
         Field f = jmxConnectionInfoField(this.cmd);
         if(f != null) {
            f.setAccessible(true);
            f.set(this.cmd, new JmxConnectionInfo(this) {
               public MBeanServerConnection getMServer() {
                  return probe.getMbeanServerConn();
               }
            });
         }

         f = pidField(this.cmd);
         if(f != null) {
            long pid = probe.getPid();
            f.setAccessible(true);
            if(f.getType() == Integer.TYPE) {
               f.setInt(this.cmd, (int)pid);
            }

            if(f.getType() == Long.TYPE) {
               f.setLong(this.cmd, pid);
            }
         }

      }

      private boolean isHelp() {
         try {
            Field f = CommandLauncher.class.getDeclaredField("help");
            f.setAccessible(true);
            return f.getBoolean(this);
         } catch (Exception var2) {
            throw new RuntimeException(var2);
         }
      }

      private boolean isListCommands() {
         try {
            Field f = CommandLauncher.class.getDeclaredField("listCommands");
            f.setAccessible(true);
            return f.getBoolean(this);
         } catch (Exception var2) {
            throw new RuntimeException(var2);
         }
      }

      protected List<String> getCommandPackages() {
         return Collections.singletonList("org.gridkit.jvmtool.cmd");
      }

      private void addCommands() throws InstantiationException, IllegalAccessException {
         Iterator var1 = this.getCommandPackages().iterator();

         while(var1.hasNext()) {
            String pack = (String)var1.next();
            Iterator var3 = findClasses(pack).iterator();

            while(var3.hasNext()) {
               Class<?> c = (Class)var3.next();
               if(CmdRef.class.isAssignableFrom(c)) {
                  CmdRef cmd = (CmdRef)c.newInstance();
                  String cmdName = cmd.getCommandName();
                  Runnable cmdTask = cmd.newCommand(this);
                  if(this.commands.containsKey(cmdName)) {
                     this.fail(new String[]{"Ambiguous implementation for '" + cmdName + '\''});
                  }

                  this.commands.put(cmdName, cmdTask);
                  this.parser.addCommand(cmdName, cmdTask);
               }
            }
         }

      }

      private void fixCommands() throws Exception {
         Field mFields = JCommander.class.getDeclaredField("m_fields");
         mFields.setAccessible(true);
         Iterator var2 = this.parser.getCommands().values().iterator();

         while(var2.hasNext()) {
            JCommander cmdr = (JCommander)var2.next();
            Map<Parameterized, ParameterDescription> fields = (Map)mFields.get(cmdr);
            Iterator iPar = fields.entrySet().iterator();

            while(iPar.hasNext()) {
               Entry<Parameterized, ParameterDescription> par = (Entry)iPar.next();
               String var7 = ((Parameterized)par.getKey()).getName();
               byte var8 = -1;
               switch(var7.hashCode()) {
               case 110987:
                  if(var7.equals("pid")) {
                     var8 = 0;
                  }
                  break;
               case 3198785:
                  if(var7.equals("help")) {
                     var8 = 5;
                  }
                  break;
               case 3599307:
                  if(var7.equals("user")) {
                     var8 = 2;
                  }
                  break;
               case 351107458:
                  if(var7.equals("verbose")) {
                     var8 = 4;
                  }
                  break;
               case 481272134:
                  if(var7.equals("listCommands")) {
                     var8 = 6;
                  }
                  break;
               case 1216985755:
                  if(var7.equals("password")) {
                     var8 = 3;
                  }
                  break;
               case 1223524821:
                  if(var7.equals("sockAddr")) {
                     var8 = 1;
                  }
               }

               switch(var8) {
               case 0:
               case 1:
               case 2:
               case 3:
               case 4:
               case 5:
               case 6:
                  iPar.remove();
               }
            }
         }

      }

      boolean requiresMbeanServerConn() {
         return jmxConnectionInfoField(this.cmd) != null || pidField(this.cmd) != null;
      }

      private static Field jmxConnectionInfoField(Runnable cmd) {
         if(cmd == null) {
            return null;
         } else {
            Field[] var1 = cmd.getClass().getDeclaredFields();
            int var2 = var1.length;

            for(int var3 = 0; var3 < var2; ++var3) {
               Field f = var1[var3];
               if(f.getType() == JmxConnectionInfo.class) {
                  return f;
               }
            }

            return null;
         }
      }

      private static Field pidField(Runnable cmd) {
         if(cmd == null) {
            return null;
         } else {
            Field[] var1 = cmd.getClass().getDeclaredFields();
            int var2 = var1.length;

            for(int var3 = 0; var3 < var2; ++var3) {
               Field f = var1[var3];
               if("pid".equals(f.getName()) && f.getType() == Integer.TYPE) {
                  return f;
               }
            }

            return null;
         }
      }

      private static List<Class<?>> findClasses(String packageName) {
         ArrayList result = new ArrayList();

         try {
            String path = packageName.replace('.', '/');
            Iterator var3 = findFiles(path).iterator();

            while(var3.hasNext()) {
               String f = (String)var3.next();
               if(f.endsWith(".class") && f.indexOf(36) < 0) {
                  f = f.substring(0, f.length() - ".class".length());
                  f = f.replace('/', '.');
                  result.add(Class.forName(f));
               }
            }

            return result;
         } catch (Exception var5) {
            throw new RuntimeException(var5);
         }
      }

      static List<String> findFiles(String path) throws IOException {
         List<String> result = new ArrayList();
         ClassLoader cl = Thread.currentThread().getContextClassLoader();
         Enumeration en = cl.getResources(path);

         while(en.hasMoreElements()) {
            URL u = (URL)en.nextElement();
            listFiles(result, (URL)u, (String)path);
         }

         return result;
      }

      static void listFiles(List<String> results, URL packageURL, String path) throws IOException {
         if(packageURL.getProtocol().equals("jar")) {
            String jarFileName = URLDecoder.decode(packageURL.getFile(), "UTF-8");
            jarFileName = jarFileName.substring(5, jarFileName.indexOf(33));
            JarFile jf = new JarFile(jarFileName);
            Throwable var7 = null;

            try {
               Enumeration jarEntries = jf.entries();

               while(jarEntries.hasMoreElements()) {
                  String entryName = ((JarEntry)jarEntries.nextElement()).getName();
                  if(entryName.startsWith(path)) {
                     results.add(entryName);
                  }
               }
            } catch (Throwable var16) {
               var7 = var16;
               throw var16;
            } finally {
               if(jf != null) {
                  if(var7 != null) {
                     try {
                        jf.close();
                     } catch (Throwable var15) {
                        var7.addSuppressed(var15);
                     }
                  } else {
                     jf.close();
                  }
               }

            }
         } else {
            File dir = new File(packageURL.getFile());
            String cp = dir.getCanonicalPath();

            File root;
            for(root = dir; !cp.equals((new File(root, path)).getCanonicalPath()); root = root.getParentFile()) {
               ;
            }

            listFiles(results, root, dir);
         }

      }

      static void listFiles(List<String> names, File root, File dir) {
         String rootPath = root.getAbsolutePath();
         if(dir.exists() && dir.isDirectory()) {
            File[] var4 = dir.listFiles();
            int var5 = var4.length;

            for(int var6 = 0; var6 < var5; ++var6) {
               File file = var4[var6];
               if(file.isDirectory()) {
                  listFiles(names, root, file);
               } else {
                  String name = file.getAbsolutePath().substring(rootPath.length() + 1);
                  name = name.replace('\\', '/');
                  names.add(name);
               }
            }
         }

      }
   }
}
