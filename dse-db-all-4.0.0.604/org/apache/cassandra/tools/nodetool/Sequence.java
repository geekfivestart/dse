package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Joiner;
import io.airlift.airline.Arguments;
import io.airlift.airline.Cli;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOError;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URL;
import java.text.DateFormat;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.utils.Pair;

@Command(
   name = "sequence",
   description = "Run multiple nodetool commands from a file, resource or stdin in sequence. Common options (host, port, username, password) are passed to child commands."
)
public class Sequence extends NodeTool.NodeToolCmd {
   @Option(
      name = {"--stoponerror"},
      description = "By default, if one child command fails, the sequence command continues with remaining commands. Set this option to true to stop on error."
   )
   private boolean stopOnError = false;
   @Option(
      name = {"--failonerror"},
      description = "By default, the sequence command will not fail (return an error exit code) if one or more child commands fail. Set this option to true to return an error exit code if a child command fails."
   )
   private boolean failOnError = false;
   @Option(
      name = {"-i", "--input"},
      description = "The file or classpath resource to read nodetool commands from, one command per line. Use /dev/stdin to read from standard input. Multiple input can be provided."
   )
   private List<String> input;
   @Arguments(
      description = "Commands to execute. Separate individual commands using a colon surrounded by whitespaces (' : ').Example: 'nodetool sequence 'info : gettimeout read : gettimeout write : status'."
   )
   private List<String> commands;

   public Sequence() {
   }

   protected void execute(NodeProbe probe) {
      List<Pair<Runnable, String>> runnables = new ArrayList();
      if(this.input != null) {
         Iterator var3 = this.input.iterator();

         while(var3.hasNext()) {
            String in = (String)var3.next();
            this.readInput(runnables, in);
         }
      }

      if(this.commands != null) {
         this.collectCommandLine(runnables);
      }

      long tTotal0 = System.currentTimeMillis();
      System.out.println("################################################################################");
      System.out.printf("# Executing %d commands:%n", new Object[]{Integer.valueOf(runnables.size())});
      Iterator var5 = runnables.iterator();

      while(var5.hasNext()) {
         Pair<Runnable, String> cmd = (Pair)var5.next();
         System.out.printf("# %s%n", new Object[]{cmd.right});
      }

      System.out.println("################################################################################");

      try {
         Enumeration niEnum = NetworkInterface.getNetworkInterfaces();

         while(niEnum.hasMoreElements()) {
            NetworkInterface ni = (NetworkInterface)niEnum.nextElement();
            System.out.printf("# Network interface %s (%s): %s%n", new Object[]{ni.getName(), ni.getDisplayName(), Joiner.on(", ").join(ni.getInterfaceAddresses())});
         }

         System.out.println("################################################################################");
      } catch (SocketException var10) {
         ;
      }

      boolean failed = false;
      int success = 0;
      List<Pair<String, String>> failures = new ArrayList();
      Iterator var8 = runnables.iterator();

      Pair failure;
      while(var8.hasNext()) {
         failure = (Pair)var8.next();
         if(this.executeRunnable(probe, failure, failures)) {
            ++success;
         } else {
            failed = true;
         }

         System.out.println("################################################################################");
         if(failed && this.stopOnError) {
            break;
         }
      }

      System.out.printf("# Total duration: %dms%n", new Object[]{Long.valueOf(System.currentTimeMillis() - tTotal0)});
      System.out.printf("# Out of %d commands, %d completed successfully, %d failed.%n", new Object[]{Integer.valueOf(runnables.size()), Integer.valueOf(success), Integer.valueOf(failures.size())});
      var8 = failures.iterator();

      while(var8.hasNext()) {
         failure = (Pair)var8.next();
         System.out.printf("# Failed: '%s': %s%n", new Object[]{failure.left, failure.right});
      }

      System.out.println("################################################################################");
      if(failed && this.failOnError) {
         probe.failed();
      }

   }

   private void collectCommandLine(List<Pair<Runnable, String>> runnables) {
      StringBuilder sb = new StringBuilder();
      List<String> args = new ArrayList();
      Iterator var4 = this.commands.iterator();

      while(var4.hasNext()) {
         String arg = (String)var4.next();
         arg = arg.trim();
         if(arg.equals(":")) {
            this.collectCommand(runnables, sb.toString(), (String[])args.toArray(new String[0]));
            sb.setLength(0);
            args.clear();
         } else {
            if(sb.length() > 0) {
               sb.append(' ');
            }

            sb.append(arg);
            args.add(arg);
         }
      }

      if(!args.isEmpty()) {
         this.collectCommand(runnables, sb.toString(), (String[])args.toArray(new String[0]));
         sb.setLength(0);
         args.clear();
      }

   }

   private boolean executeRunnable(NodeProbe probe, Pair<Runnable, String> cmd, List<Pair<String, String>> failures) {
      DateFormat dfZulu = DateFormat.getDateTimeInstance(1, 1);
      dfZulu.setTimeZone(TimeZone.getTimeZone(ZoneId.of("Z")));
      DateFormat dfLocal = DateFormat.getDateTimeInstance(1, 1);
      long t = System.currentTimeMillis();
      Date d = new Date(t);
      System.out.println("# Command: " + (String)cmd.right);
      System.out.println("# Timestamp: " + dfZulu.format(d));
      System.out.println("# Timestamp (local): " + dfLocal.format(d));
      System.out.println("# Timestamp (millis since epoch): " + t);
      System.out.println("################################################################################");
      if(cmd.left instanceof NodeTool.NodeToolCmd) {
         NodeTool.NodeToolCmd nodeToolCmd = (NodeTool.NodeToolCmd)cmd.left;
         nodeToolCmd.applyGeneralArugments(this);
         long t0 = System.nanoTime();

         try {
            nodeToolCmd.sequenceRun(probe);
            if(!probe.isFailed()) {
               System.out.printf("# Command '%s' completed successfully in %d ms%n", new Object[]{cmd.right, Long.valueOf(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0))});
               return true;
            } else {
               probe.clearFailed();
               System.out.printf("# Command '%s' failed%n", new Object[]{cmd.right});
               failures.add(Pair.create(cmd.right, "(see above)"));
               return false;
            }
         } catch (RuntimeException var26) {
            probe.clearFailed();
            System.out.printf("# Command '%s' failed with exception %s%n", new Object[]{cmd.right, var26});
            failures.add(Pair.create(cmd.right, var26.toString()));
            return false;
         }
      } else {
         long t0 = System.nanoTime();

         try {
            ((Runnable)cmd.left).run();
            System.out.printf("# Command '%s' completed in %d ms%n", new Object[]{cmd.right, Long.valueOf(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - t0))});
            return true;
         } catch (RuntimeException var28) {
            RuntimeException e = var28;
            StringWriter sw = new StringWriter();
            PrintWriter pw = new PrintWriter(sw);
            Throwable var14 = null;

            try {
               e.getCause().printStackTrace(pw);
            } catch (Throwable var25) {
               var14 = var25;
               throw var25;
            } finally {
               if(pw != null) {
                  if(var14 != null) {
                     try {
                        pw.close();
                     } catch (Throwable var24) {
                        var14.addSuppressed(var24);
                     }
                  } else {
                     pw.close();
                  }
               }

            }

            String err = sw.toString();
            System.out.printf("# Command '%s' failed with exception %s%n", new Object[]{cmd.right, var28.getCause()});
            failures.add(Pair.create(cmd.right, err));
            return false;
         }
      }
   }

   private void readInput(List<Pair<Runnable, String>> runnables, String in) {
      URL inputUrl = NodeTool.class.getClassLoader().getResource(in);
      if(inputUrl == null) {
         File file = new File(in);
         if(file.isFile()) {
            try {
               inputUrl = file.toURI().toURL();
            } catch (MalformedURLException var19) {
               ;
            }
         }
      }

      InputStream inputStream = null;
      if(inputUrl != null) {
         try {
            inputStream = inputUrl.openConnection().getInputStream();
         } catch (IOException var18) {
            throw new IOError(var18);
         }
      }

      if(inputStream == null && (in.equals("/dev/stdin") || in.equals("-"))) {
         inputStream = System.in;
      }

      if(inputStream == null) {
         throw new IOError(new FileNotFoundException("File/resource " + in + " not found"));
      } else {
         try {
            BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
            Throwable var6 = null;

            try {
               String line;
               try {
                  while((line = br.readLine()) != null) {
                     line = line.trim();
                     if(!line.isEmpty() && !line.startsWith("#")) {
                        this.collectCommand(runnables, line, line.split("[ \t]"));
                     }
                  }
               } catch (Throwable var20) {
                  var6 = var20;
                  throw var20;
               }
            } finally {
               if(br != null) {
                  if(var6 != null) {
                     try {
                        br.close();
                     } catch (Throwable var17) {
                        var6.addSuppressed(var17);
                     }
                  } else {
                     br.close();
                  }
               }

            }

         } catch (IOException var22) {
            throw new IOError(var22);
         }
      }
   }

   private void collectCommand(List<Pair<Runnable, String>> runnables, String line, String[] args) {
      Cli cli = NodeTool.createCli(false);

      Runnable runnable;
      try {
         runnable = (Runnable)cli.parse(args);
      } catch (Throwable var7) {
         runnable = () -> {
            throw new RuntimeException(var7);
         };
      }

      runnables.add(Pair.create(runnable, line));
   }
}
