package org.apache.cassandra.tools;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Iterator;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

public abstract class AbstractJmxClient implements Closeable {
   private static final Options options = new Options();
   protected static final int DEFAULT_JMX_PORT = 7199;
   protected static final String DEFAULT_HOST = "localhost";
   protected final String host;
   protected final int port;
   protected final String username;
   protected final String password;
   protected JMXConnection jmxConn;
   protected PrintStream out;

   public AbstractJmxClient(String host, Integer port, String username, String password) throws IOException {
      this.out = System.out;
      this.host = host != null?host:"localhost";
      this.port = port != null?port.intValue():7199;
      this.username = username;
      this.password = password;
      this.jmxConn = new JMXConnection(this.host, this.port, username, password);
   }

   public void close() throws IOException {
      this.jmxConn.close();
   }

   public void writeln(Throwable err) {
      this.writeln(err.getMessage());
   }

   public void writeln(String msg) {
      this.out.println(msg);
   }

   public void write(String msg) {
      this.out.print(msg);
   }

   public void writeln(String format, Object... args) {
      this.write(format + "%n", args);
   }

   public void write(String format, Object... args) {
      this.out.printf(format, args);
   }

   public void setOutput(PrintStream out) {
      this.out = out;
   }

   public static CommandLine processArguments(String[] args) throws ParseException {
      CommandLineParser parser = new PosixParser();
      return parser.parse(options, args);
   }

   public static void addCmdOption(String shortOpt, String longOpt, boolean hasArg, String description) {
      options.addOption(shortOpt, longOpt, hasArg, description);
   }

   public static void printHelp(String synopsis, String header) {
      System.out.printf("Usage: %s%n%n", new Object[]{synopsis});
      System.out.print(header);
      System.out.println("Options:");
      Iterator var2 = options.getOptions().iterator();

      while(var2.hasNext()) {
         Object opt = var2.next();
         String shortOpt = String.format("%s,", new Object[]{((Option)opt).getOpt()});
         String longOpt = ((Option)opt).getLongOpt();
         String description = ((Option)opt).getDescription();
         System.out.printf(" -%-4s --%-17s %s%n", new Object[]{shortOpt, longOpt, description});
      }

   }

   static {
      options.addOption("h", "host", true, "JMX hostname or IP address (Default: localhost)");
      options.addOption("p", "port", true, "JMX port number (Default: 7199)");
      options.addOption("u", "username", true, "JMX username");
      options.addOption("pw", "password", true, "JMX password");
      options.addOption("H", "help", false, "Print help information");
   }
}
