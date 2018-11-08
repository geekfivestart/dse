package com.datastax.bdp.tools;

import com.datastax.bdp.config.ClientConfiguration;
import com.datastax.bdp.config.ClientConfigurationBuilder;
import com.diffplug.common.base.Errors;
import com.diffplug.common.base.Throwing.Function;
import com.google.inject.Inject;
import java.net.InetAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class DseToolArgumentParser {
   private static final Pair<String, String> HOST_OPT = Pair.create("h", "host");
   private static final Pair<String, String> CASSANDRA_PORT_OPT = Pair.create("c", "cassandra_port");
   private static final Pair<String, String> SOLR_PORT_OPT = Pair.create("s", "port");
   private static final Pair<String, String> JMX_PORT_OPT = Pair.create("j", "jmxport");
   private static final Pair<String, String> JMX_USER_OPT = Pair.create("a", "jmxusername");
   private static final Pair<String, String> JMX_PASSWORD_OPT = Pair.create("b", "jmxpassword");
   private static final Pair<String, String> USER_OPT = Pair.create("l", "username");
   private static final Pair<String, String> PASSWORD_OPT = Pair.create("p", "password");
   private static final Pair<String, String> CONFIG_FILE_OPT = Pair.create("f", "config-file");
   private static final Pair<String, String> KEYSTORE_PATH_OPT = Pair.create((Object)null, "keystore-path");
   private static final Pair<String, String> KEYSTORE_PASSWORD_OPT = Pair.create((Object)null, "keystore-password");
   private static final Pair<String, String> KEYSTORE_TYPE_OPT = Pair.create((Object)null, "keystore-type");
   private static final Pair<String, String> TRUSTSTORE_PATH_OPT = Pair.create((Object)null, "truststore-path");
   private static final Pair<String, String> TRUSTSTORE_PASSWORD_OPT = Pair.create((Object)null, "truststore-password");
   private static final Pair<String, String> TRUSTSTORE_TYPE_OPT = Pair.create((Object)null, "truststore-type");
   private static final Pair<String, String> SSL_PROTOCOL_OPT = Pair.create((Object)null, "ssl-protocol");
   private static final Pair<String, String> CIPHER_SUITES_OPT = Pair.create((Object)null, "cipher-suites");
   private static final Pair<String, String> SSL_ENABLED_OPT = Pair.create((Object)null, "ssl");
   private static final Pair<String, String> SSL_AUTH_ENABLED_OPT = Pair.create((Object)null, "sslauth");
   private static DseToolArgumentParser.ToolOptions options = null;
   private ClientConfigurationBuilder clientConfigBuilder;
   private DseTool.Plugin commandPlugin = null;
   private String solrPort = "8983";
   private int jmxPort = 7199;
   private String rawHostArgument = "127.0.0.1";
   private String command = null;
   private String[] arguments = null;
   private Optional<String> jmxUsername = Optional.empty();
   private Optional<String> jmxPassword = Optional.empty();
   private Optional<String> username = Optional.empty();
   private Optional<String> password = Optional.empty();

   @Inject
   public DseToolArgumentParser(ClientConfiguration baseClientConfig) {
      assert baseClientConfig != null;

      this.clientConfigBuilder = new ClientConfigurationBuilder(baseClientConfig);
   }

   public ClientConfiguration getClientConfiguration() {
      return this.clientConfigBuilder.build();
   }

   public DseTool.Plugin getCommandPlugin() {
      return this.commandPlugin;
   }

   public void parse(String[] args, Map<String, DseTool.Plugin> commands) throws Exception {
      CommandLineParser parser = new DefaultParser();
      DseToolArgumentParser.ToolCommandLine cmd = new DseToolArgumentParser.ToolCommandLine(parser.parse(options, args, true));
      this.jmxUsername = cmd.getAsOptional(JMX_USER_OPT);
      this.jmxPassword = cmd.getAsOptional(JMX_PASSWORD_OPT);
      this.username = cmd.getAsOptional(USER_OPT);
      this.password = cmd.getAsOptional(PASSWORD_OPT);
      if(this.jmxPassword.isPresent() ^ this.jmxUsername.isPresent()) {
         throw new IllegalArgumentException("dsetool: You need to specify both JMX username and JMX password.");
      } else if(this.username.isPresent() ^ this.password.isPresent()) {
         throw new IllegalArgumentException("dsetool: You need to specify both a username and a password.");
      } else {
         try {
            this.command = cmd.getCommand();
         } catch (IllegalArgumentException var8) {
            throw new ParseException(var8.getMessage());
         }

         String commandString = this.command.toLowerCase();
         Iterator var6 = commands.keySet().iterator();

         while(var6.hasNext()) {
            String key = (String)var6.next();
            if(key.split(" ")[0].equals(commandString)) {
               this.commandPlugin = (DseTool.Plugin)commands.get(key);
            }
         }

         Optional var10000 = cmd.getAsOptional(HOST_OPT).map(Errors.rethrow().wrap(InetAddress::getByName));
         ClientConfigurationBuilder var10001 = this.clientConfigBuilder;
         this.clientConfigBuilder.getClass();
         var10000.ifPresent(var10001::withCassandraHost);
         this.rawHostArgument = (String)cmd.getAsOptional(HOST_OPT).orElse(this.rawHostArgument);
         this.jmxPort = ((Integer)cmd.getAsOptional(JMX_PORT_OPT).map(Integer::parseInt).orElse(Integer.valueOf(this.jmxPort))).intValue();
         this.arguments = cmd.getCommandArguments();
         this.solrPort = (String)cmd.getAsOptional(SOLR_PORT_OPT).orElse(this.solrPort);
         var10000 = cmd.getAsOptional(CASSANDRA_PORT_OPT).map(Integer::parseInt);
         var10001 = this.clientConfigBuilder;
         this.clientConfigBuilder.getClass();
         var10000.ifPresent(var10001::withNativePort);
         var10000 = cmd.getAsOptional(KEYSTORE_PATH_OPT);
         var10001 = this.clientConfigBuilder;
         this.clientConfigBuilder.getClass();
         var10000.ifPresent(var10001::withSslKeystorePath);
         var10000 = cmd.getAsOptional(KEYSTORE_PASSWORD_OPT);
         var10001 = this.clientConfigBuilder;
         this.clientConfigBuilder.getClass();
         var10000.ifPresent(var10001::withSslKeystorePassword);
         var10000 = cmd.getAsOptional(KEYSTORE_TYPE_OPT);
         var10001 = this.clientConfigBuilder;
         this.clientConfigBuilder.getClass();
         var10000.ifPresent(var10001::withSslKeystoreType);
         var10000 = cmd.getAsOptional(TRUSTSTORE_PATH_OPT);
         var10001 = this.clientConfigBuilder;
         this.clientConfigBuilder.getClass();
         var10000.ifPresent(var10001::withSslTruststorePath);
         var10000 = cmd.getAsOptional(TRUSTSTORE_PASSWORD_OPT);
         var10001 = this.clientConfigBuilder;
         this.clientConfigBuilder.getClass();
         var10000.ifPresent(var10001::withSslTruststorePassword);
         var10000 = cmd.getAsOptional(TRUSTSTORE_TYPE_OPT);
         var10001 = this.clientConfigBuilder;
         this.clientConfigBuilder.getClass();
         var10000.ifPresent(var10001::withSslTruststoreType);
         var10000 = cmd.getAsOptional(SSL_PROTOCOL_OPT);
         var10001 = this.clientConfigBuilder;
         this.clientConfigBuilder.getClass();
         var10000.ifPresent(var10001::withSslProtocol);
         var10000 = cmd.getAsOptional(CIPHER_SUITES_OPT).map((s) -> {
            return s.split("\\s*,\\s*");
         });
         var10001 = this.clientConfigBuilder;
         this.clientConfigBuilder.getClass();
         var10000.ifPresent(var10001::withCipherSuites);
         var10000 = cmd.getAsOptional(SSL_ENABLED_OPT).map(Boolean::parseBoolean);
         var10001 = this.clientConfigBuilder;
         this.clientConfigBuilder.getClass();
         var10000.ifPresent(var10001::withSslEnabled);
         boolean clientAuth = ((Boolean)cmd.getAsOptional(SSL_AUTH_ENABLED_OPT).map(Boolean::parseBoolean).orElse(Boolean.valueOf(false))).booleanValue();
         if(!clientAuth) {
            this.clientConfigBuilder.withSslKeystorePath((String)null);
            this.clientConfigBuilder.withSslKeystorePassword((String)null);
            this.clientConfigBuilder.withSslKeystoreType((String)null);
         }

      }
   }

   public String getRawHostArgument() {
      return this.rawHostArgument;
   }

   public String getSolrPort() {
      return this.solrPort;
   }

   public String getJmxHostName() {
      return this.rawHostArgument;
   }

   public int getJmxPort() {
      return this.jmxPort;
   }

   public String getJmxUsername() {
      return (String)this.jmxUsername.orElse((Object)null);
   }

   public String getJmxPassword() {
      return (String)this.jmxPassword.orElse((Object)null);
   }

   public String getUsername() {
      return (String)this.username.orElse((Object)null);
   }

   public String getPassword() {
      return (String)this.password.orElse((Object)null);
   }

   public String getCommand() {
      return this.command;
   }

   public String getPerfCommand() throws ParseException {
      if(this.arguments.length >= 1) {
         return this.arguments[0].toLowerCase();
      } else {
         throw new ParseException("Expecting a subcommand");
      }
   }

   public String[] getArguments() {
      return this.arguments;
   }

   public void printUsage() {
      HelpFormatter hf = new HelpFormatter();
      hf.printHelp("dsetool [-short <arg>] [--long=<arg>] <command> [command-args] ", "", options, "");
   }

   static {
      options = new DseToolArgumentParser.ToolOptions();
      options.addOption(HOST_OPT, true, "node hostname or ip address");
      options.addOption(SOLR_PORT_OPT, true, "Solr port to use");
      options.addOption(CASSANDRA_PORT_OPT, true, "Cassandra port to use");
      options.addOption(JMX_PORT_OPT, true, "remote jmx agent port number");
      options.addOption(JMX_USER_OPT, true, "JMX user name");
      options.addOption(JMX_PASSWORD_OPT, true, "JMX password");
      options.addOption(USER_OPT, true, "User name");
      options.addOption(PASSWORD_OPT, true, "Password");
      options.addOption(CONFIG_FILE_OPT, true, "DSE configuration file");
      options.addOption(KEYSTORE_PATH_OPT, true, "Keystore for connection to Cassandra when SSL client auth is enabled");
      options.addOption(KEYSTORE_PASSWORD_OPT, true, "Keystore password for connection to Cassandra when SSL client auth is enabled");
      options.addOption(KEYSTORE_TYPE_OPT, true, "Keystore type for connection to Cassandra when SSL client auth is enabled");
      options.addOption(TRUSTSTORE_PATH_OPT, true, "Truststore for connection to Cassandra when SSL is enabled");
      options.addOption(TRUSTSTORE_PASSWORD_OPT, true, "Truststore password for connection to Cassandra when SSL is enabled");
      options.addOption(TRUSTSTORE_TYPE_OPT, true, "Truststore type for connection to Cassandra when SSL is enabled");
      options.addOption(SSL_PROTOCOL_OPT, true, "SSL protocol for connection to Cassandra when SSL is enabled");
      options.addOption(CIPHER_SUITES_OPT, true, "Comma separated list of SSL cipher suites for connection to Cassandra when SSL is enabled");
      options.addOption(SSL_ENABLED_OPT, true, "Use or not SSL for jmx and native connections");
      options.addOption(SSL_AUTH_ENABLED_OPT, true, "Use or not SSL client authentication");
   }

   private static class ToolCommandLine {
      private final CommandLine commandLine;

      public ToolCommandLine(CommandLine commands) {
         this.commandLine = commands;
      }

      public Optional<String> getAsOptional(Pair<String, String> opt) {
         String key = opt.left != null?(String)opt.left:(String)opt.right;
         return Optional.ofNullable(this.commandLine.getOptionValue(key));
      }

      public String getCommand() throws ParseException {
         if(this.commandLine.getArgs().length == 0) {
            throw new IllegalArgumentException("Command was not specified.");
         } else {
            return this.commandLine.getArgs()[0];
         }
      }

      public String[] getCommandArguments() {
         List params = this.commandLine.getArgList();
         if(params.size() < 2) {
            return new String[0];
         } else {
            String[] toReturn = new String[params.size() - 1];

            for(int i = 1; i < params.size(); ++i) {
               toReturn[i - 1] = (String)params.get(i);
            }

            return toReturn;
         }
      }
   }

   private static class ToolOptions extends Options {
      private ToolOptions() {
      }

      public void addOption(Pair<String, String> opts, boolean hasArgument, String description) {
         this.addOption(opts, hasArgument, description, false);
      }

      public void addOption(Pair<String, String> opts, boolean hasArgument, String description, boolean required) {
         this.addOption((String)opts.left, (String)opts.right, hasArgument, description, required);
      }

      public void addOption(String opt, String longOpt, boolean hasArgument, String description, boolean required) {
         Option option = new Option(opt, longOpt, hasArgument, description);
         option.setRequired(required);
         this.addOption(option);
      }
   }
}
