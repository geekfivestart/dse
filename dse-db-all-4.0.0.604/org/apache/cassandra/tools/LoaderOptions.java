package org.apache.cassandra.tools;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.PlainTextAuthProvider;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.net.InetAddress;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Set;
import org.apache.cassandra.config.Config;
import org.apache.cassandra.config.ConfigurationLoader;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;

public class LoaderOptions {
   public static final String HELP_OPTION = "help";
   public static final String VERBOSE_OPTION = "verbose";
   public static final String NOPROGRESS_OPTION = "no-progress";
   public static final String NATIVE_PORT_OPTION = "port";
   public static final String STORAGE_PORT_OPTION = "storage-port";
   public static final String SSL_STORAGE_PORT_OPTION = "ssl-storage-port";
   public static final String USER_OPTION = "username";
   public static final String PASSWD_OPTION = "password";
   public static final String AUTH_PROVIDER_OPTION = "auth-provider";
   public static final String INITIAL_HOST_ADDRESS_OPTION = "nodes";
   public static final String IGNORE_NODES_OPTION = "ignore";
   public static final String CONNECTIONS_PER_HOST = "connections-per-host";
   public static final String CONFIG_PATH = "conf-path";
   public static final String DSE_CONFIG_PATH = "dse-conf-path";
   public static final String THROTTLE_MBITS = "throttle";
   public static final String INTER_DC_THROTTLE_MBITS = "inter-dc-throttle";
   public static final String TOOL_NAME = "sstableloader";
   public static final String SSL_TRUSTSTORE = "truststore";
   public static final String SSL_TRUSTSTORE_PW = "truststore-password";
   public static final String SSL_KEYSTORE = "keystore";
   public static final String SSL_KEYSTORE_PW = "keystore-password";
   public static final String SSL_PROTOCOL = "ssl-protocol";
   public static final String SSL_ALGORITHM = "ssl-alg";
   public static final String SSL_STORE_TYPE = "store-type";
   public static final String SSL_CIPHER_SUITES = "ssl-ciphers";
   public final File directory;
   public final boolean debug;
   public final boolean verbose;
   public final boolean noProgress;
   public final int nativePort;
   public final String user;
   public final String passwd;
   public final AuthProvider authProvider;
   public final int throttle;
   public final int interDcThrottle;
   public final int storagePort;
   public final int sslStoragePort;
   public final EncryptionOptions.ClientEncryptionOptions clientEncOptions;
   public final int connectionsPerHost;
   public final EncryptionOptions.ServerEncryptionOptions serverEncOptions;
   public final Set<InetAddress> hosts;
   public final Set<InetAddress> ignores;
   public final URI configFile;

   LoaderOptions(LoaderOptions.Builder builder) {
      this.directory = builder.directory;
      this.debug = builder.debug;
      this.verbose = builder.verbose;
      this.noProgress = builder.noProgress;
      this.nativePort = builder.nativePort;
      this.user = builder.user;
      this.passwd = builder.passwd;
      this.authProvider = builder.authProvider;
      this.throttle = builder.throttle;
      this.interDcThrottle = builder.interDcThrottle;
      this.storagePort = builder.storagePort;
      this.sslStoragePort = builder.sslStoragePort;
      this.clientEncOptions = builder.clientEncOptions;
      this.connectionsPerHost = builder.connectionsPerHost;
      this.serverEncOptions = builder.serverEncOptions;
      this.hosts = builder.hosts;
      this.ignores = builder.ignores;
      this.configFile = builder.configFile;
   }

   public static LoaderOptions.Builder builder() {
      return new LoaderOptions.Builder();
   }

   private static void errorMsg(String msg, BulkLoader.CmdLineOptions options) {
      System.err.println(msg);
      printUsage(options);
      System.exit(1);
   }

   private static BulkLoader.CmdLineOptions getCmdLineOptions() {
      BulkLoader.CmdLineOptions options = new BulkLoader.CmdLineOptions();
      options.addOption("v", "verbose", "verbose output");
      options.addOption("h", "help", "display this help message");
      options.addOption((String)null, "no-progress", "don't display progress");
      options.addOption("i", "ignore", "NODES", "don't stream to this (comma separated) list of nodes");
      options.addOption("d", "nodes", "initial hosts", "Required. try to connect to these hosts (comma separated) initially for ring information");
      options.addOption("p", "port", "native transport port", "port used for native connection (default 9042)");
      options.addOption("sp", "storage-port", "storage port", "port used for internode communication (default 7000)");
      options.addOption("ssp", "ssl-storage-port", "ssl storage port", "port used for TLS internode communication (default 7001)");
      options.addOption("t", "throttle", "throttle", "throttle speed in Mbits (default unlimited)");
      options.addOption("idct", "inter-dc-throttle", "inter-dc-throttle", "inter-datacenter throttle speed in Mbits (default unlimited)");
      options.addOption("u", "username", "username", "username for cassandra authentication");
      options.addOption("pw", "password", "password", "password for cassandra authentication");
      options.addOption("ap", "auth-provider", "auth provider", "custom AuthProvider class name for cassandra authentication");
      options.addOption("cph", "connections-per-host", "connectionsPerHost", "number of concurrent connections-per-host.");
      options.addOption("ts", "truststore", "TRUSTSTORE", "Client SSL: full path to truststore");
      options.addOption("tspw", "truststore-password", "TRUSTSTORE-PASSWORD", "Client SSL: password of the truststore");
      options.addOption("ks", "keystore", "KEYSTORE", "Client SSL: full path to keystore");
      options.addOption("kspw", "keystore-password", "KEYSTORE-PASSWORD", "Client SSL: password of the keystore");
      options.addOption("prtcl", "ssl-protocol", "PROTOCOL", "Client SSL: connections protocol to use (default: TLS)");
      options.addOption("alg", "ssl-alg", "ALGORITHM", "Client SSL: algorithm (default: SunX509)");
      options.addOption("st", "store-type", "STORE-TYPE", "Client SSL: type of store");
      options.addOption("ciphers", "ssl-ciphers", "CIPHER-SUITES", "Client SSL: comma-separated list of encryption suites to use");
      options.addOption("f", "conf-path", "path to config file", "cassandra.yaml file path for streaming throughput and client/server SSL.");
      options.addOption("df", "dse-conf-path", "path to config file", "dse.yaml file path.");
      return options;
   }

   public static void printUsage(Options options) {
      String usage = String.format("%s [options] <dir_path>", new Object[]{"sstableloader"});
      String header = System.lineSeparator() + "Bulk load the sstables found in the directory <dir_path> to the configured cluster.The parent directories of <dir_path> are used as the target keyspace/table name. So for instance, to load an sstable named Standard1-g-1-Data.db into Keyspace1/Standard1, you will need to have the files Standard1-g-1-Data.db and Standard1-g-1-Index.db into a directory /path/to/Keyspace1/Standard1/.";
      String footer = System.lineSeparator() + "You can provide cassandra.yaml file with -f command line option to set up streaming throughput, client and server encryption options. Only stream_throughput_outbound_megabits_per_sec, server_encryption_options and client_encryption_options are read from yaml. You can override options read from cassandra.yaml with corresponding command line options.";
      (new HelpFormatter()).printHelp(usage, header, options, footer);
   }

   static class Builder {
      File directory;
      boolean debug;
      boolean verbose;
      boolean noProgress;
      int nativePort = 9042;
      String user;
      String passwd;
      String authProviderName;
      AuthProvider authProvider;
      int throttle = 0;
      int interDcThrottle = 0;
      int storagePort;
      int sslStoragePort;
      EncryptionOptions.ClientEncryptionOptions clientEncOptions = new EncryptionOptions.ClientEncryptionOptions();
      int connectionsPerHost = 1;
      EncryptionOptions.ServerEncryptionOptions serverEncOptions = new EncryptionOptions.ServerEncryptionOptions();
      Set<InetAddress> hosts = new HashSet();
      Set<InetAddress> ignores = new HashSet();
      URI configFile;

      Builder() {
      }

      public LoaderOptions build() {
         this.constructAuthProvider();
         return new LoaderOptions(this);
      }

      public LoaderOptions.Builder directory(File directory) {
         this.directory = directory;
         return this;
      }

      public LoaderOptions.Builder debug(boolean debug) {
         this.debug = debug;
         return this;
      }

      public LoaderOptions.Builder verbose(boolean verbose) {
         this.verbose = verbose;
         return this;
      }

      public LoaderOptions.Builder noProgress(boolean noProgress) {
         this.noProgress = noProgress;
         return this;
      }

      public LoaderOptions.Builder nativePort(int nativePort) {
         this.nativePort = nativePort;
         return this;
      }

      public LoaderOptions.Builder user(String user) {
         this.user = user;
         return this;
      }

      public LoaderOptions.Builder password(String passwd) {
         this.passwd = passwd;
         return this;
      }

      public LoaderOptions.Builder authProvider(AuthProvider authProvider) {
         this.authProvider = authProvider;
         return this;
      }

      public LoaderOptions.Builder throttle(int throttle) {
         this.throttle = throttle;
         return this;
      }

      public LoaderOptions.Builder interDcThrottle(int interDcThrottle) {
         this.interDcThrottle = interDcThrottle;
         return this;
      }

      public LoaderOptions.Builder storagePort(int storagePort) {
         this.storagePort = storagePort;
         return this;
      }

      public LoaderOptions.Builder sslStoragePort(int sslStoragePort) {
         this.sslStoragePort = sslStoragePort;
         return this;
      }

      public LoaderOptions.Builder encOptions(EncryptionOptions.ClientEncryptionOptions encOptions) {
         this.clientEncOptions = encOptions;
         return this;
      }

      public LoaderOptions.Builder connectionsPerHost(int connectionsPerHost) {
         this.connectionsPerHost = connectionsPerHost;
         return this;
      }

      public LoaderOptions.Builder serverEncOptions(EncryptionOptions.ServerEncryptionOptions serverEncOptions) {
         this.serverEncOptions = serverEncOptions;
         return this;
      }

      public LoaderOptions.Builder hosts(Set<InetAddress> hosts) {
         this.hosts = hosts;
         return this;
      }

      public LoaderOptions.Builder host(InetAddress host) {
         this.hosts.add(host);
         return this;
      }

      public LoaderOptions.Builder ignore(Set<InetAddress> ignores) {
         this.ignores = ignores;
         return this;
      }

      public LoaderOptions.Builder ignore(InetAddress ignore) {
         this.ignores.add(ignore);
         return this;
      }

      public LoaderOptions.Builder parseArgs(String[] cmdArgs) {
         CommandLineParser parser = new GnuParser();
         BulkLoader.CmdLineOptions options = LoaderOptions.getCmdLineOptions();

         try {
            CommandLine cmd = parser.parse(options, cmdArgs, false);
            if(cmd.hasOption("help")) {
               LoaderOptions.printUsage(options);
               System.exit(0);
            }

            String[] args = cmd.getArgs();
            if(args.length == 0) {
               System.err.println("Missing sstable directory argument");
               LoaderOptions.printUsage(options);
               System.exit(1);
            }

            if(args.length > 1) {
               System.err.println("Too many arguments");
               LoaderOptions.printUsage(options);
               System.exit(1);
            }

            String dirname = args[0];
            File dir = new File(dirname);
            if(!dir.exists()) {
               LoaderOptions.errorMsg("Unknown directory: " + dirname, options);
            }

            if(!dir.isDirectory()) {
               LoaderOptions.errorMsg(dirname + " is not a directory", options);
            }

            this.directory = dir;
            this.verbose = cmd.hasOption("verbose");
            this.noProgress = cmd.hasOption("no-progress");
            if(cmd.hasOption("username")) {
               this.user = cmd.getOptionValue("username");
            }

            if(cmd.hasOption("password")) {
               this.passwd = cmd.getOptionValue("password");
            }

            if(cmd.hasOption("auth-provider")) {
               this.authProviderName = cmd.getOptionValue("auth-provider");
            }

            String[] nodes;
            String[] var9;
            int var10;
            int var11;
            String node;
            if(cmd.hasOption("nodes")) {
               nodes = cmd.getOptionValue("nodes").split(",");

               try {
                  var9 = nodes;
                  var10 = nodes.length;

                  for(var11 = 0; var11 < var10; ++var11) {
                     node = var9[var11];
                     this.hosts.add(InetAddress.getByName(node.trim()));
                  }
               } catch (UnknownHostException var14) {
                  LoaderOptions.errorMsg("Unknown host: " + var14.getMessage(), options);
               }
            } else {
               System.err.println("Initial hosts must be specified (-d)");
               LoaderOptions.printUsage(options);
               System.exit(1);
            }

            if(cmd.hasOption("ignore")) {
               nodes = cmd.getOptionValue("ignore").split(",");

               try {
                  var9 = nodes;
                  var10 = nodes.length;

                  for(var11 = 0; var11 < var10; ++var11) {
                     node = var9[var11];
                     this.ignores.add(InetAddress.getByName(node.trim()));
                  }
               } catch (UnknownHostException var13) {
                  LoaderOptions.errorMsg("Unknown host: " + var13.getMessage(), options);
               }
            }

            if(cmd.hasOption("connections-per-host")) {
               this.connectionsPerHost = Integer.parseInt(cmd.getOptionValue("connections-per-host"));
            }

            Config config;
            if(cmd.hasOption("conf-path")) {
               File configFile = new File(cmd.getOptionValue("conf-path"));
               if(!configFile.exists()) {
                  LoaderOptions.errorMsg("Config file not found", options);
               }

               this.configFile = configFile.toURI();
               config = ConfigurationLoader.create().loadConfig(this.configFile.toURL());
            } else {
               config = new Config();
               config.stream_throughput_outbound_megabits_per_sec = 0;
               config.inter_dc_stream_throughput_outbound_megabits_per_sec = 0;
            }

            if(cmd.hasOption("port")) {
               this.nativePort = Integer.parseInt(cmd.getOptionValue("port"));
            } else {
               this.nativePort = config.native_transport_port;
            }

            if(cmd.hasOption("storage-port")) {
               this.storagePort = Integer.parseInt(cmd.getOptionValue("storage-port"));
            } else {
               this.storagePort = config.storage_port;
            }

            if(cmd.hasOption("ssl-storage-port")) {
               this.sslStoragePort = Integer.parseInt(cmd.getOptionValue("ssl-storage-port"));
            } else {
               this.sslStoragePort = config.ssl_storage_port;
            }

            this.throttle = config.stream_throughput_outbound_megabits_per_sec;
            this.clientEncOptions = config.client_encryption_options;
            this.serverEncOptions = config.server_encryption_options;
            if(cmd.hasOption("throttle")) {
               this.throttle = Integer.parseInt(cmd.getOptionValue("throttle"));
            }

            if(cmd.hasOption("inter-dc-throttle")) {
               this.interDcThrottle = Integer.parseInt(cmd.getOptionValue("inter-dc-throttle"));
            }

            if(cmd.hasOption("truststore") || cmd.hasOption("truststore-password") || cmd.hasOption("keystore") || cmd.hasOption("keystore-password")) {
               this.clientEncOptions.enabled = true;
            }

            if(cmd.hasOption("truststore")) {
               this.clientEncOptions.truststore = cmd.getOptionValue("truststore");
            }

            if(cmd.hasOption("truststore-password")) {
               this.clientEncOptions.truststore_password = cmd.getOptionValue("truststore-password");
            }

            if(cmd.hasOption("keystore")) {
               this.clientEncOptions.keystore = cmd.getOptionValue("keystore");
               this.clientEncOptions.require_client_auth = true;
            }

            if(cmd.hasOption("keystore-password")) {
               this.clientEncOptions.keystore_password = cmd.getOptionValue("keystore-password");
            }

            if(cmd.hasOption("ssl-protocol")) {
               this.clientEncOptions.protocol = cmd.getOptionValue("ssl-protocol");
            }

            if(cmd.hasOption("ssl-alg")) {
               this.clientEncOptions.algorithm = cmd.getOptionValue("ssl-alg");
            }

            if(cmd.hasOption("store-type")) {
               this.clientEncOptions.store_type = cmd.getOptionValue("store-type");
            }

            if(cmd.hasOption("ssl-ciphers")) {
               this.clientEncOptions.cipher_suites = cmd.getOptionValue("ssl-ciphers").split(",");
            }

            return this;
         } catch (ConfigurationException | MalformedURLException | ParseException var15) {
            LoaderOptions.errorMsg(var15.getMessage(), options);
            return null;
         }
      }

      private void constructAuthProvider() {
         if(this.user != null != (this.passwd != null)) {
            LoaderOptions.errorMsg("Username and password must both be provided", LoaderOptions.getCmdLineOptions());
         }

         if(this.user != null) {
            if(this.authProviderName != null) {
               try {
                  Class authProviderClass = Class.forName(this.authProviderName);
                  Constructor constructor = authProviderClass.getConstructor(new Class[]{String.class, String.class});
                  this.authProvider = (AuthProvider)constructor.newInstance(new Object[]{this.user, this.passwd});
               } catch (ClassNotFoundException var4) {
                  LoaderOptions.errorMsg("Unknown auth provider: " + var4.getMessage(), LoaderOptions.getCmdLineOptions());
               } catch (NoSuchMethodException var5) {
                  LoaderOptions.errorMsg("Auth provider does not support plain text credentials: " + var5.getMessage(), LoaderOptions.getCmdLineOptions());
               } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | InstantiationException var6) {
                  LoaderOptions.errorMsg("Could not create auth provider with plain text credentials: " + var6.getMessage(), LoaderOptions.getCmdLineOptions());
               }
            } else {
               this.authProvider = new PlainTextAuthProvider(this.user, this.passwd);
            }
         } else if(this.authProviderName != null) {
            try {
               this.authProvider = (AuthProvider)Class.forName(this.authProviderName).newInstance();
            } catch (InstantiationException | IllegalAccessException | ClassNotFoundException var3) {
               LoaderOptions.errorMsg("Unknown auth provider: " + var3.getMessage(), LoaderOptions.getCmdLineOptions());
            }
         }

      }
   }
}
