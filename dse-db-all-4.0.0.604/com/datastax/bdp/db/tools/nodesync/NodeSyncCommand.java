package com.datastax.bdp.db.tools.nodesync;

import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.MaterializedViewMetadata;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TableMetadata;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.annotation.Nullable;
import org.apache.cassandra.cql3.ColumnIdentifier;

public abstract class NodeSyncCommand implements Runnable {
   @VisibleForTesting
   static final Pattern TABLE_NAME_PATTERN = Pattern.compile("((?<k>(\\w+|\"\\w+\"))\\.)?(?<t>(\\w+|\"\\w+\"))");
   private static Splitter ON_COMMA = Splitter.on(',').trimResults().omitEmptyStrings();
   @Option(
      type = OptionType.GLOBAL,
      name = {"-h", "--host"},
      description = "CQL contact point address"
   )
   private String cqlHost = "127.0.0.1";
   @Option(
      type = OptionType.GLOBAL,
      name = {"-p", "--port"},
      description = "CQL port number"
   )
   private int cqlPort = 9042;
   @Option(
      type = OptionType.GLOBAL,
      name = {"-ca", "--cql-auth-provider"},
      description = "CQL auth provider class name"
   )
   private String cqlAuthProvider;
   @Option(
      type = OptionType.GLOBAL,
      name = {"-cu", "--cql-username"},
      description = "CQL username"
   )
   private String cqlUsername;
   @Option(
      type = OptionType.GLOBAL,
      name = {"-cp", "--cql-password"},
      description = "CQL password"
   )
   private String cqlPassword;
   @Option(
      type = OptionType.GLOBAL,
      name = {"-cs", "--cql-ssl"},
      description = "Enable SSL for CQL"
   )
   private boolean cqlSSL = false;
   @Option(
      type = OptionType.GLOBAL,
      name = {"-ju", "--jmx-username"},
      description = "JMX username"
   )
   private String jmxUsername;
   @Option(
      type = OptionType.GLOBAL,
      name = {"-jp", "--jmx-password"},
      description = "JMX password"
   )
   private String jmxPassword;
   @Option(
      type = OptionType.GLOBAL,
      name = {"-jpf", "--jmx-password-file"},
      description = "Path to the JMX password file"
   )
   private String jmxPasswordFile;
   @Option(
      type = OptionType.GLOBAL,
      name = {"-js", "--jmx-ssl"},
      description = "Enable SSL for JMX"
   )
   private boolean jmxSSL = false;
   @Option(
      type = OptionType.COMMAND,
      name = {"-v", "--verbose"},
      description = "Verbose output"
   )
   private boolean verbose = false;
   @Option(
      type = OptionType.COMMAND,
      name = {"-q", "--quiet"},
      description = "Quiet output; don't print warnings"
   )
   private boolean quiet = false;

   public NodeSyncCommand() {
   }

   public void run() {
      InetAddress address;
      try {
         address = InetAddress.getByName(this.cqlHost);
      } catch (UnknownHostException var55) {
         throw new RuntimeException(var55);
      }

      Cluster cluster = this.buildCluster(address);
      Throwable var3 = null;

      try {
         Session session = cluster.newSession();
         Throwable var5 = null;

         try {
            NodeProbes probes = this.buildNodeProbes(cluster.getMetadata(), address);
            Throwable var7 = null;

            try {
               Metadata metadata = cluster.getMetadata();
               this.execute(metadata, session, probes);
            } catch (Throwable var54) {
               var7 = var54;
               throw var54;
            } finally {
               if(probes != null) {
                  if(var7 != null) {
                     try {
                        probes.close();
                     } catch (Throwable var53) {
                        var7.addSuppressed(var53);
                     }
                  } else {
                     probes.close();
                  }
               }

            }
         } catch (Throwable var57) {
            var5 = var57;
            throw var57;
         } finally {
            if(session != null) {
               if(var5 != null) {
                  try {
                     session.close();
                  } catch (Throwable var52) {
                     var5.addSuppressed(var52);
                  }
               } else {
                  session.close();
               }
            }

         }
      } catch (Throwable var59) {
         var3 = var59;
         throw var59;
      } finally {
         if(cluster != null) {
            if(var3 != null) {
               try {
                  cluster.close();
               } catch (Throwable var51) {
                  var3.addSuppressed(var51);
               }
            } else {
               cluster.close();
            }
         }

      }

   }

   public void validateOptions() throws InvalidOptionException {
      if(this.verbose && this.quiet) {
         throw new InvalidOptionException("Cannot use both --verbose and --quiet at the same time.");
      }
   }

   private Cluster buildCluster(InetAddress address) {
      return (new ClusterBuilder(address, this.cqlPort)).withAuthProvider(this.cqlAuthProvider).withUsername(this.cqlUsername).withPassword(this.cqlPassword).withSSL(this.cqlSSL).build();
   }

   private NodeProbes buildNodeProbes(Metadata metadata, InetAddress address) {
      NodeProbeBuilder builder = (new NodeProbeBuilder(metadata)).withUsername(this.jmxUsername).withPassword(this.jmxPassword).withPasswordFilePath(this.jmxPasswordFile).withSSL(this.jmxSSL);
      return new NodeProbes(metadata, builder, address);
   }

   protected abstract void execute(Metadata var1, Session var2, NodeProbes var3);

   static KeyspaceMetadata parseKeyspace(Metadata metadata, String keyspace) {
      KeyspaceMetadata keyspaceMetadata = metadata.getKeyspace(keyspace);
      if(keyspaceMetadata == null) {
         throw new NodeSyncException(String.format("Keyspace [%s] does not exist.", new Object[]{keyspace}));
      } else {
         return keyspaceMetadata;
      }
   }

   static AbstractTableMetadata parseTable(Metadata metadata, @Nullable String defaultKeyspace, String maybeQualifiedTable) {
      Matcher matcher = TABLE_NAME_PATTERN.matcher(maybeQualifiedTable);
      if(!matcher.matches()) {
         throw new NodeSyncException("Cannot parse table name: " + maybeQualifiedTable);
      } else {
         String keyspaceName = matcher.group("k");
         String tableName = matcher.group("t");
         if(keyspaceName == null) {
            if(defaultKeyspace == null) {
               throw new NodeSyncException("Keyspace required for unqualified table name: " + tableName);
            }

            keyspaceName = defaultKeyspace;
         }

         KeyspaceMetadata keyspaceMetadata = parseKeyspace(metadata, keyspaceName);
         TableMetadata tableMetadata = keyspaceMetadata.getTable(tableName);
         if(tableMetadata == null) {
            MaterializedViewMetadata viewMetadata = keyspaceMetadata.getMaterializedView(tableName);
            if(viewMetadata != null) {
               return viewMetadata;
            } else {
               throw new NodeSyncException(String.format("Table [%s.%s] does not exist.", new Object[]{keyspaceName, tableName}));
            }
         } else {
            return tableMetadata;
         }
      }
   }

   static Set<InetAddress> parseInetAddressList(String addressList) {
      Set<InetAddress> addresses = new HashSet();
      Iterator var2 = ON_COMMA.split(addressList).iterator();

      while(var2.hasNext()) {
         String s = (String)var2.next();

         try {
            addresses.add(InetAddress.getByName(s));
         } catch (UnknownHostException var5) {
            throw new NodeSyncException("Unknown or invalid address: " + s);
         }
      }

      if(addresses.isEmpty()) {
         throw new NodeSyncException("Invalid empty list of addresses provided.");
      } else {
         return addresses;
      }
   }

   void printVerbose(String msg, Object... args) {
      if(this.verbose) {
         System.out.println(String.format(msg, args));
      }

   }

   void printWarning(String msg, Object... args) {
      if(!this.quiet) {
         System.err.println(String.format("Warning: " + msg, args));
      }

   }

   static String fullyQualifiedTableName(AbstractTableMetadata metadata) {
      return fullyQualifiedTableName(metadata.getKeyspace().getName(), metadata.getName());
   }

   static String fullyQualifiedTableName(String keyspace, String table) {
      return String.format("%s.%s", new Object[]{ColumnIdentifier.maybeQuote(keyspace), ColumnIdentifier.maybeQuote(table)});
   }
}
