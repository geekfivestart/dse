package org.apache.cassandra.utils;

import com.datastax.driver.core.AuthProvider;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PlainTextAuthProvider;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.SSLOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TokenRange;
import com.datastax.driver.core.Cluster.Builder;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.ReversedType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.sstable.SSTableLoader;
import org.apache.cassandra.schema.CQLTypeParser;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.DroppedColumn;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.Types;

public class NativeSSTableLoaderClient extends SSTableLoader.Client {
   protected final Map<String, TableMetadataRef> tables;
   private final Collection<InetAddress> hosts;
   private final int port;
   private final AuthProvider authProvider;
   private final SSLOptions sslOptions;

   public NativeSSTableLoaderClient(Collection<InetAddress> hosts, int port, String username, String password, SSLOptions sslOptions) {
      this(hosts, port, new PlainTextAuthProvider(username, password), sslOptions);
   }

   public NativeSSTableLoaderClient(Collection<InetAddress> hosts, int port, AuthProvider authProvider, SSLOptions sslOptions) {
      this.tables = new HashMap();
      this.hosts = hosts;
      this.port = port;
      this.authProvider = authProvider;
      this.sslOptions = sslOptions;
   }

   public void init(String keyspace) {
      Builder builder = Cluster.builder().addContactPoints(this.hosts).withPort(this.port);
      if(this.sslOptions != null) {
         builder.withSSL(this.sslOptions);
      }

      if(this.authProvider != null) {
         builder = builder.withAuthProvider(this.authProvider);
      }

      Cluster cluster = builder.build();
      Throwable var4 = null;

      try {
         Session session = cluster.connect();
         Throwable var6 = null;

         try {
            Metadata metadata = cluster.getMetadata();
            Set<TokenRange> tokenRanges = metadata.getTokenRanges();
            IPartitioner partitioner = FBUtilities.newPartitioner(metadata.getPartitioner());
            Token.TokenFactory tokenFactory = partitioner.getTokenFactory();
            Iterator var11 = tokenRanges.iterator();

            label232:
            while(true) {
               if(var11.hasNext()) {
                  TokenRange tokenRange = (TokenRange)var11.next();
                  Set<Host> endpoints = metadata.getReplicas(Metadata.quote(keyspace), tokenRange);
                  Range<Token> range = new Range(tokenFactory.fromString(tokenRange.getStart().getValue().toString()), tokenFactory.fromString(tokenRange.getEnd().getValue().toString()));
                  Iterator var15 = endpoints.iterator();

                  while(true) {
                     if(!var15.hasNext()) {
                        continue label232;
                     }

                     Host endpoint = (Host)var15.next();
                     this.addRangeForEndpoint(range, endpoint.getBroadcastAddress());
                  }
               }

               Types types = fetchTypes(keyspace, session);
               this.tables.putAll(fetchTables(keyspace, session, partitioner, types));
               this.tables.putAll(fetchViews(keyspace, session, partitioner, types));
               return;
            }
         } catch (Throwable var38) {
            var6 = var38;
            throw var38;
         } finally {
            if(session != null) {
               if(var6 != null) {
                  try {
                     session.close();
                  } catch (Throwable var37) {
                     var6.addSuppressed(var37);
                  }
               } else {
                  session.close();
               }
            }

         }
      } catch (Throwable var40) {
         var4 = var40;
         throw var40;
      } finally {
         if(cluster != null) {
            if(var4 != null) {
               try {
                  cluster.close();
               } catch (Throwable var36) {
                  var4.addSuppressed(var36);
               }
            } else {
               cluster.close();
            }
         }

      }
   }

   public TableMetadataRef getTableMetadata(String tableName) {
      return (TableMetadataRef)this.tables.get(tableName);
   }

   public void setTableMetadata(TableMetadataRef cfm) {
      this.tables.put(cfm.name, cfm);
   }

   private static Types fetchTypes(String keyspace, Session session) {
      String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ?", new Object[]{"system_schema", "types"});
      Types.RawBuilder types = Types.rawBuilder(keyspace);
      Iterator var4 = session.execute(query, new Object[]{keyspace}).iterator();

      while(var4.hasNext()) {
         Row row = (Row)var4.next();
         String name = row.getString("type_name");
         List<String> fieldNames = row.getList("field_names", String.class);
         List<String> fieldTypes = row.getList("field_types", String.class);
         types.add(name, fieldNames, fieldTypes);
      }

      return types.build();
   }

   private static Map<String, TableMetadataRef> fetchTables(String keyspace, Session session, IPartitioner partitioner, Types types) {
      Map<String, TableMetadataRef> tables = new HashMap();
      String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ?", new Object[]{"system_schema", "tables"});
      Iterator var6 = session.execute(query, new Object[]{keyspace}).iterator();

      while(var6.hasNext()) {
         Row row = (Row)var6.next();
         String name = row.getString("table_name");
         tables.put(name, createTableMetadata(keyspace, session, partitioner, false, row, name, types));
      }

      return tables;
   }

   private static Map<String, TableMetadataRef> fetchViews(String keyspace, Session session, IPartitioner partitioner, Types types) {
      Map<String, TableMetadataRef> tables = new HashMap();
      String query = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ?", new Object[]{"system_schema", "views"});
      Iterator var6 = session.execute(query, new Object[]{keyspace}).iterator();

      while(var6.hasNext()) {
         Row row = (Row)var6.next();
         String name = row.getString("view_name");
         tables.put(name, createTableMetadata(keyspace, session, partitioner, true, row, name, types));
      }

      return tables;
   }

   private static TableMetadataRef createTableMetadata(String keyspace, Session session, IPartitioner partitioner, boolean isView, Row row, String name, Types types) {
      TableMetadata.Builder builder = TableMetadata.builder(keyspace, name, TableId.fromUUID(row.getUUID("id"))).partitioner(partitioner);
      if(!isView) {
         builder.flags(TableMetadata.Flag.fromStringSet(row.getSet("flags", String.class)));
      }

      String columnsQuery = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", new Object[]{"system_schema", "columns"});
      Iterator var9 = session.execute(columnsQuery, new Object[]{keyspace, name}).iterator();

      while(var9.hasNext()) {
         Row colRow = (Row)var9.next();
         builder.addColumn(createDefinitionFromRow(colRow, keyspace, name, types));
      }

      String droppedColumnsQuery = String.format("SELECT * FROM %s.%s WHERE keyspace_name = ? AND table_name = ?", new Object[]{"system_schema", "dropped_columns"});
      Map<ByteBuffer, DroppedColumn> droppedColumns = new HashMap();
      Iterator var11 = session.execute(droppedColumnsQuery, new Object[]{keyspace, name}).iterator();

      while(var11.hasNext()) {
         Row colRow = (Row)var11.next();
         DroppedColumn droppedColumn = createDroppedColumnFromRow(colRow, keyspace, name);
         droppedColumns.put(droppedColumn.column.name.bytes, droppedColumn);
      }

      builder.droppedColumns(droppedColumns);
      return TableMetadataRef.forOfflineTools(builder.build());
   }

   private static ColumnMetadata createDefinitionFromRow(Row row, String keyspace, String table, Types types) {
      ColumnMetadata.ClusteringOrder order = ColumnMetadata.ClusteringOrder.valueOf(row.getString("clustering_order").toUpperCase());
      AbstractType<?> type = CQLTypeParser.parse(keyspace, row.getString("type"), types);
      if(order == ColumnMetadata.ClusteringOrder.DESC) {
         type = ReversedType.getInstance((AbstractType)type);
      }

      ColumnIdentifier name = ColumnIdentifier.getInterned((AbstractType)type, row.getBytes("column_name_bytes"), row.getString("column_name"));
      int position = row.getInt("position");
      ColumnMetadata.Kind kind = ColumnMetadata.Kind.valueOf(row.getString("kind").toUpperCase());
      return new ColumnMetadata(keyspace, table, name, (AbstractType)type, position, kind);
   }

   private static DroppedColumn createDroppedColumnFromRow(Row row, String keyspace, String table) {
      String name = row.getString("column_name");
      AbstractType<?> type = CQLTypeParser.parse(keyspace, row.getString("type"), Types.none());
      ColumnMetadata.Kind kind = ColumnMetadata.Kind.valueOf(row.getString("kind").toUpperCase());
      ColumnMetadata column = new ColumnMetadata(keyspace, table, ColumnIdentifier.getInterned(name, true), type, -1, kind);
      long droppedTime = row.getTimestamp("dropped_time").getTime();
      return new DroppedColumn(column, droppedTime);
   }
}
