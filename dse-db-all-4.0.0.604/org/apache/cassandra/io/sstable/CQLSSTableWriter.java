package org.apache.cassandra.io.sstable;

import com.datastax.driver.core.ProtocolVersion;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.core.UserType;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.UpdateParameters;
import org.apache.cassandra.cql3.statements.CreateTableStatement;
import org.apache.cassandra.cql3.statements.CreateTypeStatement;
import org.apache.cassandra.cql3.statements.ModificationStatement;
import org.apache.cassandra.cql3.statements.ParsedStatement;
import org.apache.cassandra.cql3.statements.UpdateStatement;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Murmur3Partitioner;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.schema.Functions;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.KeyspaceParams;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.SchemaKeyspace;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.schema.TableMetadataRef;
import org.apache.cassandra.schema.Tables;
import org.apache.cassandra.schema.Types;
import org.apache.cassandra.schema.Views;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.JavaDriverUtils;
import org.apache.cassandra.utils.Pair;

public class CQLSSTableWriter implements Closeable {
   public static final ByteBuffer UNSET_VALUE;
   private final AbstractSSTableSimpleWriter writer;
   private final UpdateStatement insert;
   private final List<ColumnSpecification> boundNames;
   private final List<TypeCodec> typeCodecs;

   private CQLSSTableWriter(AbstractSSTableSimpleWriter writer, UpdateStatement insert, List<ColumnSpecification> boundNames) {
      this.writer = writer;
      this.insert = insert;
      this.boundNames = boundNames;
      this.typeCodecs = (List)boundNames.stream().map((bn) -> {
         return JavaDriverUtils.codecFor(bn.type);
      }).collect(Collectors.toList());
   }

   public static CQLSSTableWriter.Builder builder() {
      return new CQLSSTableWriter.Builder();
   }

   public CQLSSTableWriter addRow(Object... values) throws InvalidRequestException, IOException {
      return this.addRow(Arrays.asList(values));
   }

   public CQLSSTableWriter addRow(List<Object> values) throws InvalidRequestException, IOException {
      int size = Math.min(values.size(), this.boundNames.size());
      List<ByteBuffer> rawValues = new ArrayList(size);

      for(int i = 0; i < size; ++i) {
         Object value = values.get(i);
         rawValues.add(this.serialize(value, (TypeCodec)this.typeCodecs.get(i)));
      }

      return this.rawAddRow((List)rawValues);
   }

   public CQLSSTableWriter addRow(Map<String, Object> values) throws InvalidRequestException, IOException {
      int size = this.boundNames.size();
      List<ByteBuffer> rawValues = new ArrayList(size);

      for(int i = 0; i < size; ++i) {
         ColumnSpecification spec = (ColumnSpecification)this.boundNames.get(i);
         Object value = values.get(spec.name.toString());
         rawValues.add(this.serialize(value, (TypeCodec)this.typeCodecs.get(i)));
      }

      return this.rawAddRow((List)rawValues);
   }

   public CQLSSTableWriter rawAddRow(ByteBuffer... values) throws InvalidRequestException, IOException {
      return this.rawAddRow(Arrays.asList(values));
   }

   public CQLSSTableWriter rawAddRow(List<ByteBuffer> values) throws InvalidRequestException, IOException {
      if(values.size() != this.boundNames.size()) {
         throw new InvalidRequestException(String.format("Invalid number of arguments, expecting %d values but got %d", new Object[]{Integer.valueOf(this.boundNames.size()), Integer.valueOf(values.size())}));
      } else {
         QueryOptions options = QueryOptions.forInternalCalls((ConsistencyLevel)null, values);
         List<ByteBuffer> keys = this.insert.buildPartitionKeyNames(options);
         SortedSet<Clustering> clusterings = this.insert.createClustering(options);
         long now = System.currentTimeMillis() * 1000L;
         UpdateParameters params = new UpdateParameters(this.insert.metadata, this.insert.updatedColumns(), options, this.insert.getTimestamp(now, options), this.insert.getTimeToLive(options), Collections.emptyMap());

         try {
            Iterator var8 = keys.iterator();

            while(var8.hasNext()) {
               ByteBuffer key = (ByteBuffer)var8.next();
               Iterator var10 = clusterings.iterator();

               while(var10.hasNext()) {
                  Clustering clustering = (Clustering)var10.next();
                  this.insert.addUpdateForKey(this.writer.getUpdateFor(key), clustering, params);
               }
            }

            return this;
         } catch (SSTableSimpleUnsortedWriter.SyncException var12) {
            throw (IOException)var12.getCause();
         }
      }
   }

   public CQLSSTableWriter rawAddRow(Map<String, ByteBuffer> values) throws InvalidRequestException, IOException {
      int size = Math.min(values.size(), this.boundNames.size());
      List<ByteBuffer> rawValues = new ArrayList(size);

      for(int i = 0; i < size; ++i) {
         ColumnSpecification spec = (ColumnSpecification)this.boundNames.get(i);
         rawValues.add(values.get(spec.name.toString()));
      }

      return this.rawAddRow((List)rawValues);
   }

   public UserType getUDType(String dataType) {
      KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(this.insert.keyspace());
      org.apache.cassandra.db.marshal.UserType userType = ksm.types.getNullable(ByteBufferUtil.bytes(dataType));
      return (UserType)JavaDriverUtils.driverType((AbstractType)userType);
   }

   public void close() throws IOException {
      this.writer.close();
   }

   private ByteBuffer serialize(Object value, TypeCodec codec) {
      return value != null && value != UNSET_VALUE?codec.serialize(value, ProtocolVersion.NEWEST_SUPPORTED):(ByteBuffer)value;
   }

   static {
      UNSET_VALUE = ByteBufferUtil.UNSET_BYTE_BUFFER;
      DatabaseDescriptor.clientInitialization(false);
      if(DatabaseDescriptor.getPartitioner() == null) {
         DatabaseDescriptor.setPartitionerUnsafe(Murmur3Partitioner.instance);
      }

   }

   public static class Builder {
      private File directory;
      protected SSTableFormat.Type formatType = null;
      private CreateTableStatement.RawStatement schemaStatement;
      private final List<CreateTypeStatement> typeStatements = new ArrayList();
      private ModificationStatement.Parsed insertStatement;
      private IPartitioner partitioner;
      private boolean sorted = false;
      private long bufferSizeInMB = 128L;

      protected Builder() {
      }

      public CQLSSTableWriter.Builder inDirectory(String directory) {
         return this.inDirectory(new File(directory));
      }

      public CQLSSTableWriter.Builder inDirectory(File directory) {
         if(!directory.exists()) {
            throw new IllegalArgumentException(directory + " doesn't exists");
         } else if(!directory.canWrite()) {
            throw new IllegalArgumentException(directory + " exists but is not writable");
         } else {
            this.directory = directory;
            return this;
         }
      }

      public CQLSSTableWriter.Builder withType(String typeDefinition) throws SyntaxException {
         this.typeStatements.add(QueryProcessor.parseStatement(typeDefinition, CreateTypeStatement.class, "CREATE TYPE"));
         return this;
      }

      public CQLSSTableWriter.Builder forTable(String schema) {
         this.schemaStatement = (CreateTableStatement.RawStatement)QueryProcessor.parseStatement(schema, CreateTableStatement.RawStatement.class, "CREATE TABLE");
         return this;
      }

      public CQLSSTableWriter.Builder withPartitioner(IPartitioner partitioner) {
         this.partitioner = partitioner;
         return this;
      }

      public CQLSSTableWriter.Builder using(String insert) {
         this.insertStatement = (ModificationStatement.Parsed)QueryProcessor.parseStatement(insert, ModificationStatement.Parsed.class, "INSERT/UPDATE");
         return this;
      }

      public CQLSSTableWriter.Builder withBufferSizeInMB(int size) {
         this.bufferSizeInMB = (long)size;
         return this;
      }

      public CQLSSTableWriter.Builder sorted() {
         this.sorted = true;
         return this;
      }

      public CQLSSTableWriter build() {
         if(this.directory == null) {
            throw new IllegalStateException("No ouptut directory specified, you should provide a directory with inDirectory()");
         } else if(this.schemaStatement == null) {
            throw new IllegalStateException("Missing schema, you should provide the schema for the SSTable to create with forTable()");
         } else if(this.insertStatement == null) {
            throw new IllegalStateException("No insert statement specified, you should provide an insert statement through using()");
         } else {
            Class var1 = CQLSSTableWriter.class;
            synchronized(CQLSSTableWriter.class) {
               if(Schema.instance.getKeyspaceMetadata("system_schema") == null) {
                  Schema.instance.load(SchemaKeyspace.metadata());
               }

               if(Schema.instance.getKeyspaceMetadata("system") == null) {
                  Schema.instance.load(SystemKeyspace.metadata());
               }

               String keyspaceName = this.schemaStatement.keyspace();
               if(Schema.instance.getKeyspaceMetadata(keyspaceName) == null) {
                  Schema.instance.load(KeyspaceMetadata.create(keyspaceName, KeyspaceParams.simple(1), Tables.none(), Views.none(), Types.none(), Functions.none()));
               }

               KeyspaceMetadata ksm = Schema.instance.getKeyspaceMetadata(keyspaceName);
               TableMetadata tableMetadata = ksm.tables.getNullable(this.schemaStatement.columnFamily());
               if(tableMetadata == null) {
                  Types types = this.createTypes(keyspaceName);
                  tableMetadata = this.createTable(types);
                  Schema.instance.load(ksm.withSwapped(ksm.tables.with(tableMetadata)).withSwapped(types));
               }

               Pair<UpdateStatement, List<ColumnSpecification>> preparedInsert = this.prepareInsert();
               TableMetadataRef ref = TableMetadataRef.forOfflineTools(tableMetadata);
               AbstractSSTableSimpleWriter writer = this.sorted?new SSTableSimpleWriter(this.directory, ref, ((UpdateStatement)preparedInsert.left).updatedColumns()):new SSTableSimpleUnsortedWriter(this.directory, ref, ((UpdateStatement)preparedInsert.left).updatedColumns(), this.bufferSizeInMB);
               if(this.formatType != null) {
                  ((AbstractSSTableSimpleWriter)writer).setSSTableFormatType(this.formatType);
               }

               return new CQLSSTableWriter((AbstractSSTableSimpleWriter)writer, (UpdateStatement)preparedInsert.left, (List)preparedInsert.right);
            }
         }
      }

      private Types createTypes(String keyspace) {
         Types.RawBuilder builder = Types.rawBuilder(keyspace);
         Iterator var3 = this.typeStatements.iterator();

         while(var3.hasNext()) {
            CreateTypeStatement st = (CreateTypeStatement)var3.next();
            st.addToRawBuilder(builder);
         }

         return builder.build();
      }

      private TableMetadata createTable(Types types) {
         CreateTableStatement statement = (CreateTableStatement)this.schemaStatement.prepare(types).statement;
         statement.validate(QueryState.forInternalCalls());
         TableMetadata.Builder builder = statement.builder();
         if(this.partitioner != null) {
            builder.partitioner(this.partitioner);
         }

         return builder.build();
      }

      private Pair<UpdateStatement, List<ColumnSpecification>> prepareInsert() {
         ParsedStatement.Prepared cqlStatement = this.insertStatement.prepare();
         UpdateStatement insert = (UpdateStatement)cqlStatement.statement;
         insert.validate(QueryState.forInternalCalls());
         if(insert.hasConditions()) {
            throw new IllegalArgumentException("Conditional statements are not supported");
         } else if(insert.isCounter()) {
            throw new IllegalArgumentException("Counter update statements are not supported");
         } else if(cqlStatement.boundNames.isEmpty()) {
            throw new IllegalArgumentException("Provided insert statement has no bind variables");
         } else {
            return Pair.create(insert, cqlStatement.boundNames);
         }
      }
   }
}
