package org.apache.cassandra.cql3.functions;

import com.google.common.collect.UnmodifiableIterator;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.apache.cassandra.db.CBuilder;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.transport.ProtocolVersion;

public class TokenFct extends NativeScalarFunction {
   private final TableMetadata metadata;

   public TokenFct(TableMetadata metadata) {
      super("token", metadata.partitioner.getTokenValidator(), getKeyTypes(metadata));
      this.metadata = metadata;
   }

   public Arguments newArguments(ProtocolVersion version) {
      ArgumentDeserializer[] deserializers = new ArgumentDeserializer[this.argTypes.size()];
      Arrays.fill(deserializers, ArgumentDeserializer.NOOP_DESERIALIZER);
      return new FunctionArguments(version, deserializers);
   }

   private static AbstractType[] getKeyTypes(TableMetadata metadata) {
      AbstractType[] types = new AbstractType[metadata.partitionKeyColumns().size()];
      int i = 0;

      ColumnMetadata def;
      for(UnmodifiableIterator var3 = metadata.partitionKeyColumns().iterator(); var3.hasNext(); types[i++] = def.type) {
         def = (ColumnMetadata)var3.next();
      }

      return types;
   }

   public ByteBuffer execute(Arguments arguments) throws InvalidRequestException {
      CBuilder builder = CBuilder.create(this.metadata.partitionKeyAsClusteringComparator());
      int i = 0;

      for(int m = arguments.size(); i < m; ++i) {
         ByteBuffer bb = (ByteBuffer)arguments.get(i);
         if(bb == null) {
            return null;
         }

         builder.add(bb);
      }

      return this.metadata.partitioner.getTokenFactory().toByteArray(this.metadata.partitioner.getToken(builder.build().serializeAsPartitionKey()));
   }
}
