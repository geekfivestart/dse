package org.apache.cassandra.schema;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.function.Function;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class SchemaMigration {
   public static Versioned<SchemaVerbs.SchemaVersion, Serializer<SchemaMigration>> serializers = SchemaVerbs.SchemaVersion.versioned((v) -> {
      return new Serializer<SchemaMigration>() {
         public void serialize(SchemaMigration schema, DataOutputPlus out) throws IOException {
            if(v.compareTo(SchemaVerbs.SchemaVersion.DSE_603) >= 0) {
               out.writeBoolean(schema.isCompatible);
            } else {
               assert schema.isCompatible;
            }

            if(schema.isCompatible) {
               out.writeInt(schema.mutations.size());
               Iterator var3 = schema.mutations.iterator();

               while(var3.hasNext()) {
                  Mutation mutation = (Mutation)var3.next();
                  ((Mutation.MutationSerializer)Mutation.rawSerializers.get(v.encodingVersion)).serialize(mutation, out);
               }
            }

         }

         public SchemaMigration deserialize(DataInputPlus in) throws IOException {
            boolean isCompatible = v.compareTo(SchemaVerbs.SchemaVersion.DSE_603) < 0 || in.readBoolean();
            if(!isCompatible) {
               return SchemaMigration.incompatible();
            } else {
               int count = in.readInt();
               Collection schema = new ArrayList(count);

               for(int i = 0; i < count; ++i) {
                  schema.add(((Mutation.MutationSerializer)Mutation.rawSerializers.get(v.encodingVersion)).deserialize(in));
               }

               return SchemaMigration.schema(schema);
            }
         }

         public long serializedSize(SchemaMigration schema) {
            long size = 0L;
            if(v.compareTo(SchemaVerbs.SchemaVersion.DSE_603) >= 0) {
               ++size;
            }

            if(schema.isCompatible) {
               size += (long)TypeSizes.sizeof(schema.mutations.size());

               Mutation mutation;
               for(Iterator var4 = schema.mutations.iterator(); var4.hasNext(); size += ((Mutation.MutationSerializer)Mutation.rawSerializers.get(v.encodingVersion)).serializedSize(mutation)) {
                  mutation = (Mutation)var4.next();
               }
            }

            return size;
         }
      };
   });
   public final boolean isCompatible;
   public final Collection<Mutation> mutations;

   private SchemaMigration(boolean isCompatible, Collection<Mutation> mutations) {
      this.isCompatible = isCompatible;
      this.mutations = mutations;
   }

   static SchemaMigration incompatible() {
      return new SchemaMigration(false, Collections.emptyList());
   }

   public static SchemaMigration schema(Collection<Mutation> schema) {
      return new SchemaMigration(true, schema);
   }
}
