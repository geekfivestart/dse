package org.apache.cassandra.auth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.function.Function;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.Versioned;

public class RoleInvalidation {
   public static Versioned<AuthVerbs.AuthVersion, Serializer<RoleInvalidation>> serializers = AuthVerbs.AuthVersion.versioned((v) -> {
      return new Serializer<RoleInvalidation>() {
         public void serialize(RoleInvalidation schema, DataOutputPlus out) throws IOException {
            out.writeInt(schema.roles.size());
            Iterator var3 = schema.roles.iterator();

            while(var3.hasNext()) {
               RoleResource role = (RoleResource)var3.next();
               ((RoleResource.RoleResourceSerializer)RoleResource.rawSerializers.get(v.encodingVersion)).serialize(role, out);
            }

         }

         public RoleInvalidation deserialize(DataInputPlus in) throws IOException {
            int count = in.readInt();
            Collection roles = new ArrayList(count);

            for(int i = 0; i < count; ++i) {
               roles.add(((RoleResource.RoleResourceSerializer)RoleResource.rawSerializers.get(v.encodingVersion)).deserialize(in));
            }

            return new RoleInvalidation(roles);
         }

         public long serializedSize(RoleInvalidation schema) {
            long size = (long)TypeSizes.sizeof(schema.roles.size());

            RoleResource role;
            for(Iterator var4 = schema.roles.iterator(); var4.hasNext(); size += ((RoleResource.RoleResourceSerializer)RoleResource.rawSerializers.get(v.encodingVersion)).serializedSize(role)) {
               role = (RoleResource)var4.next();
            }

            return size;
         }
      };
   });
   public final Collection<RoleResource> roles;

   RoleInvalidation(Collection<RoleResource> roles) {
      this.roles = roles;
   }
}
