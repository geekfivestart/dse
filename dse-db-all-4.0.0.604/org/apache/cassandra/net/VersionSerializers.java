package org.apache.cassandra.net;

import com.carrotsearch.hppc.IntObjectHashMap;
import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.Version;

class VersionSerializers {
   private final String groupName;
   private final Version<?> version;
   private final List<VerbSerializer> verbSerializers = new ArrayList();
   private final IntObjectHashMap<VerbSerializer> codeToVerb = new IntObjectHashMap();

   VersionSerializers(String groupName, Version<?> version) {
      this.groupName = groupName;
      this.version = version;
   }

   <P, Q> void add(Verb<P, Q> verb, int code, Serializer<P> requestSerializer, Serializer<Q> responseSerializer) {
      VerbSerializer<P, Q> serializer = new VerbSerializer(verb, code, requestSerializer, responseSerializer);

      assert verb.groupIdx() == this.verbSerializers.size();

      this.verbSerializers.add(serializer);
      this.codeToVerb.put(code, serializer);
   }

   <P, Q> VerbSerializer<P, Q> getByCode(int code) {
      VerbSerializer<P, Q> serializer = (VerbSerializer)this.codeToVerb.get(code);
      if(serializer == null) {
         throw new IllegalArgumentException(String.format("Invalid verb code %d for %s in version %s ", new Object[]{Integer.valueOf(code), this.groupName, this.version}));
      } else {
         return serializer;
      }
   }

   <P, Q> VerbSerializer<P, Q> getByVerb(Verb<P, Q> verb) {
      return (VerbSerializer)this.verbSerializers.get(verb.groupIdx());
   }
}
