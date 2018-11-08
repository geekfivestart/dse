package org.apache.cassandra.gms;

import java.io.IOException;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Serializer;

class EndpointStateSerializer implements Serializer<EndpointState> {
   EndpointStateSerializer() {
   }

   public void serialize(EndpointState epState, DataOutputPlus out) throws IOException {
      HeartBeatState hbState = epState.getHeartBeatState();
      HeartBeatState.serializer.serialize(hbState, out);
      Set<Entry<ApplicationState, VersionedValue>> states = epState.states();
      out.writeInt(states.size());
      Iterator var5 = states.iterator();

      while(var5.hasNext()) {
         Entry<ApplicationState, VersionedValue> state = (Entry)var5.next();
         VersionedValue value = (VersionedValue)state.getValue();
         out.writeInt(((ApplicationState)state.getKey()).ordinal());
         VersionedValue.serializer.serialize(value, out);
      }

   }

   public EndpointState deserialize(DataInputPlus in) throws IOException {
      HeartBeatState hbState = (HeartBeatState)HeartBeatState.serializer.deserialize(in);
      int appStateSize = in.readInt();
      Map<ApplicationState, VersionedValue> states = new EnumMap(ApplicationState.class);

      for(int i = 0; i < appStateSize; ++i) {
         int key = in.readInt();
         VersionedValue value = (VersionedValue)VersionedValue.serializer.deserialize(in);
         states.put(Gossiper.STATES[key], value);
      }

      return new EndpointState(hbState, states);
   }

   public long serializedSize(EndpointState epState) {
      long size = HeartBeatState.serializer.serializedSize(epState.getHeartBeatState());
      Set<Entry<ApplicationState, VersionedValue>> states = epState.states();
      size += (long)TypeSizes.sizeof(states.size());

      VersionedValue value;
      for(Iterator var5 = states.iterator(); var5.hasNext(); size += VersionedValue.serializer.serializedSize(value)) {
         Entry<ApplicationState, VersionedValue> state = (Entry)var5.next();
         value = (VersionedValue)state.getValue();
         size += (long)TypeSizes.sizeof(((ApplicationState)state.getKey()).ordinal());
      }

      return size;
   }
}
