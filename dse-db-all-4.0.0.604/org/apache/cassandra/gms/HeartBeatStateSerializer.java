package org.apache.cassandra.gms;

import java.io.IOException;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.Serializer;

class HeartBeatStateSerializer implements Serializer<HeartBeatState> {
   HeartBeatStateSerializer() {
   }

   public void serialize(HeartBeatState hbState, DataOutputPlus out) throws IOException {
      out.writeInt(hbState.getGeneration());
      out.writeInt(hbState.getHeartBeatVersion());
   }

   public HeartBeatState deserialize(DataInputPlus in) throws IOException {
      return new HeartBeatState(in.readInt(), in.readInt());
   }

   public long serializedSize(HeartBeatState state) {
      return (long)(TypeSizes.sizeof(state.getGeneration()) + TypeSizes.sizeof(state.getHeartBeatVersion()));
   }
}
