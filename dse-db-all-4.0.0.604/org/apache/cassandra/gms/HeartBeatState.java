package org.apache.cassandra.gms;

import org.apache.cassandra.utils.Serializer;

class HeartBeatState {
   public static final Serializer<HeartBeatState> serializer = new HeartBeatStateSerializer();
   private volatile int generation;
   private volatile int version;

   HeartBeatState(int gen) {
      this(gen, 0);
   }

   HeartBeatState(int gen, int ver) {
      this.generation = gen;
      this.version = ver;
   }

   int getGeneration() {
      return this.generation;
   }

   void updateHeartBeat() {
      this.version = VersionGenerator.getNextVersion();
   }

   int getHeartBeatVersion() {
      return this.version;
   }

   void forceNewerGenerationUnsafe() {
      ++this.generation;
   }

   void forceHighestPossibleVersionUnsafe() {
      this.version = 2147483647;
   }

   public String toString() {
      return String.format("HeartBeat: generation = %d, version = %d", new Object[]{Integer.valueOf(this.generation), Integer.valueOf(this.version)});
   }
}
