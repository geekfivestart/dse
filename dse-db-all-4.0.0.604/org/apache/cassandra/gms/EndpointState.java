package org.apache.cassandra.gms;

import java.util.Collections;
import java.util.EnumMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.cassandra.utils.CassandraVersion;
import org.apache.cassandra.utils.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EndpointState {
   protected static final Logger logger = LoggerFactory.getLogger(EndpointState.class);
   public static final Serializer<EndpointState> serializer = new EndpointStateSerializer();
   private volatile HeartBeatState hbState;
   private final AtomicReference<Map<ApplicationState, VersionedValue>> applicationState;
   private volatile long updateTimestamp;
   private volatile boolean isAlive;

   EndpointState(HeartBeatState initialHbState) {
      this(initialHbState, new EnumMap(ApplicationState.class));
   }

   EndpointState(HeartBeatState initialHbState, Map<ApplicationState, VersionedValue> states) {
      this.hbState = initialHbState;
      this.applicationState = new AtomicReference(new EnumMap(states));
      this.updateTimestamp = System.nanoTime();
      this.isAlive = true;
   }

   HeartBeatState getHeartBeatState() {
      return this.hbState;
   }

   void setHeartBeatState(HeartBeatState newHbState) {
      this.updateTimestamp();
      this.hbState = newHbState;
   }

   public VersionedValue getApplicationState(ApplicationState key) {
      return (VersionedValue)((Map)this.applicationState.get()).get(key);
   }

   public Set<Entry<ApplicationState, VersionedValue>> states() {
      return ((Map)this.applicationState.get()).entrySet();
   }

   public void addApplicationState(ApplicationState key, VersionedValue value) {
      this.addApplicationStates(Collections.singletonMap(key, value));
   }

   public void addApplicationStates(Map<ApplicationState, VersionedValue> values) {
      this.addApplicationStates(values.entrySet());
   }

   public void addApplicationStates(Set<Entry<ApplicationState, VersionedValue>> values) {
      Map orig;
      EnumMap copy;
      do {
         orig = (Map)this.applicationState.get();
         copy = new EnumMap(orig);
         Iterator var4 = values.iterator();

         while(var4.hasNext()) {
            Entry<ApplicationState, VersionedValue> value = (Entry)var4.next();
            copy.put(value.getKey(), value.getValue());
         }
      } while(!this.applicationState.compareAndSet(orig, copy));

   }

   public long getUpdateTimestamp() {
      return this.updateTimestamp;
   }

   void updateTimestamp() {
      this.updateTimestamp = System.nanoTime();
   }

   public boolean isAlive() {
      return this.isAlive;
   }

   void markAlive() {
      this.isAlive = true;
   }

   void markDead() {
      this.isAlive = false;
   }

   public boolean isRpcReady() {
      VersionedValue rpcState = this.getApplicationState(ApplicationState.NATIVE_TRANSPORT_READY);
      return rpcState != null && Boolean.parseBoolean(rpcState.value);
   }

   public String getStatus() {
      VersionedValue status = this.getApplicationState(ApplicationState.STATUS);
      if(status == null) {
         return "";
      } else {
         String[] pieces = status.value.split(VersionedValue.DELIMITER_STR, -1);

         assert pieces.length > 0;

         return pieces[0];
      }
   }

   public UUID getSchemaVersion() {
      VersionedValue applicationState = this.getApplicationState(ApplicationState.SCHEMA);
      return applicationState != null?UUID.fromString(applicationState.value):null;
   }

   public CassandraVersion getReleaseVersion() {
      VersionedValue applicationState = this.getApplicationState(ApplicationState.RELEASE_VERSION);
      return applicationState != null?new CassandraVersion(applicationState.value):null;
   }

   public String toString() {
      return "EndpointState: HeartBeatState = " + this.hbState + ", AppStateMap = " + this.applicationState.get();
   }
}
