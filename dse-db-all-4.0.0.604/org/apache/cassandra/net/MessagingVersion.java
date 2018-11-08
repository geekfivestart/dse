package org.apache.cassandra.net;

import com.datastax.bdp.db.nodesync.NodeSyncVerbs;
import java.util.EnumMap;
import org.apache.cassandra.auth.AuthVerbs;
import org.apache.cassandra.db.ReadVerbs;
import org.apache.cassandra.db.WriteVerbs;
import org.apache.cassandra.gms.GossipVerbs;
import org.apache.cassandra.hints.HintsVerbs;
import org.apache.cassandra.repair.messages.RepairVerbs;
import org.apache.cassandra.schema.SchemaVerbs;
import org.apache.cassandra.service.OperationsVerbs;
import org.apache.cassandra.service.paxos.LWTVerbs;
import org.apache.cassandra.utils.versioning.Version;

public enum MessagingVersion {
   OSS_30(ProtocolVersion.oss(10), ReadVerbs.ReadVersion.OSS_30, WriteVerbs.WriteVersion.OSS_30, LWTVerbs.LWTVersion.OSS_30, HintsVerbs.HintsVersion.OSS_30, OperationsVerbs.OperationsVersion.OSS_30, GossipVerbs.GossipVersion.OSS_30, (RepairVerbs.RepairVersion)null, SchemaVerbs.SchemaVersion.OSS_30, (AuthVerbs.AuthVersion)null, (NodeSyncVerbs.NodeSyncVersion)null),
   OSS_3014(ProtocolVersion.oss(11), ReadVerbs.ReadVersion.OSS_3014, WriteVerbs.WriteVersion.OSS_30, LWTVerbs.LWTVersion.OSS_30, HintsVerbs.HintsVersion.OSS_30, OperationsVerbs.OperationsVersion.OSS_30, GossipVerbs.GossipVersion.OSS_30, (RepairVerbs.RepairVersion)null, SchemaVerbs.SchemaVersion.OSS_30, (AuthVerbs.AuthVersion)null, (NodeSyncVerbs.NodeSyncVersion)null),
   OSS_40(ProtocolVersion.oss(12), ReadVerbs.ReadVersion.OSS_40, WriteVerbs.WriteVersion.OSS_30, LWTVerbs.LWTVersion.OSS_30, HintsVerbs.HintsVersion.OSS_30, OperationsVerbs.OperationsVersion.OSS_30, GossipVerbs.GossipVersion.OSS_30, RepairVerbs.RepairVersion.OSS_40, SchemaVerbs.SchemaVersion.OSS_30, (AuthVerbs.AuthVersion)null, (NodeSyncVerbs.NodeSyncVersion)null),
   DSE_60(ProtocolVersion.dse(1), ReadVerbs.ReadVersion.DSE_60, WriteVerbs.WriteVersion.OSS_30, LWTVerbs.LWTVersion.OSS_30, HintsVerbs.HintsVersion.OSS_30, OperationsVerbs.OperationsVersion.DSE_60, GossipVerbs.GossipVersion.OSS_30, RepairVerbs.RepairVersion.DSE_60, SchemaVerbs.SchemaVersion.OSS_30, AuthVerbs.AuthVersion.DSE_60, (NodeSyncVerbs.NodeSyncVersion)null),
   DSE_603(ProtocolVersion.dse(2), ReadVerbs.ReadVersion.DSE_60, WriteVerbs.WriteVersion.OSS_30, LWTVerbs.LWTVersion.OSS_30, HintsVerbs.HintsVersion.OSS_30, OperationsVerbs.OperationsVersion.DSE_60, GossipVerbs.GossipVersion.OSS_30, RepairVerbs.RepairVersion.DSE_60, SchemaVerbs.SchemaVersion.DSE_603, AuthVerbs.AuthVersion.DSE_60, NodeSyncVerbs.NodeSyncVersion.DSE_603);

   private final ProtocolVersion protocolVersion;
   private final EnumMap<Verbs.Group, Version<?>> groupVersions;

   private MessagingVersion(ProtocolVersion version, ReadVerbs.ReadVersion readVersion, WriteVerbs.WriteVersion writeVersion, LWTVerbs.LWTVersion lwtVersion, HintsVerbs.HintsVersion hintsVersion, OperationsVerbs.OperationsVersion operationsVersion, GossipVerbs.GossipVersion gossipVersion, RepairVerbs.RepairVersion repairVersion, SchemaVerbs.SchemaVersion schemaVersion, AuthVerbs.AuthVersion authVersion, NodeSyncVerbs.NodeSyncVersion nodeSyncVersion) {
      this.protocolVersion = version;
      this.groupVersions = new EnumMap(Verbs.Group.class);
      this.groupVersions.put(Verbs.Group.READS, readVersion);
      this.groupVersions.put(Verbs.Group.WRITES, writeVersion);
      this.groupVersions.put(Verbs.Group.LWT, lwtVersion);
      this.groupVersions.put(Verbs.Group.HINTS, hintsVersion);
      this.groupVersions.put(Verbs.Group.OPERATIONS, operationsVersion);
      this.groupVersions.put(Verbs.Group.GOSSIP, gossipVersion);
      this.groupVersions.put(Verbs.Group.REPAIR, repairVersion);
      this.groupVersions.put(Verbs.Group.SCHEMA, schemaVersion);
      this.groupVersions.put(Verbs.Group.AUTH, authVersion);
      this.groupVersions.put(Verbs.Group.NODESYNC, nodeSyncVersion);
   }

   public boolean isDSE() {
      return this.protocolVersion.isDSE;
   }

   public ProtocolVersion protocolVersion() {
      return this.protocolVersion;
   }

   public static MessagingVersion min(MessagingVersion v1, MessagingVersion v2) {
      return v1.compareTo(v2) < 0?v1:v2;
   }

   public static MessagingVersion from(ProtocolVersion protocolVersion) {
      MessagingVersion previous = null;
      MessagingVersion[] var2 = values();
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         MessagingVersion version = var2[var4];
         int cmp = version.protocolVersion.compareTo(protocolVersion);
         if(cmp == 0) {
            return version;
         }

         if(cmp > 0) {
            return previous;
         }

         previous = version;
      }

      return previous;
   }

   public static MessagingVersion fromHandshakeVersion(int handshakeVersion) {
      return from(ProtocolVersion.fromHandshakeVersion(handshakeVersion));
   }

   public <V extends Enum<V> & Version<V>> V groupVersion(Verbs.Group groupId) {
      return (Enum)this.groupVersions.get(groupId);
   }

   public <P, Q, V extends Enum<V> & Version<V>> VerbSerializer<P, Q> serializer(Verb<P, Q> verb) {
      VerbGroup<V> group = verb.group();
      V version = this.groupVersion(verb.group().id());
      if(version == null) {
         throw new IllegalArgumentException(group.getUnsupportedVersionMessage(this));
      } else {
         return group.forVersion(version).getByVerb(verb);
      }
   }

   public <P, Q, V extends Enum<V> & Version<V>> VerbSerializer<P, Q> serializerByVerbCode(VerbGroup<V> group, int code) {
      V version = this.groupVersion(group.id());
      if(version == null) {
         throw new IllegalArgumentException(group.getUnsupportedVersionMessage(this));
      } else {
         VerbSerializer<P, Q> serializer = group.forVersion(version).getByCode(code);
         if(serializer == null) {
            throw new IllegalArgumentException(String.format("Invalid verb code %d for group %s at version %s", new Object[]{Integer.valueOf(code), group, this}));
         } else {
            return serializer;
         }
      }
   }
}
