package org.apache.cassandra.db;

import java.net.InetAddress;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.cassandra.cql3.UntypedResultSet;
import org.apache.cassandra.utils.CassandraVersion;

class PeerInfo {
   @Nullable
   String rack;
   @Nullable
   String dc;
   @Nullable
   CassandraVersion version;
   @Nullable
   UUID schemaVersion;
   @Nullable
   UUID hostId;
   @Nullable
   InetAddress preferredIp;

   PeerInfo() {
      this((UntypedResultSet.Row)null);
   }

   PeerInfo(UntypedResultSet.Row row) {
      if(row != null) {
         this.rack = row.has("rack")?row.getString("rack"):null;
         this.dc = row.has("data_center")?row.getString("data_center"):null;
         this.version = row.has("release_version")?new CassandraVersion(row.getString("release_version")):null;
         this.schemaVersion = row.has("schema_version")?row.getUUID("schema_version"):null;
         this.hostId = row.has("host_id")?row.getUUID("host_id"):null;
         this.preferredIp = row.has("preferred_ip")?row.getInetAddress("preferred_ip"):null;
      }
   }

   PeerInfo setRack(String rack) {
      this.rack = rack;
      return this;
   }

   PeerInfo setDc(String dc) {
      this.dc = dc;
      return this;
   }

   PeerInfo setVersion(CassandraVersion version) {
      this.version = version;
      return this;
   }

   PeerInfo setSchemaVersion(UUID schemaVersion) {
      this.schemaVersion = schemaVersion;
      return this;
   }

   PeerInfo setHostId(UUID hostId) {
      this.hostId = hostId;
      return this;
   }

   PeerInfo setPreferredIp(InetAddress preferredIp) {
      this.preferredIp = preferredIp;
      return this;
   }

   PeerInfo setValue(String name, Object value) {
      return name.equals("rack")?this.setRack((String)value):(name.equals("data_center")?this.setDc((String)value):(name.equals("release_version")?this.setVersion(new CassandraVersion((String)value)):(name.equals("schema_version")?this.setSchemaVersion((UUID)value):(name.equals("host_id")?this.setHostId((UUID)value):(name.equals("preferred_ip")?this.setPreferredIp((InetAddress)value):this)))));
   }
}
