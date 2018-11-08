package org.apache.cassandra.transport;

import io.netty.buffer.ByteBuf;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

public abstract class Event {
   public final Event.Type type;

   private Event(Event.Type type) {
      this.type = type;
   }

   public static Event deserialize(ByteBuf cb, ProtocolVersion version) {
      Event.Type eventType = (Event.Type)CBUtil.readEnumValue(Event.Type.class, cb);
      if(eventType.minimumVersion.isGreaterThan(version)) {
         throw new ProtocolException("Event " + eventType.name() + " not valid for protocol version " + version);
      } else {
         switch(null.$SwitchMap$org$apache$cassandra$transport$Event$Type[eventType.ordinal()]) {
         case 1:
            return Event.TopologyChange.deserializeEvent(cb, version);
         case 2:
            return Event.StatusChange.deserializeEvent(cb, version);
         case 3:
            return Event.SchemaChange.deserializeEvent(cb, version);
         default:
            throw new AssertionError();
         }
      }
   }

   public void serialize(ByteBuf dest, ProtocolVersion version) {
      if(this.type.minimumVersion.isGreaterThan(version)) {
         throw new ProtocolException("Event " + this.type.name() + " not valid for protocol version " + version);
      } else {
         CBUtil.writeEnumValue(this.type, dest);
         this.serializeEvent(dest, version);
      }
   }

   public int serializedSize(ProtocolVersion version) {
      return CBUtil.sizeOfEnumValue(this.type) + this.eventSerializedSize(version);
   }

   protected abstract void serializeEvent(ByteBuf var1, ProtocolVersion var2);

   protected abstract int eventSerializedSize(ProtocolVersion var1);

   public static class SchemaChange extends Event {
      public static final Event.SchemaChange NONE;
      public final Event.SchemaChange.Change change;
      public final Event.SchemaChange.Target target;
      public final String keyspace;
      public final String name;
      public final List<String> argTypes;

      public SchemaChange(Event.SchemaChange.Change change, Event.SchemaChange.Target target, String keyspace, String name, List<String> argTypes) {
         super(Event.Type.SCHEMA_CHANGE, null);
         this.change = change;
         this.target = target;
         this.keyspace = keyspace;
         this.name = name;

         assert target == Event.SchemaChange.Target.KEYSPACE || this.name != null : "Table, type, function or aggregate name should be set for non-keyspace schema change events";

         this.argTypes = argTypes;
      }

      public SchemaChange(Event.SchemaChange.Change change, Event.SchemaChange.Target target, String keyspace, String name) {
         this(change, target, keyspace, name, (List)null);
      }

      public SchemaChange(Event.SchemaChange.Change change, String keyspace) {
         this(change, Event.SchemaChange.Target.KEYSPACE, keyspace, (String)null);
      }

      public static Event.SchemaChange deserializeEvent(ByteBuf cb, ProtocolVersion version) {
         Event.SchemaChange.Change change = (Event.SchemaChange.Change)CBUtil.readEnumValue(Event.SchemaChange.Change.class, cb);
         String keyspace;
         if(version.isGreaterOrEqualTo(ProtocolVersion.V3)) {
            Event.SchemaChange.Target target = (Event.SchemaChange.Target)CBUtil.readEnumValue(Event.SchemaChange.Target.class, cb);
            keyspace = CBUtil.readString(cb);
            String tableOrType = target == Event.SchemaChange.Target.KEYSPACE?null:CBUtil.readString(cb);
            List<String> argTypes = null;
            if(target == Event.SchemaChange.Target.FUNCTION || target == Event.SchemaChange.Target.AGGREGATE) {
               argTypes = CBUtil.readStringList(cb);
            }

            return new Event.SchemaChange(change, target, keyspace, tableOrType, argTypes);
         } else {
            String keyspace = CBUtil.readString(cb);
            keyspace = CBUtil.readString(cb);
            return new Event.SchemaChange(change, keyspace.isEmpty()?Event.SchemaChange.Target.KEYSPACE:Event.SchemaChange.Target.TABLE, keyspace, keyspace.isEmpty()?null:keyspace);
         }
      }

      public void serializeEvent(ByteBuf dest, ProtocolVersion version) {
         if(this.target != Event.SchemaChange.Target.FUNCTION && this.target != Event.SchemaChange.Target.AGGREGATE) {
            if(version.isGreaterOrEqualTo(ProtocolVersion.V3)) {
               CBUtil.writeEnumValue(this.change, dest);
               CBUtil.writeEnumValue(this.target, dest);
               CBUtil.writeString(this.keyspace, dest);
               if(this.target != Event.SchemaChange.Target.KEYSPACE) {
                  CBUtil.writeString(this.name, dest);
               }
            } else if(this.target == Event.SchemaChange.Target.TYPE) {
               CBUtil.writeEnumValue(Event.SchemaChange.Change.UPDATED, dest);
               CBUtil.writeString(this.keyspace, dest);
               CBUtil.writeString("", dest);
            } else {
               CBUtil.writeEnumValue(this.change, dest);
               CBUtil.writeString(this.keyspace, dest);
               CBUtil.writeString(this.target == Event.SchemaChange.Target.KEYSPACE?"":this.name, dest);
            }

         } else {
            if(version.isGreaterOrEqualTo(ProtocolVersion.V4)) {
               CBUtil.writeEnumValue(this.change, dest);
               CBUtil.writeEnumValue(this.target, dest);
               CBUtil.writeString(this.keyspace, dest);
               CBUtil.writeString(this.name, dest);
               CBUtil.writeStringList(this.argTypes, dest);
            } else {
               CBUtil.writeEnumValue(Event.SchemaChange.Change.UPDATED, dest);
               if(version.isGreaterOrEqualTo(ProtocolVersion.V3)) {
                  CBUtil.writeEnumValue(Event.SchemaChange.Target.KEYSPACE, dest);
               }

               CBUtil.writeString(this.keyspace, dest);
               CBUtil.writeString("", dest);
            }

         }
      }

      public int eventSerializedSize(ProtocolVersion version) {
         if(this.target != Event.SchemaChange.Target.FUNCTION && this.target != Event.SchemaChange.Target.AGGREGATE) {
            if(version.isGreaterOrEqualTo(ProtocolVersion.V3)) {
               int size = CBUtil.sizeOfEnumValue(this.change) + CBUtil.sizeOfEnumValue(this.target) + CBUtil.sizeOfString(this.keyspace);
               if(this.target != Event.SchemaChange.Target.KEYSPACE) {
                  size += CBUtil.sizeOfString(this.name);
               }

               return size;
            } else {
               return this.target == Event.SchemaChange.Target.TYPE?CBUtil.sizeOfEnumValue(Event.SchemaChange.Change.UPDATED) + CBUtil.sizeOfString(this.keyspace) + CBUtil.sizeOfString(""):CBUtil.sizeOfEnumValue(this.change) + CBUtil.sizeOfString(this.keyspace) + CBUtil.sizeOfString(this.target == Event.SchemaChange.Target.KEYSPACE?"":this.name);
            }
         } else {
            return version.isGreaterOrEqualTo(ProtocolVersion.V4)?CBUtil.sizeOfEnumValue(this.change) + CBUtil.sizeOfEnumValue(this.target) + CBUtil.sizeOfString(this.keyspace) + CBUtil.sizeOfString(this.name) + CBUtil.sizeOfStringList(this.argTypes):(version.isGreaterOrEqualTo(ProtocolVersion.V3)?CBUtil.sizeOfEnumValue(Event.SchemaChange.Change.UPDATED) + CBUtil.sizeOfEnumValue(Event.SchemaChange.Target.KEYSPACE) + CBUtil.sizeOfString(this.keyspace):CBUtil.sizeOfEnumValue(Event.SchemaChange.Change.UPDATED) + CBUtil.sizeOfString(this.keyspace) + CBUtil.sizeOfString(""));
         }
      }

      public String toString() {
         StringBuilder sb = (new StringBuilder()).append(this.change).append(' ').append(this.target).append(' ').append(this.keyspace);
         if(this.name != null) {
            sb.append('.').append(this.name);
         }

         if(this.argTypes != null) {
            sb.append(" (");
            Iterator iter = this.argTypes.iterator();

            while(iter.hasNext()) {
               sb.append((String)iter.next());
               if(iter.hasNext()) {
                  sb.append(',');
               }
            }

            sb.append(')');
         }

         return sb.toString();
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.change, this.target, this.keyspace, this.name, this.argTypes});
      }

      public boolean equals(Object other) {
         if(!(other instanceof Event.SchemaChange)) {
            return false;
         } else {
            Event.SchemaChange scc = (Event.SchemaChange)other;
            return Objects.equals(this.change, scc.change) && Objects.equals(this.target, scc.target) && Objects.equals(this.keyspace, scc.keyspace) && Objects.equals(this.name, scc.name) && Objects.equals(this.argTypes, scc.argTypes);
         }
      }

      static {
         NONE = new Event.SchemaChange(Event.SchemaChange.Change.CREATED, "");
      }

      public static enum Target {
         KEYSPACE,
         TABLE,
         TYPE,
         FUNCTION,
         AGGREGATE;

         private Target() {
         }
      }

      public static enum Change {
         CREATED,
         UPDATED,
         DROPPED;

         private Change() {
         }
      }
   }

   public static class StatusChange extends Event.NodeEvent {
      public final Event.StatusChange.Status status;

      private StatusChange(Event.StatusChange.Status status, InetSocketAddress node) {
         super(Event.Type.STATUS_CHANGE, node, null);
         this.status = status;
      }

      public static Event.StatusChange nodeUp(InetAddress host, int port) {
         return new Event.StatusChange(Event.StatusChange.Status.UP, new InetSocketAddress(host, port));
      }

      public static Event.StatusChange nodeDown(InetAddress host, int port) {
         return new Event.StatusChange(Event.StatusChange.Status.DOWN, new InetSocketAddress(host, port));
      }

      private static Event.StatusChange deserializeEvent(ByteBuf cb, ProtocolVersion version) {
         Event.StatusChange.Status status = (Event.StatusChange.Status)CBUtil.readEnumValue(Event.StatusChange.Status.class, cb);
         InetSocketAddress node = CBUtil.readInet(cb);
         return new Event.StatusChange(status, node);
      }

      protected void serializeEvent(ByteBuf dest, ProtocolVersion version) {
         CBUtil.writeEnumValue(this.status, dest);
         CBUtil.writeInet(this.node, dest);
      }

      protected int eventSerializedSize(ProtocolVersion version) {
         return CBUtil.sizeOfEnumValue(this.status) + CBUtil.sizeOfInet(this.node);
      }

      public String toString() {
         return this.status + " " + this.node;
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.status, this.node});
      }

      public boolean equals(Object other) {
         if(!(other instanceof Event.StatusChange)) {
            return false;
         } else {
            Event.StatusChange stc = (Event.StatusChange)other;
            return Objects.equals(this.status, stc.status) && Objects.equals(this.node, stc.node);
         }
      }

      public static enum Status {
         UP,
         DOWN;

         private Status() {
         }
      }
   }

   public static class TopologyChange extends Event.NodeEvent {
      public final Event.TopologyChange.Change change;

      private TopologyChange(Event.TopologyChange.Change change, InetSocketAddress node) {
         super(Event.Type.TOPOLOGY_CHANGE, node, null);
         this.change = change;
      }

      public static Event.TopologyChange newNode(InetAddress host, int port) {
         return new Event.TopologyChange(Event.TopologyChange.Change.NEW_NODE, new InetSocketAddress(host, port));
      }

      public static Event.TopologyChange removedNode(InetAddress host, int port) {
         return new Event.TopologyChange(Event.TopologyChange.Change.REMOVED_NODE, new InetSocketAddress(host, port));
      }

      public static Event.TopologyChange movedNode(InetAddress host, int port) {
         return new Event.TopologyChange(Event.TopologyChange.Change.MOVED_NODE, new InetSocketAddress(host, port));
      }

      private static Event.TopologyChange deserializeEvent(ByteBuf cb, ProtocolVersion version) {
         Event.TopologyChange.Change change = (Event.TopologyChange.Change)CBUtil.readEnumValue(Event.TopologyChange.Change.class, cb);
         InetSocketAddress node = CBUtil.readInet(cb);
         return new Event.TopologyChange(change, node);
      }

      protected void serializeEvent(ByteBuf dest, ProtocolVersion version) {
         CBUtil.writeEnumValue(this.change, dest);
         CBUtil.writeInet(this.node, dest);
      }

      protected int eventSerializedSize(ProtocolVersion version) {
         return CBUtil.sizeOfEnumValue(this.change) + CBUtil.sizeOfInet(this.node);
      }

      public String toString() {
         return this.change + " " + this.node;
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.change, this.node});
      }

      public boolean equals(Object other) {
         if(!(other instanceof Event.TopologyChange)) {
            return false;
         } else {
            Event.TopologyChange tpc = (Event.TopologyChange)other;
            return Objects.equals(this.change, tpc.change) && Objects.equals(this.node, tpc.node);
         }
      }

      public static enum Change {
         NEW_NODE,
         REMOVED_NODE,
         MOVED_NODE;

         private Change() {
         }
      }
   }

   public abstract static class NodeEvent extends Event {
      public final InetSocketAddress node;

      public InetAddress nodeAddress() {
         return this.node.getAddress();
      }

      private NodeEvent(Event.Type type, InetSocketAddress node) {
         super(type, null);
         this.node = node;
      }
   }

   public static enum Type {
      TOPOLOGY_CHANGE(ProtocolVersion.V3),
      STATUS_CHANGE(ProtocolVersion.V3),
      SCHEMA_CHANGE(ProtocolVersion.V3),
      TRACE_COMPLETE(ProtocolVersion.V4);

      public final ProtocolVersion minimumVersion;

      private Type(ProtocolVersion minimumVersion) {
         this.minimumVersion = minimumVersion;
      }
   }
}
