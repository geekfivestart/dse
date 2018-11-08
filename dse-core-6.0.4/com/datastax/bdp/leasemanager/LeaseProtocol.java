package com.datastax.bdp.leasemanager;

import com.datastax.bdp.node.transport.MessageBodySerializer;
import com.datastax.bdp.node.transport.MessageType;
import com.datastax.bdp.node.transport.internode.InternodeProtocol;
import com.datastax.bdp.node.transport.internode.InternodeProtocolRegistry;
import com.datastax.bdp.util.LambdaMayThrow;
import com.google.inject.Inject;
import com.google.inject.Provider;
import com.google.inject.Singleton;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class LeaseProtocol implements InternodeProtocol {
   private static final Logger LOGGER = LoggerFactory.getLogger(LeaseProtocol.class);
   public static final MessageType CLIENT_PING;
   public static final MessageType CLIENT_PING_RESULT;
   public static final MessageType CREATE_LEASE;
   public static final MessageType DISABLE_LEASE;
   public static final MessageType DELETE_LEASE;
   public static final MessageType LEASE_DURATION;
   public static final MessageType BOOLEAN_RESPONSE;
   public static final MessageType INTEGER_RESPONSE;
   private final Provider<LeasePlugin> leasePlugin;

   @Inject
   public LeaseProtocol(Provider<LeasePlugin> leasePlugin) {
      this.leasePlugin = leasePlugin;
   }

   public void register(InternodeProtocolRegistry registry) {
      registry.addSerializer(CLIENT_PING, new LeaseProtocol.ClientPingSerializer(), new byte[]{1});
      registry.addSerializer(CLIENT_PING_RESULT, new LeaseProtocol.ClientPingResultSerializer(), new byte[]{1});
      registry.addSerializer(CREATE_LEASE, new LeaseProtocol.CreateLeaseSerializer(), new byte[]{1});
      registry.addSerializer(DISABLE_LEASE, new LeaseProtocol.DisableLeaseSerializer(), new byte[]{1});
      registry.addSerializer(DELETE_LEASE, new LeaseProtocol.DeleteLeaseSerializer(), new byte[]{1});
      registry.addSerializer(LEASE_DURATION, new LeaseProtocol.LeaseDurationSerializer(), new byte[]{1});
      registry.addSerializer(BOOLEAN_RESPONSE, new LeaseProtocol.BooleanResponseSerializer(), new byte[]{1});
      registry.addProcessor(CLIENT_PING, CLIENT_PING_RESULT, (msg) -> {
         return ((LeasePlugin)this.leasePlugin.get()).getResources().getManager().clientPing(msg.name, msg.dc, msg.client, msg.takeIfOpen);
      });
      registry.addProcessor(CREATE_LEASE, BOOLEAN_RESPONSE, (msg) -> {
         return Boolean.valueOf(((LeasePlugin)this.leasePlugin.get()).getResources().getManager().createLease(msg.name, msg.dc, msg.duration_ms));
      });
      registry.addProcessor(DISABLE_LEASE, BOOLEAN_RESPONSE, (msg) -> {
         return Boolean.valueOf(((LeasePlugin)this.leasePlugin.get()).getResources().getManager().disableLease(msg.name, msg.dc));
      });
      registry.addProcessor(DELETE_LEASE, BOOLEAN_RESPONSE, (msg) -> {
         return Boolean.valueOf(((LeasePlugin)this.leasePlugin.get()).getResources().getManager().deleteLease(msg.name, msg.dc));
      });
      registry.addProcessor(LEASE_DURATION, INTEGER_RESPONSE, (msg) -> {
         return ((LeasePlugin)this.leasePlugin.get()).getResources().getManager().getLeaseDuration(msg.name, msg.dc);
      });
      LOGGER.info("Registered LeaseProtocol");
   }

   public static void serializeInetAddress(DataOutputStream out, InetAddress addr) throws IOException {
      if(addr == null) {
         out.writeByte(0);
      } else {
         byte[] octets = addr.getAddress();
         out.writeByte(octets.length);
         out.write(octets);
      }

   }

   public static InetAddress deserializeInetAddress(DataInputStream in) throws IOException {
      int len = in.readByte();
      if(len == 0) {
         return null;
      } else {
         byte[] octets = new byte[len];
         in.read(octets);
         return InetAddress.getByAddress(octets);
      }
   }

   static {
      CLIENT_PING = MessageType.of(MessageType.Domain.LEASE, 1);
      CLIENT_PING_RESULT = MessageType.of(MessageType.Domain.LEASE, 2);
      CREATE_LEASE = MessageType.of(MessageType.Domain.LEASE, 3);
      DISABLE_LEASE = MessageType.of(MessageType.Domain.LEASE, 4);
      DELETE_LEASE = MessageType.of(MessageType.Domain.LEASE, 5);
      LEASE_DURATION = MessageType.of(MessageType.Domain.LEASE, 6);
      BOOLEAN_RESPONSE = MessageType.of(MessageType.Domain.LEASE, 7);
      INTEGER_RESPONSE = MessageType.of(MessageType.Domain.LEASE, 8);
   }

   public static class IntegerResponseSerializer implements MessageBodySerializer<Integer> {
      public IntegerResponseSerializer() {
      }

      public void serialize(Integer body, OutputStream stream) throws IOException {
         DataOutputStream out = new DataOutputStream(stream);
         out.writeInt(body.intValue());
      }

      public Integer deserialize(InputStream stream) throws IOException {
         DataInputStream in = new DataInputStream(stream);
         return new Integer(in.readInt());
      }
   }

   public static class BooleanResponseSerializer implements MessageBodySerializer<Boolean> {
      public BooleanResponseSerializer() {
      }

      public void serialize(Boolean body, OutputStream stream) throws IOException {
         DataOutputStream out = new DataOutputStream(stream);
         out.writeBoolean(body.booleanValue());
      }

      public Boolean deserialize(InputStream stream) throws IOException {
         DataInputStream in = new DataInputStream(stream);
         return new Boolean(in.readBoolean());
      }
   }

   public static class LeaseDurationSerializer extends LeaseProtocol.LeaseIdSerializer<LeaseProtocol.LeaseDurationMessage> {
      public LeaseDurationSerializer() {
      }

      public LeaseProtocol.LeaseDurationMessage constructor(String name, String dc) {
         return new LeaseProtocol.LeaseDurationMessage(name, dc);
      }
   }

   public static class LeaseDurationMessage extends LeaseProtocol.LeaseRequest {
      public LeaseDurationMessage(String name, String dc) {
         super(LeaseProtocol.LEASE_DURATION, name, dc);
      }
   }

   public static class DisableLeaseSerializer extends LeaseProtocol.LeaseIdSerializer<LeaseProtocol.DisableLeaseMessage> {
      public DisableLeaseSerializer() {
      }

      public LeaseProtocol.DisableLeaseMessage constructor(String name, String dc) {
         return new LeaseProtocol.DisableLeaseMessage(name, dc);
      }
   }

   public static class DisableLeaseMessage extends LeaseProtocol.LeaseRequest {
      public DisableLeaseMessage(String name, String dc) {
         super(LeaseProtocol.DISABLE_LEASE, name, dc);
      }
   }

   public static class DeleteLeaseSerializer extends LeaseProtocol.LeaseIdSerializer<LeaseProtocol.DeleteLeaseMessage> {
      public DeleteLeaseSerializer() {
      }

      public LeaseProtocol.DeleteLeaseMessage constructor(String name, String dc) {
         return new LeaseProtocol.DeleteLeaseMessage(name, dc);
      }
   }

   public static class DeleteLeaseMessage extends LeaseProtocol.LeaseRequest {
      public DeleteLeaseMessage(String name, String dc) {
         super(LeaseProtocol.DELETE_LEASE, name, dc);
      }
   }

   public abstract static class LeaseIdSerializer<T extends LeaseMonitorCore.LeaseId> implements MessageBodySerializer<T> {
      public LeaseIdSerializer() {
      }

      public abstract T constructor(String var1, String var2);

      public void serialize(T body, OutputStream stream) throws IOException {
         DataOutputStream out = new DataOutputStream(stream);
         out.writeUTF(body.name);
         out.writeUTF(body.dc);
      }

      public T deserialize(InputStream stream) throws IOException {
         DataInputStream in = new DataInputStream(stream);
         return this.constructor(in.readUTF(), in.readUTF());
      }
   }

   public static class CreateLeaseSerializer implements MessageBodySerializer<LeaseProtocol.CreateLeaseMessage> {
      public CreateLeaseSerializer() {
      }

      public void serialize(LeaseProtocol.CreateLeaseMessage body, OutputStream stream) throws IOException {
         DataOutputStream out = new DataOutputStream(stream);
         out.writeUTF(body.name);
         out.writeUTF(body.dc);
         out.writeInt(body.duration_ms);
      }

      public LeaseProtocol.CreateLeaseMessage deserialize(InputStream stream) throws IOException {
         DataInputStream in = new DataInputStream(stream);
         return new LeaseProtocol.CreateLeaseMessage(in.readUTF(), in.readUTF(), in.readInt());
      }
   }

   public static class CreateLeaseMessage extends LeaseProtocol.LeaseRequest {
      public final int duration_ms;

      public CreateLeaseMessage(String name, String dc, int duration_ms) {
         super(LeaseProtocol.CREATE_LEASE, name, dc);
         this.duration_ms = duration_ms;
      }

      public boolean equals(Object obj) {
         if(obj != null && obj instanceof LeaseProtocol.CreateLeaseMessage) {
            LeaseProtocol.CreateLeaseMessage other = (LeaseProtocol.CreateLeaseMessage)obj;
            return this.type.equals(other.type) && this.name.equals(other.name) && this.dc.equals(other.dc) && this.duration_ms == other.duration_ms;
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.name, this.dc, Integer.valueOf(this.duration_ms)});
      }
   }

   public static class ClientPingResultSerializer implements MessageBodySerializer<LeaseMonitor.ClientPingResult> {
      public ClientPingResultSerializer() {
      }

      public void serialize(LeaseMonitor.ClientPingResult body, OutputStream stream) throws IOException {
         DataOutputStream out = new DataOutputStream(stream);
         LeaseProtocol.serializeInetAddress(out, body.holder);
         out.writeLong(body.leaseTimeRemaining);
      }

      public LeaseMonitor.ClientPingResult deserialize(InputStream stream) throws IOException {
         DataInputStream in = new DataInputStream(stream);
         return new LeaseMonitor.ClientPingResult(LeaseProtocol.deserializeInetAddress(in), in.readLong());
      }
   }

   public static class ClientPingSerializer implements MessageBodySerializer<LeaseProtocol.ClientPingMessage> {
      public ClientPingSerializer() {
      }

      public void serialize(LeaseProtocol.ClientPingMessage body, OutputStream stream) throws IOException {
         DataOutputStream out = new DataOutputStream(stream);
         out.writeUTF(body.name);
         out.writeUTF(body.dc);
         LeaseProtocol.serializeInetAddress(out, body.client);
         out.writeBoolean(body.takeIfOpen);
      }

      public LeaseProtocol.ClientPingMessage deserialize(InputStream stream) throws IOException {
         DataInputStream in = new DataInputStream(stream);
         return new LeaseProtocol.ClientPingMessage(in.readUTF(), in.readUTF(), LeaseProtocol.deserializeInetAddress(in), in.readBoolean());
      }
   }

   public static class ClientPingMessage extends LeaseProtocol.LeaseRequest {
      public final InetAddress client;
      public final boolean takeIfOpen;

      ClientPingMessage(String name, String dc, InetAddress client, boolean takeIfOpen) {
         super(LeaseProtocol.CLIENT_PING, name, dc);
         this.client = client;
         this.takeIfOpen = takeIfOpen;
      }

      public boolean equals(Object obj) {
         if(obj != null && obj instanceof LeaseProtocol.ClientPingMessage) {
            LeaseProtocol.ClientPingMessage other = (LeaseProtocol.ClientPingMessage)obj;
            return this.type.equals(other.type) && this.name.equals(other.name) && this.dc.equals(other.dc) && Objects.equals(this.client, other.client) && this.takeIfOpen == other.takeIfOpen;
         } else {
            return false;
         }
      }

      public int hashCode() {
         return Objects.hash(new Object[]{this.name, this.dc, this.client, Boolean.valueOf(this.takeIfOpen)});
      }
   }

   public static class LeaseRequest extends LeaseMonitorCore.LeaseId {
      public final MessageType type;

      LeaseRequest(MessageType type, String name, String dc) {
         super(name, dc);
         this.type = type;
      }

      public boolean equals(Object obj) {
         if(obj != null && obj instanceof LeaseProtocol.LeaseRequest) {
            LeaseProtocol.LeaseRequest other = (LeaseProtocol.LeaseRequest)obj;
            return this.type.equals(other.type) && this.name.equals(other.name) && this.dc.equals(other.dc);
         } else {
            return false;
         }
      }
   }
}
