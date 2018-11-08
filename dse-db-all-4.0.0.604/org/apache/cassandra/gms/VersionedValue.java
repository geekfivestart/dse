package org.apache.cassandra.gms;

import com.google.common.collect.Iterables;
import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.UUID;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Serializer;
import org.apache.commons.lang3.StringUtils;

public class VersionedValue implements Comparable<VersionedValue> {
   public static final Serializer<VersionedValue> serializer = new VersionedValue.ValueSerializer();
   public static final char DELIMITER = ',';
   public static final String DELIMITER_STR = new String(new char[]{','});
   public static final String STATUS_BOOTSTRAPPING = "BOOT";
   public static final String STATUS_BOOTSTRAPPING_REPLACE = "BOOT_REPLACE";
   public static final String STATUS_NORMAL = "NORMAL";
   public static final String STATUS_LEAVING = "LEAVING";
   public static final String STATUS_LEFT = "LEFT";
   public static final String STATUS_MOVING = "MOVING";
   public static final String REMOVING_TOKEN = "removing";
   public static final String REMOVED_TOKEN = "removed";
   public static final String HIBERNATE = "hibernate";
   public static final String SHUTDOWN = "shutdown";
   public static final String REMOVAL_COORDINATOR = "REMOVER";
   public final int version;
   public final String value;

   private VersionedValue(String value, int version) {
      assert value != null;

      this.value = value.intern();
      this.version = version;
   }

   private VersionedValue(String value) {
      this(value, VersionGenerator.getNextVersion());
   }

   public int compareTo(VersionedValue value) {
      return this.version - value.version;
   }

   public String toString() {
      return "Value(" + this.value + "," + this.version + ")";
   }

   public byte[] toBytes() {
      return this.value.getBytes(StandardCharsets.ISO_8859_1);
   }

   private static String versionString(String... args) {
      return StringUtils.join(args, ',');
   }

   private static class ValueSerializer implements Serializer<VersionedValue> {
      private ValueSerializer() {
      }

      public void serialize(VersionedValue value, DataOutputPlus out) throws IOException {
         out.writeUTF(value.value);
         out.writeInt(value.version);
      }

      public VersionedValue deserialize(DataInputPlus in) throws IOException {
         String value = in.readUTF();
         int valVersion = in.readInt();
         return new VersionedValue(value, valVersion);
      }

      public long serializedSize(VersionedValue value) {
         return (long)(TypeSizes.sizeof(value.value) + TypeSizes.sizeof(value.version));
      }
   }

   public static class VersionedValueFactory {
      final IPartitioner partitioner;

      public VersionedValueFactory(IPartitioner partitioner) {
         this.partitioner = partitioner;
      }

      public VersionedValue cloneWithHigherVersion(VersionedValue value) {
         return new VersionedValue(value.value);
      }

      public VersionedValue bootReplacing(InetAddress oldNode) {
         return new VersionedValue(VersionedValue.versionString(new String[]{"BOOT_REPLACE", oldNode.getHostAddress()}));
      }

      public VersionedValue bootstrapping(Collection<Token> tokens) {
         return new VersionedValue(VersionedValue.versionString(new String[]{"BOOT", this.makeTokenString(tokens)}));
      }

      public VersionedValue normal(Collection<Token> tokens) {
         return new VersionedValue(VersionedValue.versionString(new String[]{"NORMAL", this.makeTokenString(tokens)}));
      }

      private String makeTokenString(Collection<Token> tokens) {
         return this.partitioner.getTokenFactory().toString((Token)Iterables.get(tokens, 0));
      }

      public VersionedValue load(double load) {
         return new VersionedValue(String.valueOf(load));
      }

      public VersionedValue schema(UUID newVersion) {
         return new VersionedValue(newVersion.toString());
      }

      public VersionedValue leaving(Collection<Token> tokens) {
         return new VersionedValue(VersionedValue.versionString(new String[]{"LEAVING", this.makeTokenString(tokens)}));
      }

      public VersionedValue left(Collection<Token> tokens, long expireTime) {
         return new VersionedValue(VersionedValue.versionString(new String[]{"LEFT", this.makeTokenString(tokens), Long.toString(expireTime)}));
      }

      public VersionedValue moving(Token token) {
         return new VersionedValue("MOVING," + this.partitioner.getTokenFactory().toString(token));
      }

      public VersionedValue hostId(UUID hostId) {
         return new VersionedValue(hostId.toString());
      }

      public VersionedValue tokens(Collection<Token> tokens) {
         ByteArrayOutputStream bos = new ByteArrayOutputStream();
         DataOutputStream out = new DataOutputStream(bos);

         try {
            TokenSerializer.serialize(this.partitioner, tokens, out);
         } catch (IOException var5) {
            throw new RuntimeException(var5);
         }

         return new VersionedValue(new String(bos.toByteArray(), StandardCharsets.ISO_8859_1));
      }

      public VersionedValue removingNonlocal(UUID hostId) {
         return new VersionedValue(VersionedValue.versionString(new String[]{"removing", hostId.toString()}));
      }

      public VersionedValue removedNonlocal(UUID hostId, long expireTime) {
         return new VersionedValue(VersionedValue.versionString(new String[]{"removed", hostId.toString(), Long.toString(expireTime)}));
      }

      public VersionedValue removalCoordinator(UUID hostId) {
         return new VersionedValue(VersionedValue.versionString(new String[]{"REMOVER", hostId.toString()}));
      }

      public VersionedValue hibernate(boolean value) {
         return new VersionedValue("hibernate," + value);
      }

      public VersionedValue nativeTransportReady(boolean value) {
         return new VersionedValue(String.valueOf(value));
      }

      public VersionedValue shutdown(boolean value) {
         return new VersionedValue("shutdown," + value);
      }

      public VersionedValue datacenter(String dcId) {
         return new VersionedValue(dcId);
      }

      public VersionedValue rack(String rackId) {
         return new VersionedValue(rackId);
      }

      public VersionedValue rpcaddress(InetAddress endpoint) {
         return new VersionedValue(endpoint.getHostAddress());
      }

      public VersionedValue releaseVersion() {
         return new VersionedValue(FBUtilities.getReleaseVersionString());
      }

      public VersionedValue schemaCompatibilityVersion() {
         return new VersionedValue(Integer.toString(0));
      }

      public VersionedValue networkVersion() {
         return new VersionedValue(String.valueOf(MessagingService.current_version.protocolVersion().handshakeVersion));
      }

      public VersionedValue internalIP(String private_ip) {
         return new VersionedValue(private_ip);
      }

      public VersionedValue severity(double value) {
         return new VersionedValue(String.valueOf(value));
      }

      public VersionedValue nativeTransportPort(int port) {
         return new VersionedValue(String.valueOf(port));
      }

      public VersionedValue nativeTransportPortSSL(int port) {
         return new VersionedValue(String.valueOf(port));
      }

      public VersionedValue storagePort(int port) {
         return new VersionedValue(String.valueOf(port));
      }

      public VersionedValue storagePortSSL(int port) {
         return new VersionedValue(String.valueOf(port));
      }

      public VersionedValue jmxPort(int port) {
         return new VersionedValue(String.valueOf(port));
      }
   }
}
