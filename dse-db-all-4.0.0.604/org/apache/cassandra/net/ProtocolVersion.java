package org.apache.cassandra.net;

import java.util.Objects;

public class ProtocolVersion implements Comparable<ProtocolVersion> {
   public final boolean isDSE;
   public final int version;
   private final int rawHeader;
   public final int handshakeVersion;

   private ProtocolVersion(boolean isDSE, int version, int rawHeader, int handshakeVersion) {
      this.isDSE = isDSE;
      this.version = version;
      this.rawHeader = rawHeader;
      this.handshakeVersion = handshakeVersion;
   }

   public static ProtocolVersion oss(int version) {
      assert version < 256;

      return new ProtocolVersion(false, version, version << 8, version);
   }

   public static ProtocolVersion dse(int version) {
      assert version < 256;

      return new ProtocolVersion(true, version, version << 24 | '\uff00', version << 8);
   }

   public int makeProtocolHeader(boolean compressionEnabled, boolean isStream) {
      int header = this.rawHeader;
      if(compressionEnabled) {
         header |= 4;
      }

      if(isStream) {
         header |= 8;
      }

      return header;
   }

   static ProtocolVersion fromProtocolHeader(int header) {
      int version = MessagingService.getBits(header, 15, 8);
      return version == 255?dse(MessagingService.getBits(header, 31, 8)):oss(version);
   }

   public static ProtocolVersion fromHandshakeVersion(int handshakeVersion) {
      return handshakeVersion < 256?oss(handshakeVersion):dse(handshakeVersion >>> 8);
   }

   public int compareTo(ProtocolVersion protocolVersion) {
      return Integer.compare(this.handshakeVersion, protocolVersion.handshakeVersion);
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         ProtocolVersion that = (ProtocolVersion)o;
         return this.handshakeVersion == that.handshakeVersion;
      } else {
         return false;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{Integer.valueOf(this.handshakeVersion)});
   }

   public String toString() {
      return this.isDSE?String.format("dse(%d)", new Object[]{Integer.valueOf(this.version)}):String.valueOf(this.version);
   }
}
