package com.datastax.bdp.node.transport;

import com.google.common.annotations.VisibleForTesting;
import java.util.Objects;

public class MessageType {
   private final byte domain;
   private final byte code;

   public static MessageType of(MessageType.Domain domain, byte code) {
      return new MessageType(domain.id, code);
   }

   @VisibleForTesting
   public MessageType(byte domain, byte code) {
      if(domain < 0) {
         throw new IllegalArgumentException("Cannot accept negative domain values!");
      } else if(code < 0) {
         throw new IllegalArgumentException("Cannot accept negative code values!");
      } else {
         this.domain = domain;
         this.code = code;
      }
   }

   public MessageType(short serialized) {
      this.domain = (byte)(serialized >> 8);
      this.code = (byte)serialized;
   }

   public short getSerialized() {
      return (short)(this.domain << 8 | this.code);
   }

   public short getDomain() {
      return (short)this.domain;
   }

   public short getCode() {
      return (short)this.code;
   }

   public String toString() {
      return this.domain + ":" + this.code;
   }

   public boolean equals(Object obj) {
      if(!(obj instanceof MessageType)) {
         return false;
      } else {
         MessageType other = (MessageType)obj;
         return Objects.equals(Byte.valueOf(this.domain), Byte.valueOf(other.domain)) && Objects.equals(Byte.valueOf(this.code), Byte.valueOf(other.code));
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{Byte.valueOf(this.domain), Byte.valueOf(this.code)});
   }

   public static enum Domain {
      SYSTEM(0),
      SEARCH(1),
      LEASE(2),
      GRAPH(3),
      GENERIC_QUERY_ROUTER(4),
      ADVREP(5);

      public final byte id;

      private Domain(byte id) {
         this.id = id;
      }
   }
}
