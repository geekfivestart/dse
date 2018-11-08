package com.datastax.bdp.node.transport;

import com.google.common.annotations.VisibleForTesting;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;

public class Message<T> {
   private final EnumSet<Message.Flag> flags;
   private final long id;
   private final MessageType type;
   private final T body;
   private volatile byte version;

   public Message(long id, MessageType type, T body) {
      this(EnumSet.noneOf(Message.Flag.class), id, type, body);
   }

   public Message(EnumSet<Message.Flag> flags, long id, MessageType type, T body) {
      this.version = -128;
      this.flags = flags;
      this.id = id;
      this.type = type;
      this.body = body;
   }

   public long getId() {
      return this.id;
   }

   public MessageType getType() {
      return this.type;
   }

   public T getBody() {
      return this.body;
   }

   public EnumSet<Message.Flag> getFlags() {
      return this.flags;
   }

   public byte getVersion() {
      return this.version;
   }

   @VisibleForTesting
   public void trySetVersion(byte version) {
      if(this.version == -128) {
         if(version == -128) {
            throw new IllegalArgumentException("Not a valid version: " + version);
         }

         this.version = version;
      }

   }

   public boolean equals(Object obj) {
      if(!(obj instanceof Message)) {
         return false;
      } else {
         Message other = (Message)obj;
         return Objects.equals(Long.valueOf(this.id), Long.valueOf(other.id)) && Objects.equals(this.type, other.type);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{Long.valueOf(this.id), this.type});
   }

   public String toString() {
      return String.format("\nFlags: %s\nId: %s\nType: %s\nVersion: %s\nBody: %s", new Object[]{this.flags, Long.valueOf(this.id), this.type, Byte.valueOf(this.version), this.body});
   }

   public static enum Flag {
      CUSTOM_1(1),
      CUSTOM_2(2),
      CUSTOM_3(3),
      CUSTOM_4(4),
      UNSUPPORTED_MESSAGE(5),
      FAILED_PROCESSOR(6);

      private static final Message.Flag[] STATIC_VALUES = values();
      private final int position;

      private Flag(int position) {
         this.position = position;
      }

      public static EnumSet<Message.Flag> deserialize(int flags) {
         EnumSet<Message.Flag> set = EnumSet.noneOf(Message.Flag.class);
         Message.Flag[] var2 = STATIC_VALUES;
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            Message.Flag flag = var2[var4];
            if((flags & 1 << flag.position) != 0) {
               set.add(flag);
            }
         }

         return set;
      }

      public static int serialize(EnumSet<Message.Flag> flags) {
         int tmp = 0;

         Message.Flag flag;
         for(Iterator var2 = flags.iterator(); var2.hasNext(); tmp |= 1 << flag.position) {
            flag = (Message.Flag)var2.next();
         }

         return tmp;
      }
   }
}
