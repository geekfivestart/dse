package org.apache.cassandra.io.sstable;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.regex.Pattern;

public class Component {
   public static final char separator = '-';
   static final EnumSet<Component.Type> TYPES = EnumSet.allOf(Component.Type.class);
   public static final Component DATA;
   public static final Component PARTITION_INDEX;
   public static final Component ROW_INDEX;
   public static final Component PRIMARY_INDEX;
   public static final Component FILTER;
   public static final Component COMPRESSION_INFO;
   public static final Component STATS;
   public static final Component DIGEST;
   public static final Component CRC;
   public static final Component SUMMARY;
   public static final Component TOC;
   public final Component.Type type;
   public final String name;
   public final int hashCode;

   public Component(Component.Type type) {
      this(type, type.repr);

      assert type != Component.Type.CUSTOM;

   }

   public Component(Component.Type type, String name) {
      assert name != null : "Component name cannot be null";

      this.type = type;
      this.name = name;
      this.hashCode = Objects.hash(new Object[]{type, name});
   }

   public String name() {
      return this.name;
   }

   static Component parse(String name) {
      Type type = Type.fromRepresentation(name);
      switch (type) {
         case DATA: {
            return DATA;
         }
         case PARTITION_INDEX: {
            return PARTITION_INDEX;
         }
         case ROW_INDEX: {
            return ROW_INDEX;
         }
         case PRIMARY_INDEX: {
            return PRIMARY_INDEX;
         }
         case FILTER: {
            return FILTER;
         }
         case COMPRESSION_INFO: {
            return COMPRESSION_INFO;
         }
         case STATS: {
            return STATS;
         }
         case DIGEST: {
            return DIGEST;
         }
         case CRC: {
            return CRC;
         }
         case SUMMARY: {
            return SUMMARY;
         }
         case TOC: {
            return TOC;
         }
         case SECONDARY_INDEX: {
            return new Component(Type.SECONDARY_INDEX, name);
         }
         case CUSTOM: {
            return new Component(Type.CUSTOM, name);
         }
      }
      throw new AssertionError();
   }

   public String toString() {
      return this.name();
   }

   public boolean equals(Object o) {
      if(o == this) {
         return true;
      } else if(!(o instanceof Component)) {
         return false;
      } else {
         Component that = (Component)o;
         return this.type == that.type && this.name.equals(that.name);
      }
   }

   public int hashCode() {
      return this.hashCode;
   }

   static {
      DATA = new Component(Component.Type.DATA);
      PARTITION_INDEX = new Component(Component.Type.PARTITION_INDEX);
      ROW_INDEX = new Component(Component.Type.ROW_INDEX);
      PRIMARY_INDEX = new Component(Component.Type.PRIMARY_INDEX);
      FILTER = new Component(Component.Type.FILTER);
      COMPRESSION_INFO = new Component(Component.Type.COMPRESSION_INFO);
      STATS = new Component(Component.Type.STATS);
      DIGEST = new Component(Component.Type.DIGEST);
      CRC = new Component(Component.Type.CRC);
      SUMMARY = new Component(Component.Type.SUMMARY);
      TOC = new Component(Component.Type.TOC);
   }

   public static enum Type {
      DATA("Data.db"),
      PARTITION_INDEX("Partitions.db"),
      ROW_INDEX("Rows.db"),
      PRIMARY_INDEX("Index.db"),
      FILTER("Filter.db"),
      COMPRESSION_INFO("CompressionInfo.db"),
      STATS("Statistics.db"),
      DIGEST("Digest.crc32"),
      CRC("CRC.db"),
      SUMMARY("Summary.db"),
      TOC("TOC.txt"),
      SECONDARY_INDEX("SI_.*.db"),
      CUSTOM((String)null);

      final String repr;

      private Type(String repr) {
         this.repr = repr;
      }

      static Component.Type fromRepresentation(String repr) {
         Iterator var1 = Component.TYPES.iterator();

         Component.Type type;
         do {
            if(!var1.hasNext()) {
               return CUSTOM;
            }

            type = (Component.Type)var1.next();
         } while(type.repr == null || !Pattern.matches(type.repr, repr));

         return type;
      }
   }
}
