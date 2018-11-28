package org.apache.cassandra.cql3;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.ListType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.SetType;
import org.apache.cassandra.utils.ByteBufferUtil;

public enum Operator {
   EQ(0) {
      public String toString() {
         return "=";
      }

      public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand) {
         return type.compareForCQL(leftOperand, rightOperand) == 0;
      }
   },
   LT(4) {
      public String toString() {
         return "<";
      }

      public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand) {
         return type.compareForCQL(leftOperand, rightOperand) < 0;
      }
   },
   LTE(3) {
      public String toString() {
         return "<=";
      }

      public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand) {
         return type.compareForCQL(leftOperand, rightOperand) <= 0;
      }
   },
   GTE(1) {
      public String toString() {
         return ">=";
      }

      public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand) {
         return type.compareForCQL(leftOperand, rightOperand) >= 0;
      }
   },
   GT(2) {
      public String toString() {
         return ">";
      }

      public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand) {
         return type.compareForCQL(leftOperand, rightOperand) > 0;
      }
   },
   IN(7) {
      public String toString() {
         return "IN";
      }

      public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand) {
         List<?> inValues = (List)ListType.getInstance(type, false).getSerializer().deserialize(rightOperand);
         return inValues.contains(type.getSerializer().deserialize(leftOperand));
      }
   },
   CONTAINS(5) {
      public String toString() {
         return "CONTAINS";
      }

      public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand) {
         switch (((CollectionType)type).kind) {
            case LIST: {
               ListType listType = (ListType)type;
               List list = (List)listType.getSerializer().deserialize(leftOperand);
               return list.contains(listType.getElementsType().getSerializer().deserialize(rightOperand));
            }
            case SET: {
               SetType setType = (SetType)type;
               Set set = (Set)setType.getSerializer().deserialize(leftOperand);
               return set.contains(setType.getElementsType().getSerializer().deserialize(rightOperand));
            }
            case MAP: {
               MapType mapType = (MapType)type;
               Map map = (Map)mapType.getSerializer().deserialize(leftOperand);
               return map.containsValue(mapType.getValuesType().getSerializer().deserialize(rightOperand));
            }
         }
         throw new AssertionError();
      }
   },
   CONTAINS_KEY(6) {
      public String toString() {
         return "CONTAINS KEY";
      }

      public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand) {
         MapType<?, ?> mapType = (MapType)type;
         Map<?, ?> map = (Map)mapType.getSerializer().deserialize(leftOperand);
         return map.containsKey(mapType.getKeysType().getSerializer().deserialize(rightOperand));
      }
   },
   NEQ(8) {
      public String toString() {
         return "!=";
      }

      public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand) {
         return type.compareForCQL(leftOperand, rightOperand) != 0;
      }
   },
   IS_NOT(9) {
      public String toString() {
         return "IS NOT";
      }

      public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand) {
         throw new UnsupportedOperationException();
      }
   },
   LIKE_PREFIX(10) {
      public String toString() {
         return "LIKE '<term>%'";
      }

      public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand) {
         return ByteBufferUtil.startsWith(leftOperand, rightOperand);
      }
   },
   LIKE_SUFFIX(11) {
      public String toString() {
         return "LIKE '%<term>'";
      }

      public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand) {
         return ByteBufferUtil.endsWith(leftOperand, rightOperand);
      }
   },
   LIKE_CONTAINS(12) {
      public String toString() {
         return "LIKE '%<term>%'";
      }

      public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand) {
         return ByteBufferUtil.contains(leftOperand, rightOperand);
      }
   },
   LIKE_MATCHES(13) {
      public String toString() {
         return "LIKE '<term>'";
      }

      public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand) {
         return ByteBufferUtil.contains(leftOperand, rightOperand);
      }
   },
   LIKE(14) {
      public String toString() {
         return "LIKE";
      }

      public boolean isSatisfiedBy(AbstractType<?> type, ByteBuffer leftOperand, ByteBuffer rightOperand) {
         throw new UnsupportedOperationException();
      }
   };

   private final int b;

   private Operator(int b) {
      this.b = b;
   }

   public void writeTo(DataOutput output) throws IOException {
      output.writeInt(this.b);
   }

   public int getValue() {
      return this.b;
   }

   public static Operator readFrom(DataInput input) throws IOException {
      int b = input.readInt();
      Operator[] var2 = values();
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         Operator operator = var2[var4];
         if(operator.b == b) {
            return operator;
         }
      }

      throw new IOException(String.format("Cannot resolve Relation.Type from binary representation: %s", new Object[]{Integer.valueOf(b)}));
   }

   public abstract boolean isSatisfiedBy(AbstractType<?> var1, ByteBuffer var2, ByteBuffer var3);

   public int serializedSize() {
      return 4;
   }

   public boolean isSlice() {
      return this == LT || this == LTE || this == GT || this == GTE;
   }

   public boolean isLike() {
      return this == LIKE_PREFIX || this == LIKE_CONTAINS || this == LIKE_SUFFIX || this == LIKE_MATCHES;
   }

   public String toString() {
      return this.name();
   }

   public boolean isIN() {
      return this == IN;
   }
}
