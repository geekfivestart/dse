package org.apache.cassandra.schema;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import org.apache.cassandra.cql3.CQL3Type;
import org.apache.cassandra.cql3.CQLFragmentParser;
import org.apache.cassandra.cql3.CqlParser;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.utils.ByteBufferUtil;

public final class CQLTypeParser {
   private static final ImmutableSet<String> PRIMITIVE_TYPES;

   public CQLTypeParser() {
   }

   public static AbstractType<?> parse(String keyspace, String unparsed, Types userTypes) {
      String lowercased = unparsed.toLowerCase();
      if(PRIMITIVE_TYPES.contains(lowercased)) {
         return CQL3Type.Native.valueOf(unparsed.toUpperCase()).getType();
      } else {
         UserType udt = userTypes.getNullable(ByteBufferUtil.bytes(lowercased));
         return (AbstractType)(udt != null?udt:parseRaw(unparsed).prepareInternal(keyspace, userTypes).getType());
      }
   }

   static CQL3Type.Raw parseRaw(String type) {
      return (CQL3Type.Raw)CQLFragmentParser.parseAny(CqlParser::comparatorTypeWithMultiCellTuple, type, "CQL type");
   }

   static {
      Builder<String> builder = ImmutableSet.builder();
      CQL3Type.Native[] var1 = CQL3Type.Native.values();
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         CQL3Type.Native primitive = var1[var3];
         builder.add(primitive.name().toLowerCase());
      }

      PRIMITIVE_TYPES = builder.build();
   }
}
