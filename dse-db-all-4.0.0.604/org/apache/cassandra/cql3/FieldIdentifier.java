package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.Locale;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.serializers.MarshalException;

public class FieldIdentifier {
   public final ByteBuffer bytes;
   private int hashCode = -1;

   public FieldIdentifier(ByteBuffer bytes) {
      this.bytes = bytes;
   }

   public static FieldIdentifier forUnquoted(String text) {
      return new FieldIdentifier(convert(text == null?null:text.toLowerCase(Locale.US)));
   }

   public static FieldIdentifier forQuoted(String text) {
      return new FieldIdentifier(convert(text));
   }

   public static FieldIdentifier forInternalString(String text) {
      return forQuoted(text);
   }

   private static ByteBuffer convert(String text) {
      try {
         return UTF8Type.instance.decompose(text);
      } catch (MarshalException var2) {
         throw new SyntaxException(String.format("For field name %s: %s", new Object[]{text, var2.getMessage()}));
      }
   }

   public String toString() {
      return (String)UTF8Type.instance.compose(this.bytes);
   }

   public final int hashCode() {
      int currHashCode = this.hashCode;
      if(currHashCode == -1) {
         currHashCode = this.bytes.hashCode();
         this.hashCode = currHashCode;
      }

      return currHashCode;
   }

   public final boolean equals(Object o) {
      if(!(o instanceof FieldIdentifier)) {
         return false;
      } else {
         FieldIdentifier that = (FieldIdentifier)o;
         return this.bytes.equals(that.bytes);
      }
   }
}
