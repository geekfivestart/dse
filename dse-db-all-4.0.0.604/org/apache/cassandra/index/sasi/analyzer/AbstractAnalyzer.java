package org.apache.cassandra.index.sasi.analyzer;

import java.nio.ByteBuffer;
import java.text.Normalizer;
import java.text.Normalizer.Form;
import java.util.Iterator;
import java.util.Map;
import org.apache.cassandra.db.marshal.AbstractType;

public abstract class AbstractAnalyzer implements Iterator<ByteBuffer> {
   protected ByteBuffer next = null;

   public AbstractAnalyzer() {
   }

   public ByteBuffer next() {
      return this.next;
   }

   public void remove() {
      throw new UnsupportedOperationException();
   }

   public abstract void init(Map<String, String> var1, AbstractType var2);

   public abstract void reset(ByteBuffer var1);

   public boolean isTokenizing() {
      return false;
   }

   public static String normalize(String original) {
      return Normalizer.isNormalized(original, Form.NFC)?original:Normalizer.normalize(original, Form.NFC);
   }
}
