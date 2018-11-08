package org.apache.cassandra.index.sasi.analyzer;

import com.google.common.annotations.Beta;
import com.google.common.base.Preconditions;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.AsciiType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.AbstractIterator;

@Beta
public class DelimiterAnalyzer extends AbstractAnalyzer {
   private static final Map<AbstractType<?>, Charset> VALID_ANALYZABLE_TYPES = new HashMap<AbstractType<?>, Charset>() {
      {
         this.put(UTF8Type.instance, StandardCharsets.UTF_8);
         this.put(AsciiType.instance, StandardCharsets.US_ASCII);
      }
   };
   private char delimiter;
   private Charset charset;
   private Iterator<ByteBuffer> iter;

   public DelimiterAnalyzer() {
   }

   public ByteBuffer next() {
      return (ByteBuffer)this.iter.next();
   }

   public void init(Map<String, String> options, AbstractType validator) {
      DelimiterTokenizingOptions tokenizingOptions = DelimiterTokenizingOptions.buildFromMap(options);
      this.delimiter = tokenizingOptions.getDelimiter();
      if(!VALID_ANALYZABLE_TYPES.containsKey(validator)) {
         throw new IllegalArgumentException(String.format("Only text types supported, got %s", new Object[]{validator}));
      } else {
         this.charset = (Charset)VALID_ANALYZABLE_TYPES.get(validator);
      }
   }

   public boolean hasNext() {
      return this.iter.hasNext();
   }

   public void reset(ByteBuffer input) {
      Objects.requireNonNull(input);
      final CharBuffer cb = this.charset.decode(input);
      this.iter = new AbstractIterator<ByteBuffer>() {
         protected ByteBuffer computeNext() {
            if(!cb.hasRemaining()) {
               return (ByteBuffer)this.endOfData();
            } else {
               CharBuffer readahead = cb.duplicate();

               boolean readaheadRemaining;
               while((readaheadRemaining = readahead.hasRemaining()) && readahead.get() != DelimiterAnalyzer.this.delimiter) {
                  ;
               }

               char[] chars = new char[readahead.position() - cb.position() - (readaheadRemaining?1:0)];
               cb.get(chars);
               Preconditions.checkState(!cb.hasRemaining() || cb.get() == DelimiterAnalyzer.this.delimiter);
               return 0 < chars.length?DelimiterAnalyzer.this.charset.encode(CharBuffer.wrap(chars)):this.computeNext();
            }
         }
      };
   }

   public boolean isTokenizing() {
      return true;
   }
}
