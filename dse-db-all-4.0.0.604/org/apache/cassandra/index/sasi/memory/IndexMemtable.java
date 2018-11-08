package org.apache.cassandra.index.sasi.memory;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.plan.Expression;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.index.sasi.utils.TypeUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IndexMemtable {
   private static final Logger logger = LoggerFactory.getLogger(IndexMemtable.class);
   private final MemIndex index;

   public IndexMemtable(ColumnIndex columnIndex) {
      this.index = MemIndex.forColumn(columnIndex.keyValidator(), columnIndex);
   }

   public long index(DecoratedKey key, ByteBuffer value) {
      if(value != null && value.remaining() != 0) {
         AbstractType<?> validator = this.index.columnIndex.getValidator();
         if(!TypeUtil.isValid(value, validator)) {
            int size = value.remaining();
            if((value = TypeUtil.tryUpcast(value, validator)) == null) {
               logger.error("Can't add column {} to index for key: {}, value size {}, validator: {}.", new Object[]{this.index.columnIndex.getColumnName(), this.index.columnIndex.keyValidator().getString(key.getKey()), FBUtilities.prettyPrintMemory((long)size), validator});
               return 0L;
            }
         }

         return this.index.add(key, value);
      } else {
         return 0L;
      }
   }

   public RangeIterator<Long, Token> search(Expression expression) {
      return this.index == null?null:this.index.search(expression);
   }
}
