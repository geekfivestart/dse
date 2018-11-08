package org.apache.cassandra.index.internal.composites;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CollectionType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.rows.Cell;
import org.apache.cassandra.db.rows.CellPath;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;

public class CollectionEntryIndex extends CollectionKeyIndexBase {
   public CollectionEntryIndex(ColumnFamilyStore baseCfs, IndexMetadata indexDef) {
      super(baseCfs, indexDef);
   }

   public ByteBuffer getIndexedValue(ByteBuffer partitionKey, Clustering clustering, CellPath path, ByteBuffer cellValue) {
      return CompositeType.build(new ByteBuffer[]{path.get(0), cellValue});
   }

   public boolean isStale(Row data, ByteBuffer indexValue, int nowInSec) {
      ByteBuffer[] components = ((CompositeType)this.functions.getIndexedValueType(this.indexedColumn)).split(indexValue);
      ByteBuffer mapKey = components[0];
      ByteBuffer mapValue = components[1];
      ColumnMetadata columnDef = this.indexedColumn;
      Cell cell = data.getCell(columnDef, CellPath.create(mapKey));
      if(cell != null && cell.isLive(nowInSec)) {
         AbstractType<?> valueComparator = ((CollectionType)columnDef.type).valueComparator();
         return valueComparator.compare(mapValue, cell.value()) != 0;
      } else {
         return true;
      }
   }
}
