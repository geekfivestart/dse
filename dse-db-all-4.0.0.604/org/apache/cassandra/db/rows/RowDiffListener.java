package org.apache.cassandra.db.rows;

import org.apache.cassandra.db.Clustering;
import org.apache.cassandra.db.DeletionTime;
import org.apache.cassandra.db.LivenessInfo;
import org.apache.cassandra.schema.ColumnMetadata;

public interface RowDiffListener {
   void onPrimaryKeyLivenessInfo(int var1, Clustering var2, LivenessInfo var3, LivenessInfo var4);

   void onDeletion(int var1, Clustering var2, Row.Deletion var3, Row.Deletion var4);

   void onComplexDeletion(int var1, Clustering var2, ColumnMetadata var3, DeletionTime var4, DeletionTime var5);

   void onCell(int var1, Clustering var2, Cell var3, Cell var4);
}
