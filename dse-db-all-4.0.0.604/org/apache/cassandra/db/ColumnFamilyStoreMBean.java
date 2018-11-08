package org.apache.cassandra.db;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.management.openmbean.CompositeData;
import javax.management.openmbean.OpenDataException;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public interface ColumnFamilyStoreMBean {
   /** @deprecated */
   @Deprecated
   String getColumnFamilyName();

   String getTableName();

   void forceMajorCompaction(boolean var1) throws ExecutionException, InterruptedException;

   void forceCompactionForTokenRange(Collection<Range<Token>> var1) throws ExecutionException, InterruptedException;

   int getMinimumCompactionThreshold();

   void setMinimumCompactionThreshold(int var1);

   int getMaximumCompactionThreshold();

   void setCompactionThresholds(int var1, int var2);

   void setMaximumCompactionThreshold(int var1);

   void setCompactionParametersJson(String var1);

   String getCompactionParametersJson();

   void setCompactionParameters(Map<String, String> var1);

   Map<String, String> getCompactionParameters();

   Map<String, String> getCompressionParameters();

   String getCompressionParametersJson();

   void setCompressionParameters(Map<String, String> var1);

   void setCompressionParametersJson(String var1);

   void setCrcCheckChance(double var1);

   boolean isAutoCompactionDisabled();

   long estimateKeys();

   List<String> getBuiltIndexes();

   List<String> getSSTablesForKey(String var1);

   List<String> getSSTablesForKey(String var1, boolean var2);

   void loadNewSSTables();

   int getUnleveledSSTables();

   int[] getSSTableCountPerLevel();

   int getLevelFanoutSize();

   double getDroppableTombstoneRatio();

   long trueSnapshotsSize();

   void beginLocalSampling(String var1, int var2);

   CompositeData finishLocalSampling(String var1, int var2) throws OpenDataException;

   boolean isCompactionDiskSpaceCheckEnabled();

   void compactionDiskSpaceCheck(boolean var1);
}
