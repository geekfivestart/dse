package org.apache.cassandra.db.commitlog;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface CommitLogMBean {
   String getArchiveCommand();

   String getRestoreCommand();

   String getRestoreDirectories();

   long getRestorePointInTime();

   String getRestorePrecision();

   void recover(String var1) throws IOException;

   List<String> getActiveSegmentNames();

   List<String> getArchivingSegmentNames();

   long getActiveContentSize();

   long getActiveOnDiskSize();

   Map<String, Double> getActiveSegmentCompressionRatios();
}
