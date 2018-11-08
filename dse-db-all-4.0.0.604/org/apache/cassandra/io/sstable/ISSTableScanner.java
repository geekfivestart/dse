package org.apache.cassandra.io.sstable;

import com.google.common.base.Throwables;
import java.util.Collection;
import java.util.Iterator;
import org.apache.cassandra.db.partitions.UnfilteredPartitionIterator;
import org.apache.cassandra.utils.JVMStabilityInspector;

public interface ISSTableScanner extends UnfilteredPartitionIterator {
   long getLengthInBytes();

   long getCompressedLengthInBytes();

   long getCurrentPosition();

   long getBytesScanned();

   String getBackingFiles();

   static default void closeAllAndPropagate(Collection<ISSTableScanner> scanners, Throwable throwable) {
      Iterator var2 = scanners.iterator();

      while(var2.hasNext()) {
         ISSTableScanner scanner = (ISSTableScanner)var2.next();

         try {
            scanner.close();
         } catch (Throwable var5) {
            JVMStabilityInspector.inspectThrowable(var5);
            if(throwable == null) {
               throwable = var5;
            } else {
               throwable.addSuppressed(var5);
            }
         }
      }

      if(throwable != null) {
         Throwables.propagate(throwable);
      }

   }
}
