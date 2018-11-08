package org.apache.cassandra.utils.progress;

public interface ProgressEventNotifier {
   void addProgressListener(ProgressListener var1);

   void removeProgressListener(ProgressListener var1);
}
