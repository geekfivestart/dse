package org.apache.cassandra.utils.progress;

import java.util.Iterator;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

public abstract class ProgressEventNotifierSupport implements ProgressEventNotifier {
   private List<ProgressListener> listeners = new CopyOnWriteArrayList();

   public ProgressEventNotifierSupport() {
   }

   public void addProgressListener(ProgressListener listener) {
      this.listeners.add(listener);
   }

   public void removeProgressListener(ProgressListener listener) {
      this.listeners.remove(listener);
   }

   protected void fireProgressEvent(String tag, ProgressEvent event) {
      Iterator var3 = this.listeners.iterator();

      while(var3.hasNext()) {
         ProgressListener listener = (ProgressListener)var3.next();
         listener.progress(tag, event);
      }

   }
}
