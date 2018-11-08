package org.apache.cassandra.streaming;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.io.Serializable;
import java.net.InetAddress;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.cassandra.utils.FBUtilities;

public final class SessionInfo implements Serializable {
   public final InetAddress peer;
   public final int sessionIndex;
   public final InetAddress connecting;
   public final Collection<StreamSummary> receivingSummaries;
   public final Collection<StreamSummary> sendingSummaries;
   public final StreamSession.State state;
   private Map<String, ProgressInfo> receivingFiles;
   private Map<String, ProgressInfo> sendingFiles;

   public SessionInfo(InetAddress peer, int sessionIndex, InetAddress connecting, Collection<StreamSummary> receivingSummaries, Collection<StreamSummary> sendingSummaries, StreamSession.State state) {
      this.peer = peer;
      this.sessionIndex = sessionIndex;
      this.connecting = connecting;
      this.receivingSummaries = ImmutableSet.copyOf(receivingSummaries);
      this.sendingSummaries = ImmutableSet.copyOf(sendingSummaries);
      this.receivingFiles = new ConcurrentHashMap();
      this.sendingFiles = new ConcurrentHashMap();
      this.state = state;
   }

   public void copyProgress(SessionInfo previous) {
      this.receivingFiles = new ConcurrentHashMap(previous.receivingFiles);
      this.sendingFiles = new ConcurrentHashMap(previous.sendingFiles);
   }

   public boolean isFailed() {
      return this.state == StreamSession.State.FAILED;
   }

   public boolean isAborted() {
      return this.state == StreamSession.State.ABORTED;
   }

   public void updateProgress(ProgressInfo newProgress) {
      assert this.peer.equals(newProgress.peer);

      Map<String, ProgressInfo> currentFiles = newProgress.direction == ProgressInfo.Direction.IN?this.receivingFiles:this.sendingFiles;
      currentFiles.put(newProgress.fileName, newProgress);
   }

   public Collection<ProgressInfo> getReceivingFiles() {
      return this.receivingFiles.values();
   }

   public Collection<ProgressInfo> getSendingFiles() {
      return this.sendingFiles.values();
   }

   public long getTotalFilesReceived() {
      return this.getTotalFilesCompleted(this.receivingFiles.values());
   }

   public long getTotalFilesSent() {
      return this.getTotalFilesCompleted(this.sendingFiles.values());
   }

   public long getTotalSizeReceived() {
      return this.getTotalSizeInProgress(this.receivingFiles.values());
   }

   public long getTotalSizeSent() {
      return this.getTotalSizeInProgress(this.sendingFiles.values());
   }

   public long getTotalFilesToReceive() {
      return this.getTotalFiles(this.receivingSummaries);
   }

   public long getTotalFilesToSend() {
      return this.getTotalFiles(this.sendingSummaries);
   }

   public long getTotalSizeToReceive() {
      return this.getTotalSizes(this.receivingSummaries);
   }

   public long getTotalSizeToSend() {
      return this.getTotalSizes(this.sendingSummaries);
   }

   private long getTotalSizeInProgress(Collection<ProgressInfo> files) {
      long total = 0L;

      ProgressInfo file;
      for(Iterator var4 = files.iterator(); var4.hasNext(); total += file.currentBytes) {
         file = (ProgressInfo)var4.next();
      }

      return total;
   }

   private long getTotalFiles(Collection<StreamSummary> summaries) {
      long total = 0L;

      StreamSummary summary;
      for(Iterator var4 = summaries.iterator(); var4.hasNext(); total += (long)summary.files) {
         summary = (StreamSummary)var4.next();
      }

      return total;
   }

   private long getTotalSizes(Collection<StreamSummary> summaries) {
      long total = 0L;

      StreamSummary summary;
      for(Iterator var4 = summaries.iterator(); var4.hasNext(); total += summary.totalSize) {
         summary = (StreamSummary)var4.next();
      }

      return total;
   }

   private long getTotalFilesCompleted(Collection<ProgressInfo> files) {
      Iterable<ProgressInfo> completed = Iterables.filter(files, new Predicate<ProgressInfo>() {
         public boolean apply(ProgressInfo input) {
            return input.isCompleted();
         }
      });
      return (long)Iterables.size(completed);
   }

   public SessionSummary createSummary() {
      return new SessionSummary(FBUtilities.getBroadcastAddress(), this.peer, this.receivingSummaries, this.sendingSummaries);
   }
}
