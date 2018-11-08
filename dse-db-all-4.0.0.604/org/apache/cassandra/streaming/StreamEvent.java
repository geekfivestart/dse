package org.apache.cassandra.streaming;

import com.google.common.collect.ImmutableSet;
import java.net.InetAddress;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;

public abstract class StreamEvent {
   public final StreamEvent.Type eventType;
   public final UUID planId;

   protected StreamEvent(StreamEvent.Type eventType, UUID planId) {
      this.eventType = eventType;
      this.planId = planId;
   }

   public static class SessionPreparedEvent extends StreamEvent {
      public final SessionInfo session;

      public SessionPreparedEvent(UUID planId, SessionInfo session) {
         super(StreamEvent.Type.STREAM_PREPARED, planId);
         this.session = session;
      }
   }

   public static class ProgressEvent extends StreamEvent {
      public final ProgressInfo progress;

      public ProgressEvent(UUID planId, ProgressInfo progress) {
         super(StreamEvent.Type.FILE_PROGRESS, planId);
         this.progress = progress;
      }

      public String toString() {
         return "<ProgressEvent " + this.progress + ">";
      }
   }

   public static class SessionCompleteEvent extends StreamEvent {
      public final InetAddress peer;
      public final boolean success;
      public final int sessionIndex;
      public final Set<StreamRequest> requests;
      public final StreamOperation streamOperation;
      public final Map<String, Set<Range<Token>>> transferredRangesPerKeyspace;

      public SessionCompleteEvent(StreamSession session) {
         super(StreamEvent.Type.STREAM_COMPLETE, session.planId());
         this.peer = session.peer;
         this.success = session.isSuccess();
         this.sessionIndex = session.sessionIndex();
         this.requests = ImmutableSet.copyOf(session.requests);
         this.streamOperation = session.streamOperation();
         this.transferredRangesPerKeyspace = Collections.unmodifiableMap(session.transferredRangesPerKeyspace);
      }
   }

   public static enum Type {
      STREAM_PREPARED,
      STREAM_COMPLETE,
      FILE_PROGRESS;

      private Type() {
      }
   }
}
