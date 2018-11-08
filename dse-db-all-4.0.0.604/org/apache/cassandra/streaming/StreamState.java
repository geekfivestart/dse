package org.apache.cassandra.streaming;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.UUID;

public class StreamState implements Serializable {
   public final UUID planId;
   public final StreamOperation streamOperation;
   public final Set<SessionInfo> sessions;

   public StreamState(UUID planId, StreamOperation streamOperation, Set<SessionInfo> sessions) {
      this.planId = planId;
      this.sessions = sessions;
      this.streamOperation = streamOperation;
   }

   public boolean hasFailedSession() {
      return Iterables.any(this.sessions, SessionInfo::isFailed);
   }

   public boolean hasAbortedSession() {
      return Iterables.any(this.sessions, SessionInfo::isAborted);
   }

   public List<SessionSummary> createSummaries() {
      return Lists.newArrayList(Iterables.transform(this.sessions, SessionInfo::createSummary));
   }
}
