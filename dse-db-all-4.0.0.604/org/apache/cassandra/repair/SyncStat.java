package org.apache.cassandra.repair;

import java.util.List;
import org.apache.cassandra.streaming.SessionSummary;

public class SyncStat {
   public final NodePair nodes;
   public final long numberOfDifferences;
   public final List<SessionSummary> summaries;

   public SyncStat(NodePair nodes, long numberOfDifferences) {
      this(nodes, numberOfDifferences, (List)null);
   }

   public SyncStat(NodePair nodes, long numberOfDifferences, List<SessionSummary> summaries) {
      this.nodes = nodes;
      this.numberOfDifferences = numberOfDifferences;
      this.summaries = summaries;
   }

   public SyncStat withSummaries(List<SessionSummary> summaries) {
      return new SyncStat(this.nodes, this.numberOfDifferences, summaries);
   }
}
