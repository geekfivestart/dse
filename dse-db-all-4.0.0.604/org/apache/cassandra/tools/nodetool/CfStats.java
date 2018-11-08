package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;

/** @deprecated */
@Command(
   name = "cfstats",
   hidden = true,
   description = "Print statistics on tables"
)
@Deprecated
public class CfStats extends TableStats {
   public CfStats() {
   }
}
