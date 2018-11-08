package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Command;

/** @deprecated */
@Command(
   name = "cfhistograms",
   hidden = true,
   description = "Print statistic histograms for a given column family"
)
@Deprecated
public class CfHistograms extends TableHistograms {
   public CfHistograms() {
   }
}
