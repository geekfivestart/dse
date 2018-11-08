package org.apache.cassandra.tools.nodetool;

import com.google.common.base.Preconditions;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "settraceprobability",
   description = "Sets the probability for tracing any given request to value. 0 disables, 1 enables for all requests, 0 is the default"
)
public class SetTraceProbability extends NodeTool.NodeToolCmd {
   @Arguments(
      title = "trace_probability",
      usage = "<value>",
      description = "Trace probability between 0 and 1 (ex: 0.2)",
      required = true
   )
   private Double traceProbability = null;

   public SetTraceProbability() {
   }

   public void execute(NodeProbe probe) {
      Preconditions.checkArgument(this.traceProbability.doubleValue() >= 0.0D && this.traceProbability.doubleValue() <= 1.0D, "Trace probability must be between 0 and 1");
      probe.setTraceProbability(this.traceProbability.doubleValue());
   }
}
