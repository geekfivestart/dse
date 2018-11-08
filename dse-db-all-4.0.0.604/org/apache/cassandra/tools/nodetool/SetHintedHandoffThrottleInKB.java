package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "sethintedhandoffthrottlekb",
   description = "Set hinted handoff throttle in kb per second, per delivery thread."
)
public class SetHintedHandoffThrottleInKB extends NodeTool.NodeToolCmd {
   @Arguments(
      title = "throttle_in_kb",
      usage = "<value_in_kb_per_sec>",
      description = "Value in KB per second",
      required = true
   )
   private Integer throttleInKB = null;

   public SetHintedHandoffThrottleInKB() {
   }

   public void execute(NodeProbe probe) {
      probe.setHintedHandoffThrottleInKB(this.throttleInKB.intValue());
   }
}
