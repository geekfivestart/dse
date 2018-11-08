package com.datastax.bdp.plugin.health;

public class NodeHealthFunction {
   private NodeHealthFunction() {
   }

   public static double apply(double uptime, double droppedMessagesRate) {
      return (double)Math.round(uptime / (1.0D + droppedMessagesRate) * 10.0D) / 10.0D;
   }
}
