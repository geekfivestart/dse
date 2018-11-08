package com.datastax.bdp.concurrent;

import com.datastax.bdp.system.TimeSource;

public abstract class RoutableTask extends BaseTask {
   private final String key;

   public RoutableTask(TimeSource timeSource, String key) {
      super(timeSource);
      this.key = key;
   }

   public String getKey() {
      return this.key;
   }
}
