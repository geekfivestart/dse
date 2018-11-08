package org.apache.cassandra.net.interceptors;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

public enum MessageDirection {
   SENDING,
   RECEIVING;

   private MessageDirection() {
   }

   public static ImmutableSet<MessageDirection> all() {
      return Sets.immutableEnumSet(SENDING, new MessageDirection[]{RECEIVING});
   }
}
