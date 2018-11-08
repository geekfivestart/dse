package com.datastax.bdp.reporting;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;

public abstract class PersistedSystemInfo extends CqlWriter<PersistedSystemInfo> implements CqlWritable {
   protected PersistedSystemInfo(InetAddress nodeAddress, int ttl) {
      super(nodeAddress, ttl);
   }

   protected abstract List<ByteBuffer> getVariables();

   protected List<ByteBuffer> getVariables(PersistedSystemInfo writeable) {
      assert this == writeable;

      return this.getVariables();
   }
}
