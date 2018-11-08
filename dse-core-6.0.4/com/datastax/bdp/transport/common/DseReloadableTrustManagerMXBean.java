package com.datastax.bdp.transport.common;

public interface DseReloadableTrustManagerMXBean {
   void reloadTrustManager() throws Exception;
}
