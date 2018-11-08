package org.apache.cassandra.service;

import java.io.IOException;

public class EmbeddedCassandraService {
   CassandraDaemon cassandraDaemon;

   public EmbeddedCassandraService() {
   }

   public void start() throws IOException {
      this.cassandraDaemon = CassandraDaemon.instance;
      this.cassandraDaemon.applyConfig();
      this.cassandraDaemon.init((String[])null);
      this.cassandraDaemon.start();
   }

   public void stop() {
      this.cassandraDaemon.stop();
   }
}
