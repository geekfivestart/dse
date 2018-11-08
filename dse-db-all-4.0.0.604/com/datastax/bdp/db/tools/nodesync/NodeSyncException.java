package com.datastax.bdp.db.tools.nodesync;

public class NodeSyncException extends RuntimeException {
   NodeSyncException(String message) {
      super(message);
   }
}
