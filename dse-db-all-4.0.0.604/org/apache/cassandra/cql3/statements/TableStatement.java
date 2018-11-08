package org.apache.cassandra.cql3.statements;

public interface TableStatement extends KeyspaceStatement {
   String columnFamily();
}
