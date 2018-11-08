package org.apache.cassandra.dht.tokenallocator;

import java.util.Collection;
import org.apache.cassandra.dht.Token;

public interface TokenAllocator<Unit> {
   Collection<Token> addUnit(Unit var1, int var2);
}
