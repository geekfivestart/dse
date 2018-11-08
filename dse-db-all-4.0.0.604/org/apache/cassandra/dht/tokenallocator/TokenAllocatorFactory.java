package org.apache.cassandra.dht.tokenallocator;

import java.net.InetAddress;
import java.util.NavigableMap;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenAllocatorFactory {
   private static final Logger logger = LoggerFactory.getLogger(TokenAllocatorFactory.class);

   public TokenAllocatorFactory() {
   }

   public static TokenAllocator<InetAddress> createTokenAllocator(NavigableMap<Token, InetAddress> sortedTokens, ReplicationStrategy<InetAddress> strategy, IPartitioner partitioner) {
      if(strategy.replicas() == 1) {
         logger.info("Using NoReplicationTokenAllocator.");
         return new NoReplicationTokenAllocator(sortedTokens, strategy, partitioner);
      } else {
         logger.info("Using ReplicationAwareTokenAllocator.");
         return new ReplicationAwareTokenAllocator(sortedTokens, strategy, partitioner);
      }
   }
}
