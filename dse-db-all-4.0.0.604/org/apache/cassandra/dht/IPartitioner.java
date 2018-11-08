package org.apache.cassandra.dht;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.marshal.AbstractType;

public interface IPartitioner {
   DecoratedKey decorateKey(ByteBuffer var1);

   Token midpoint(Token var1, Token var2);

   Token split(Token var1, Token var2, double var3);

   Token getMinimumToken();

   default Token getMaximumToken() {
      throw new UnsupportedOperationException("If you are using a splitting partitioner, getMaximumToken has to be implemented");
   }

   Token getToken(ByteBuffer var1);

   Token getRandomToken();

   Token getRandomToken(Random var1);

   Token.TokenFactory getTokenFactory();

   boolean preservesOrder();

   Map<Token, Float> describeOwnership(List<Token> var1);

   AbstractType<?> getTokenValidator();

   AbstractType<?> partitionOrdering();

   default Optional<Splitter> splitter() {
      return Optional.empty();
   }

   default boolean hasNumericTokens() {
      return false;
   }

   default BigInteger valueForToken(Token token) {
      throw new UnsupportedOperationException();
   }
}
