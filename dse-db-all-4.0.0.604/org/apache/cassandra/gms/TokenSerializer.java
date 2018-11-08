package org.apache.cassandra.gms;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TokenSerializer {
   private static final Logger logger = LoggerFactory.getLogger(TokenSerializer.class);

   public TokenSerializer() {
   }

   public static void serialize(IPartitioner partitioner, Collection<Token> tokens, DataOutput out) throws IOException {
      Iterator var3 = tokens.iterator();

      while(var3.hasNext()) {
         Token token = (Token)var3.next();
         ByteBuffer tokenBuffer = partitioner.getTokenFactory().toByteArray(token);

         assert tokenBuffer.arrayOffset() == 0;

         ByteBufferUtil.writeWithLength(tokenBuffer.array(), out);
      }

      out.writeInt(0);
   }

   public static Collection<Token> deserialize(IPartitioner partitioner, DataInput in) throws IOException {
      ArrayList tokens = new ArrayList();

      while(true) {
         int size = in.readInt();
         if(size < 1) {
            return tokens;
         }

         logger.trace("Reading token of {}", FBUtilities.prettyPrintMemory((long)size));
         byte[] bintoken = new byte[size];
         in.readFully(bintoken);
         tokens.add(partitioner.getTokenFactory().fromByteArray(ByteBuffer.wrap(bintoken)));
      }
   }
}
