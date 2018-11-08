package com.datastax.bdp.node.transport.internode;

import com.datastax.bdp.node.transport.MessageBodySerializer;
import com.datastax.bdp.node.transport.MessageType;
import com.datastax.bdp.node.transport.ServerProcessor;
import com.datastax.bdp.util.LambdaMayThrow;

public interface InternodeProtocolRegistry {
   byte VERSION_1 = 1;
   byte VERSION_2 = 2;
   byte CURRENT_VERSION = 2;

   <T> void addSerializer(MessageType var1, MessageBodySerializer<T> var2, byte... var3);

   <I, O> void addProcessor(MessageType var1, ServerProcessor<I, O> var2);

   <I, O> void addProcessor(MessageType var1, MessageType var2, LambdaMayThrow.FunctionMayThrow<I, O> var3);

   default void register(InternodeProtocol protocol) {
      protocol.register(this);
   }
}
