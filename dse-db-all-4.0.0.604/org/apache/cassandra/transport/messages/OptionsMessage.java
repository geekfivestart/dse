package org.apache.cassandra.transport.messages;

import io.netty.buffer.ByteBuf;
import io.reactivex.Single;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.FrameCompressor;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolVersion;

public class OptionsMessage extends Message.Request {
   public static final Message.Codec<OptionsMessage> codec = new Message.Codec<OptionsMessage>() {
      public OptionsMessage decode(ByteBuf body, ProtocolVersion version) {
         return new OptionsMessage();
      }

      public void encode(OptionsMessage msg, ByteBuf dest, ProtocolVersion version) {
      }

      public int encodedSize(OptionsMessage msg, ProtocolVersion version) {
         return 0;
      }
   };

   public OptionsMessage() {
      super(Message.Type.OPTIONS);
   }

   public Single<? extends Message.Response> execute(Single<QueryState> state, long queryStartNanoTime) {
      List<String> cqlVersions = new ArrayList();
      cqlVersions.add(QueryProcessor.CQL_VERSION.toString());
      List<String> compressions = new ArrayList();
      if(FrameCompressor.SnappyCompressor.instance != null) {
         compressions.add("snappy");
      }

      compressions.add("lz4");
      Map<String, List<String>> supported = new HashMap();
      supported.put("CQL_VERSION", cqlVersions);
      supported.put("COMPRESSION", compressions);
      supported.put("PROTOCOL_VERSIONS", ProtocolVersion.supportedVersions());
      return Single.just(new SupportedMessage(supported));
   }

   public String toString() {
      return "OPTIONS";
   }
}
