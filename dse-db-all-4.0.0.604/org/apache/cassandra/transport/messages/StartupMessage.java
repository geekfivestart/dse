package org.apache.cassandra.transport.messages;

import io.netty.buffer.ByteBuf;
import io.reactivex.Single;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.CBUtil;
import org.apache.cassandra.transport.FrameCompressor;
import org.apache.cassandra.transport.Message;
import org.apache.cassandra.transport.ProtocolException;
import org.apache.cassandra.transport.ProtocolVersion;
import org.apache.cassandra.utils.CassandraVersion;

public class StartupMessage extends Message.Request {
   public static final String CQL_VERSION = "CQL_VERSION";
   public static final String COMPRESSION = "COMPRESSION";
   public static final String PROTOCOL_VERSIONS = "PROTOCOL_VERSIONS";
   public static final String CLIENT_ID = "CLIENT_ID";
   public static final String APPLICATION_NAME = "APPLICATION_NAME";
   public static final String APPLICATION_VERSION = "APPLICATION_VERSION";
   public static final String DRIVER_NAME = "DRIVER_NAME";
   public static final String DRIVER_VERSION = "DRIVER_VERSION";
   public static final Message.Codec<StartupMessage> codec = new Message.Codec<StartupMessage>() {
      public StartupMessage decode(ByteBuf body, ProtocolVersion version) {
         return new StartupMessage(StartupMessage.upperCaseKeys(CBUtil.readStringMap(body)));
      }

      public void encode(StartupMessage msg, ByteBuf dest, ProtocolVersion version) {
         CBUtil.writeStringMap(msg.options, dest);
      }

      public int encodedSize(StartupMessage msg, ProtocolVersion version) {
         return CBUtil.sizeOfStringMap(msg.options);
      }
   };
   public final Map<String, String> options;

   public StartupMessage(Map<String, String> options) {
      super(Message.Type.STARTUP);
      this.options = options;
   }

   public Single<? extends Message.Response> execute(Single<QueryState> state, long queryStartNanoTime) {
      String cqlVersion = (String)this.options.get("CQL_VERSION");
      if(cqlVersion == null) {
         throw new ProtocolException("Missing value CQL_VERSION in STARTUP message");
      } else {
         try {
            if((new CassandraVersion(cqlVersion)).compareTo(new CassandraVersion("2.99.0")) < 0) {
               throw new ProtocolException(String.format("CQL version %s is not supported by the binary protocol (supported version are >= 3.0.0)", new Object[]{cqlVersion}));
            }
         } catch (IllegalArgumentException var7) {
            throw new ProtocolException(var7.getMessage());
         }

         if(this.options.containsKey("COMPRESSION")) {
            String compression = ((String)this.options.get("COMPRESSION")).toLowerCase();
            if(compression.equals("snappy")) {
               if(FrameCompressor.SnappyCompressor.instance == null) {
                  throw new ProtocolException("This instance does not support Snappy compression");
               }

               this.connection.setCompressor(FrameCompressor.SnappyCompressor.instance);
            } else {
               if(!compression.equals("lz4")) {
                  throw new ProtocolException(String.format("Unknown compression algorithm: %s", new Object[]{compression}));
               }

               this.connection.setCompressor(FrameCompressor.LZ4Compressor.instance);
            }
         }

         QueryState queryState = (QueryState)state.blockingGet();
         ClientState clientState = queryState.getClientState();
         clientState.setClientID((String)this.options.get("CLIENT_ID"));
         clientState.setApplicationName((String)this.options.get("APPLICATION_NAME"));
         clientState.setApplicationVersion((String)this.options.get("APPLICATION_VERSION"));
         clientState.setDriverName((String)this.options.get("DRIVER_NAME"));
         clientState.setDriverVersion((String)this.options.get("DRIVER_VERSION"));
         return DatabaseDescriptor.getAuthenticator().requireAuthentication()?Single.just(new AuthenticateMessage(DatabaseDescriptor.getAuthenticator().getClass().getName())):Single.just(new ReadyMessage());
      }
   }

   private static Map<String, String> upperCaseKeys(Map<String, String> options) {
      Map<String, String> newMap = new HashMap(options.size());
      Iterator var2 = options.entrySet().iterator();

      while(var2.hasNext()) {
         Entry<String, String> entry = (Entry)var2.next();
         newMap.put(((String)entry.getKey()).toUpperCase(), entry.getValue());
      }

      return newMap;
   }

   public String toString() {
      return "STARTUP " + this.options;
   }
}
