package org.apache.cassandra.transport;

import com.google.common.base.Splitter;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.config.EncryptionOptions;
import org.apache.cassandra.cql3.QueryOptions;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.service.pager.PagingState;
import org.apache.cassandra.transport.messages.AuthResponse;
import org.apache.cassandra.transport.messages.ExecuteMessage;
import org.apache.cassandra.transport.messages.OptionsMessage;
import org.apache.cassandra.transport.messages.PrepareMessage;
import org.apache.cassandra.transport.messages.QueryMessage;
import org.apache.cassandra.transport.messages.RegisterMessage;
import org.apache.cassandra.transport.messages.StartupMessage;
import org.apache.cassandra.utils.Hex;
import org.apache.cassandra.utils.JVMStabilityInspector;
import org.apache.cassandra.utils.MD5Digest;

public class Client extends SimpleClient {
   private final SimpleClient.SimpleEventHandler eventHandler = new SimpleClient.SimpleEventHandler();

   public Client(String host, int port, ProtocolVersion version, EncryptionOptions.ClientEncryptionOptions encryptionOptions) {
      super(host, port, version, encryptionOptions);
      this.setEventHandler(this.eventHandler);
   }

   public void run() throws IOException {
      System.out.print("Connecting...");
      this.establishConnection();
      System.out.println();
      BufferedReader in = new BufferedReader(new InputStreamReader(System.in));

      while(true) {
         Event event;
         while((event = (Event)this.eventHandler.queue.poll()) == null) {
            System.out.print(">> ");
            System.out.flush();
            String line = in.readLine();
            if(line == null) {
               this.close();
               return;
            }

            Message.Request req = this.parseLine(line.trim());
            if(req == null) {
               System.out.println("! Error parsing line.");
            } else {
               try {
                  Message.Response resp = this.execute(req);
                  System.out.println("-> " + resp);
               } catch (Exception var6) {
                  JVMStabilityInspector.inspectThrowable(var6);
                  System.err.println("ERROR: " + var6.getMessage());
               }
            }
         }

         System.out.println("<< " + event);
      }
   }

   private Message.Request parseLine(String line) {
      Splitter splitter = Splitter.on(' ').trimResults().omitEmptyStrings();
      Iterator<String> iter = splitter.split(line).iterator();
      if(!iter.hasNext()) {
         return null;
      } else {
         String msgType = ((String)iter.next()).toUpperCase();
         if(msgType.equals("STARTUP")) {
            Map<String, String> options = new HashMap();
            options.put("CQL_VERSION", "3.0.0");

            while(iter.hasNext()) {
               String next = (String)iter.next();
               if(next.toLowerCase().equals("snappy")) {
                  options.put("COMPRESSION", "snappy");
                  this.connection.setCompressor(FrameCompressor.SnappyCompressor.instance);
               }
            }

            return new StartupMessage(options);
         } else {
            String type;
            if(msgType.equals("QUERY")) {
               line = line.substring(6);
               type = line;
               int pageSize = -1;
               if(line.matches(".+ !\\d+$")) {
                  int idx = line.lastIndexOf(33);
                  type = line.substring(0, idx - 1);

                  try {
                     pageSize = Integer.parseInt(line.substring(idx + 1, line.length()));
                  } catch (NumberFormatException var11) {
                     return null;
                  }
               }

               return new QueryMessage(type, QueryOptions.create(ConsistencyLevel.ONE, Collections.emptyList(), false, pageSize, (PagingState)null, (ConsistencyLevel)null, this.version, (String)null));
            } else if(msgType.equals("PREPARE")) {
               type = line.substring(8);
               return new PrepareMessage(type, (String)null);
            } else if(msgType.equals("EXECUTE")) {
               try {
                  byte[] id = Hex.hexToBytes((String)iter.next());
                  byte[] preparedStatementId = Hex.hexToBytes((String)iter.next());

                  ArrayList values;
                  ByteBuffer bb;
                  for(values = new ArrayList(); iter.hasNext(); values.add(bb)) {
                     String next = (String)iter.next();

                     try {
                        int v = Integer.parseInt(next);
                        bb = Int32Type.instance.decompose(Integer.valueOf(v));
                     } catch (NumberFormatException var12) {
                        bb = UTF8Type.instance.decompose(next);
                     }
                  }

                  return new ExecuteMessage(MD5Digest.wrap(id), MD5Digest.wrap(preparedStatementId), QueryOptions.forInternalCalls(ConsistencyLevel.ONE, values));
               } catch (Exception var14) {
                  return null;
               }
            } else if(msgType.equals("OPTIONS")) {
               return new OptionsMessage();
            } else if(msgType.equals("AUTHENTICATE")) {
               Map<String, String> credentials = this.readCredentials(iter);
               if(credentials != null && credentials.containsKey("username") && credentials.containsKey("password")) {
                  return new AuthResponse(this.encodeCredentialsForSasl(credentials));
               } else {
                  System.err.println("[ERROR] Authentication requires both 'username' and 'password'");
                  return null;
               }
            } else if(msgType.equals("REGISTER")) {
               type = line.substring(9).toUpperCase();

               try {
                  return new RegisterMessage(Collections.singletonList(Enum.valueOf(Event.Type.class, type)));
               } catch (IllegalArgumentException var13) {
                  System.err.println("[ERROR] Unknown event type: " + type);
                  return null;
               }
            } else {
               return null;
            }
         }
      }
   }

   private Map<String, String> readCredentials(Iterator<String> iter) {
      HashMap credentials = new HashMap();

      while(iter.hasNext()) {
         String next = (String)iter.next();
         String[] kv = next.split("=");
         if(kv.length != 2) {
            if(!next.isEmpty()) {
               System.err.println("[ERROR] Expected key=val, instead got: " + next);
            }

            return null;
         }

         credentials.put(kv[0], kv[1]);
      }

      return credentials;
   }

   public static void main(String[] args) throws Exception {
      DatabaseDescriptor.clientInitialization();
      if(args.length >= 2 && args.length <= 3) {
         String host = args[0];
         int port = Integer.parseInt(args[1]);
         ProtocolVersion version = args.length == 3?ProtocolVersion.decode(Integer.parseInt(args[2])):ProtocolVersion.CURRENT;
         EncryptionOptions.ClientEncryptionOptions encryptionOptions = new EncryptionOptions.ClientEncryptionOptions();
         System.out.println("CQL binary protocol console " + host + "@" + port + " using native protocol version " + version);
         (new Client(host, port, version, encryptionOptions)).run();
         System.exit(0);
      } else {
         System.err.println("Usage: " + Client.class.getSimpleName() + " <host> <port> [<version>]");
      }
   }
}
