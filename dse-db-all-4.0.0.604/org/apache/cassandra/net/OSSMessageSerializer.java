package org.apache.cassandra.net;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.Map.Entry;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.db.monitoring.ApproximateTime;
import org.apache.cassandra.exceptions.RequestFailureReason;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.FastByteArrayInputStream;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.repair.messages.RepairVerbs;
import org.apache.cassandra.tracing.Tracing;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.UUIDGen;

public class OSSMessageSerializer implements Message.Serializer {
   private static final OSSMessageSerializer.OSSVerb[] LEGACY_VERB_VALUES = OSSMessageSerializer.OSSVerb.values();
   private static final Map<Verb<?, ?>, OSSMessageSerializer.OSSVerb> definitionToVerb;
   private static final BiMap<Verb<?, ?>, Integer> repairVerbToLegacyCode;
   private static final String TRACE_HEADER = "TraceSession";
   private static final String TRACE_TYPE = "TraceType";
   private static final String FORWARD_FROM = "FWD_FRM";
   private static final String FORWARD_TO = "FWD_TO";
   private static final String FAILURE_CALLBACK_PARAM = "CAL_BAC";
   private static final byte[] ONE_BYTE;
   private static final String FAILURE_RESPONSE_PARAM = "FAIL";
   private static final String FAILURE_REASON_PARAM = "FAIL_REASON";
   private final MessagingVersion version;

   OSSMessageSerializer(MessagingVersion version) {
      this.version = version;
   }

   public void writeSerializedSize(int serializedSize, DataOutputPlus out) {
   }

   public int readSerializedSize(DataInputPlus in) {
      return -1;
   }

   public void serialize(Message message, DataOutputPlus out) throws IOException {
      OSSMessageSerializer.OSSVerb ossVerb = this.computeLegacyVerb(message);
      if(ossVerb == null) {
         throw new IllegalStateException(String.format("Cannot write message %s to legacy node", new Object[]{message}));
      } else {
         out.writeInt(-900387334);
         out.writeInt(message.id());
         out.writeInt((int)message.operationStartMillis());
         CompactEndpointSerializationHelper.serialize(message.from(), out);
         out.writeInt(ossVerb.ordinal());
         Map<String, byte[]> parameters = this.computeLegacyParameters(message);
         out.writeInt(parameters.size());
         Iterator var5 = parameters.entrySet().iterator();

         while(var5.hasNext()) {
            Entry<String, byte[]> entry = (Entry)var5.next();
            out.writeUTF((String)entry.getKey());
            out.writeInt(((byte[])entry.getValue()).length);
            out.write((byte[])entry.getValue());
         }

         if(message.payload() != null) {
            VerbSerializer<?, ?> verbSerializer = this.version.serializer(message.verb());
            Serializer serializer = message.isRequest()?verbSerializer.requestSerializer:verbSerializer.responseSerializer;
            DataOutputBuffer dob = (DataOutputBuffer)DataOutputBuffer.scratchBuffer.get();
            Throwable var8 = null;

            try {
               if(message.group() == Verbs.REPAIR) {
                  dob.write(((Integer)repairVerbToLegacyCode.get(message.verb())).intValue());
               }

               serializer.serialize(message.payload(), dob);
               int size = dob.getLength();
               out.writeInt(size);
               out.write(dob.getData(), 0, size);
            } catch (Throwable var17) {
               var8 = var17;
               throw var17;
            } finally {
               if(dob != null) {
                  if(var8 != null) {
                     try {
                        dob.close();
                     } catch (Throwable var16) {
                        var8.addSuppressed(var16);
                     }
                  } else {
                     dob.close();
                  }
               }

            }
         } else {
            out.writeInt(0);
         }

      }
   }

   private OSSMessageSerializer.OSSVerb computeLegacyVerb(Message message) {
      return message.isResponse()?(this.wasUsingAndInternalResponse((Response)message)?OSSMessageSerializer.OSSVerb.INTERNAL_RESPONSE:OSSMessageSerializer.OSSVerb.REQUEST_RESPONSE):(OSSMessageSerializer.OSSVerb)definitionToVerb.get(message.verb());
   }

   private boolean wasUsingAndInternalResponse(Response<?> response) {
      if(response.isFailure()) {
         return true;
      } else {
         Verb<?, ?> def = response.verb();
         VerbGroup<?> group = def.group();
         return group != Verbs.SCHEMA && group != Verbs.REPAIR?(group != Verbs.OPERATIONS?false:def == Verbs.OPERATIONS.SNAPSHOT || def == Verbs.OPERATIONS.REPLICATION_FINISHED):true;
      }
   }

   private Map<String, byte[]> computeLegacyParameters(Message message) {
      Map<String, byte[]> params = new HashMap();
      Tracing.SessionInfo info = message.tracingInfo();
      if(info != null) {
         params.put("TraceSession", UUIDGen.decompose(info.sessionId));
         params.put("TraceType", new byte[]{Tracing.TraceType.serialize(info.traceType)});
      }

      if(message.isRequest()) {
         Request<?, ?> request = (Request)message;
         if(!request.verb().isOneWay()) {
            params.put("CAL_BAC", ONE_BYTE);
         }

         List<Request.Forward> forwards = request.forwards();
         if(!forwards.isEmpty()) {
            try {
               DataOutputBuffer out = new DataOutputBuffer();
               Throwable var7 = null;

               try {
                  out.writeInt(forwards.size());
                  Iterator var8 = forwards.iterator();

                  while(var8.hasNext()) {
                     Request.Forward forward = (Request.Forward)var8.next();
                     CompactEndpointSerializationHelper.serialize(forward.to, out);
                     out.writeInt(forward.id);
                  }

                  params.put("FWD_TO", out.getData());
               } catch (Throwable var18) {
                  var7 = var18;
                  throw var18;
               } finally {
                  if(out != null) {
                     if(var7 != null) {
                        try {
                           out.close();
                        } catch (Throwable var17) {
                           var7.addSuppressed(var17);
                        }
                     } else {
                        out.close();
                     }
                  }

               }
            } catch (IOException var20) {
               throw new AssertionError(var20);
            }
         }

         if(request.isForwarded()) {
            params.put("FWD_FRM", ((ForwardRequest)request).replyTo.getAddress());
         }
      } else {
         Response<?> response = (Response)message;
         if(response.isFailure()) {
            params.put("FAIL", ONE_BYTE);
            RequestFailureReason reason = ((FailureResponse)response).reason();
            params.put("FAIL_REASON", ByteBufferUtil.getArray(ByteBufferUtil.bytes((short)reason.codeForInternodeProtocol(this.version))));
         }
      }

      return params;
   }

   public long serializedSize(Message message) {
      OSSMessageSerializer.OSSVerb ossVerb = this.computeLegacyVerb(message);
      if(ossVerb == null) {
         throw new IllegalStateException(String.format("Cannot write message %s to legacy node", new Object[]{message}));
      } else {
         long size = 12L;
         size += (long)CompactEndpointSerializationHelper.serializedSize(message.from());
         size += 4L;
         Map<String, byte[]> parameters = this.computeLegacyParameters(message);
         size += 4L;

         Entry entry;
         for(Iterator var6 = parameters.entrySet().iterator(); var6.hasNext(); size += (long)((byte[])entry.getValue()).length) {
            entry = (Entry)var6.next();
            size += (long)TypeSizes.sizeof((String)entry.getKey());
            size += 4L;
         }

         if(message.group() == Verbs.REPAIR) {
            ++size;
         }

         if(message.payload() != null) {
            size += message.payloadSerializedSize(this.version);
         }

         return size;
      }
   }

   public Message deserialize(TrackedDataInputPlus in, int size, InetAddress from) throws IOException {
      MessagingService.validateMagic(in.readInt());
      int id = in.readInt();
      long timestamp = this.deserializeTimestampPre40(in, from);
      CompactEndpointSerializationHelper.deserialize(in);
      OSSMessageSerializer.OSSVerb ossVerb = LEGACY_VERB_VALUES[in.readInt()];
      int parameterCount = in.readInt();
      Map<String, byte[]> rawParameters = new HashMap();

      for(int i = 0; i < parameterCount; ++i) {
         String key = in.readUTF();
         byte[] value = new byte[in.readInt()];
         in.readFully(value);
         rawParameters.put(key, value);
      }

      Tracing.SessionInfo tracingInfo = this.extractAndRemoveTracingInfo(rawParameters);
      int payloadSize = in.readInt();
      long startBytes = in.getBytesRead();
      boolean isResponse = ossVerb == OSSMessageSerializer.OSSVerb.INTERNAL_RESPONSE || ossVerb == OSSMessageSerializer.OSSVerb.REQUEST_RESPONSE;
      Message.Data data;
      long timeoutMillis;
      if(isResponse) {
         CallbackInfo<?> info = MessagingService.instance().getRegisteredCallback(id, false, from);
         if(info == null) {
            in.skipBytesFully(payloadSize);
            return null;
         } else {
            Verb<?, ?> verb = info.verb;
            timeoutMillis = 9223372036854775807L;
            if(rawParameters.containsKey("FAIL")) {
               rawParameters.remove("FAIL");
               RequestFailureReason reason = rawParameters.containsKey("FAIL_REASON")?RequestFailureReason.fromCode(ByteBufferUtil.toShort(ByteBuffer.wrap((byte[])rawParameters.remove("FAIL_REASON")))):RequestFailureReason.UNKNOWN;
               data = new Message.Data((Object)null, -1L, timestamp, timeoutMillis, MessageParameters.from(rawParameters), tracingInfo);
               return new FailureResponse(from, FBUtilities.getBroadcastAddress(), id, verb, reason, data);
            } else {
               try {
                  Object payload = this.version.serializer(verb).responseSerializer.deserialize(in);
                  data = new Message.Data(payload, (long)payloadSize, timestamp, timeoutMillis, MessageParameters.from(rawParameters), tracingInfo);
                  return new Response(from, FBUtilities.getBroadcastAddress(), id, verb, data);
               } catch (Exception var21) {
                  throw MessageDeserializationException.forResponsePayloadDeserializationException(var21, from, verb, payloadSize - (int)(in.getBytesRead() - startBytes));
               }
            }
         }
      } else {
         rawParameters.remove("FAIL_REASON");
         Verb<?, ?> verb = ossVerb == OSSMessageSerializer.OSSVerb.REPAIR_MESSAGE?(Verb)repairVerbToLegacyCode.inverse().get(Integer.valueOf(in.readByte())):ossVerb.verb;

         assert verb != null : "Unknown definition for verb " + ossVerb;

         try {
            Object payload = this.version.serializer(verb).requestSerializer.deserialize(in);
            timeoutMillis = verb.isOneWay()?-1L:verb.timeoutSupplier().get(payload);
            if(rawParameters.containsKey("FWD_FRM")) {
               InetAddress replyTo = InetAddress.getByAddress((byte[])rawParameters.remove("FWD_FRM"));
               data = new Message.Data(payload, (long)payloadSize, timestamp, timeoutMillis, MessageParameters.from(rawParameters), tracingInfo);
               return new ForwardRequest(from, FBUtilities.getBroadcastAddress(), replyTo, id, verb, data);
            } else {
               List<Request.Forward> forwards = this.extractAndRemoveForwards(rawParameters);
               data = new Message.Data(payload, (long)payloadSize, timestamp, timeoutMillis, MessageParameters.from(rawParameters), tracingInfo);
               return (Message)(verb.isOneWay()?new OneWayRequest(from, Request.local, (Verb.OneWay)verb, data, forwards):new Request(from, Request.local, id, verb, data, forwards));
            }
         } catch (Exception var22) {
            int remainingBytes = payloadSize - (int)(in.getBytesRead() - startBytes);
            throw MessageDeserializationException.forRequestPayloadDeserializationException(var22, from, id, verb, timestamp, -1L, MessageParameters.from(rawParameters), tracingInfo, remainingBytes);
         }
      }
   }

   private Tracing.SessionInfo extractAndRemoveTracingInfo(Map<String, byte[]> parameters) {
      if(!parameters.containsKey("TraceSession")) {
         return null;
      } else {
         UUID sessionId = UUIDGen.getUUID(ByteBuffer.wrap((byte[])parameters.remove("TraceSession")));
         Tracing.TraceType traceType = Tracing.TraceType.QUERY;
         if(parameters.containsKey("TraceType")) {
            traceType = Tracing.TraceType.deserialize(((byte[])parameters.remove("TraceType"))[0]);
         }

         return new Tracing.SessionInfo(sessionId, traceType);
      }
   }

   private List<Request.Forward> extractAndRemoveForwards(Map<String, byte[]> parameters) {
      if(!parameters.containsKey("FWD_TO")) {
         return Collections.emptyList();
      } else {
         try {
            DataInputStream in = new DataInputStream(new FastByteArrayInputStream((byte[])parameters.remove("FWD_TO")));
            Throwable var3 = null;

            try {
               int size = in.readInt();
               List<Request.Forward> forwards = new ArrayList(size);

               for(int i = 0; i < size; ++i) {
                  InetAddress address = CompactEndpointSerializationHelper.deserialize(in);
                  int id = in.readInt();
                  forwards.add(new Request.Forward(address, id));
               }

               ArrayList var20 = forwards;
               return var20;
            } catch (Throwable var17) {
               var3 = var17;
               throw var17;
            } finally {
               if(in != null) {
                  if(var3 != null) {
                     try {
                        in.close();
                     } catch (Throwable var16) {
                        var3.addSuppressed(var16);
                     }
                  } else {
                     in.close();
                  }
               }

            }
         } catch (IOException var19) {
            throw new AssertionError();
         }
      }
   }

   private long deserializeTimestampPre40(DataInputPlus in, InetAddress from) throws IOException {
      int partial = in.readInt();
      long currentTime = ApproximateTime.currentTimeMillis();
      long sentConstructionTime = currentTime & -4294967296L | ((long)partial & 4294967295L) << 2 >> 2;
      long elapsed = currentTime - sentConstructionTime;
      if(elapsed > 0L) {
         MessagingService.instance().metrics.addTimeTaken(from, elapsed);
      }

      boolean useSentTime = DatabaseDescriptor.hasCrossNodeTimeout() && elapsed > 0L;
      return useSentTime?sentConstructionTime + DatabaseDescriptor.getEndpointSnitch().getCrossDcRttLatency(from) / 2L:currentTime;
   }

   static {
      Builder<Verb<?, ?>, OSSMessageSerializer.OSSVerb> builder = ImmutableMap.builder();
      OSSMessageSerializer.OSSVerb[] var1 = OSSMessageSerializer.OSSVerb.values();
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         OSSMessageSerializer.OSSVerb ossVerb = var1[var3];
         if(ossVerb.verb != null) {
            builder.put(ossVerb.verb, ossVerb);
         }
      }

      Iterator var6 = Verbs.REPAIR.iterator();

      while(var6.hasNext()) {
         Verb<?, ?> msg = (Verb)var6.next();
         builder.put(msg, OSSMessageSerializer.OSSVerb.REPAIR_MESSAGE);
      }

      builder.put(Verbs.WRITES.VIEW_WRITE, OSSMessageSerializer.OSSVerb.MUTATION);
      definitionToVerb = builder.build();
      repairVerbToLegacyCode = HashBiMap.create();
      RepairVerbs rm = Verbs.REPAIR;
      repairVerbToLegacyCode.put(rm.VALIDATION_REQUEST, Integer.valueOf(0));
      repairVerbToLegacyCode.put(rm.VALIDATION_COMPLETE, Integer.valueOf(1));
      repairVerbToLegacyCode.put(rm.SYNC_REQUEST, Integer.valueOf(2));
      repairVerbToLegacyCode.put(rm.SYNC_COMPLETE, Integer.valueOf(3));
      repairVerbToLegacyCode.put(rm.PREPARE, Integer.valueOf(5));
      repairVerbToLegacyCode.put(rm.SNAPSHOT, Integer.valueOf(6));
      repairVerbToLegacyCode.put(rm.CLEANUP, Integer.valueOf(7));
      repairVerbToLegacyCode.put(rm.CONSISTENT_REQUEST, Integer.valueOf(8));
      repairVerbToLegacyCode.put(rm.CONSISTENT_RESPONSE, Integer.valueOf(9));
      repairVerbToLegacyCode.put(rm.FINALIZE_COMMIT, Integer.valueOf(10));
      repairVerbToLegacyCode.put(rm.FAILED_SESSION, Integer.valueOf(11));
      repairVerbToLegacyCode.put(rm.STATUS_REQUEST, Integer.valueOf(12));
      ONE_BYTE = new byte[1];
   }

   private static enum OSSVerb {
      MUTATION(Verbs.WRITES.WRITE),
      HINT(Verbs.HINTS.HINT),
      READ_REPAIR(Verbs.WRITES.READ_REPAIR),
      READ(Verbs.READS.SINGLE_READ),
      REQUEST_RESPONSE((Verb)null),
      BATCH_STORE(Verbs.WRITES.BATCH_STORE),
      BATCH_REMOVE(Verbs.WRITES.BATCH_REMOVE),
      /** @deprecated */
      @Deprecated
      STREAM_REPLY((Verb)null),
      /** @deprecated */
      @Deprecated
      STREAM_REQUEST((Verb)null),
      RANGE_SLICE(Verbs.READS.RANGE_READ),
      /** @deprecated */
      @Deprecated
      BOOTSTRAP_TOKEN((Verb)null),
      /** @deprecated */
      @Deprecated
      TREE_REQUEST((Verb)null),
      /** @deprecated */
      @Deprecated
      TREE_RESPONSE((Verb)null),
      /** @deprecated */
      @Deprecated
      JOIN((Verb)null),
      GOSSIP_DIGEST_SYN(Verbs.GOSSIP.SYN),
      GOSSIP_DIGEST_ACK(Verbs.GOSSIP.ACK),
      GOSSIP_DIGEST_ACK2(Verbs.GOSSIP.ACK2),
      /** @deprecated */
      @Deprecated
      DEFINITIONS_ANNOUNCE((Verb)null),
      DEFINITIONS_UPDATE(Verbs.SCHEMA.PUSH),
      TRUNCATE(Verbs.OPERATIONS.TRUNCATE),
      SCHEMA_CHECK(Verbs.SCHEMA.VERSION),
      /** @deprecated */
      @Deprecated
      INDEX_SCAN((Verb)null),
      REPLICATION_FINISHED(Verbs.OPERATIONS.REPLICATION_FINISHED),
      INTERNAL_RESPONSE((Verb)null),
      COUNTER_MUTATION(Verbs.WRITES.COUNTER_FORWARDING),
      /** @deprecated */
      @Deprecated
      STREAMING_REPAIR_REQUEST((Verb)null),
      /** @deprecated */
      @Deprecated
      STREAMING_REPAIR_RESPONSE((Verb)null),
      SNAPSHOT(Verbs.OPERATIONS.SNAPSHOT),
      MIGRATION_REQUEST(Verbs.SCHEMA.PULL),
      GOSSIP_SHUTDOWN(Verbs.GOSSIP.SHUTDOWN),
      _TRACE((Verb)null),
      ECHO(Verbs.GOSSIP.ECHO),
      REPAIR_MESSAGE((Verb)null),
      PAXOS_PREPARE(Verbs.LWT.PREPARE),
      PAXOS_PROPOSE(Verbs.LWT.PROPOSE),
      PAXOS_COMMIT(Verbs.LWT.COMMIT),
      /** @deprecated */
      @Deprecated
      PAGED_RANGE((Verb)null),
      UNUSED_1((Verb)null),
      UNUSED_2((Verb)null),
      UNUSED_3((Verb)null),
      UNUSED_4((Verb)null),
      UNUSED_5((Verb)null);

      private final Verb<?, ?> verb;

      private OSSVerb(Verb<?, ?> verb) {
         this.verb = verb;
      }
   }
}
