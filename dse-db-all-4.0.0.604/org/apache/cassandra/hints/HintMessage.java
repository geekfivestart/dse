package org.apache.cassandra.hints;

import com.google.common.primitives.Ints;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.cassandra.concurrent.SchedulableMessage;
import org.apache.cassandra.concurrent.StagedScheduler;
import org.apache.cassandra.concurrent.TPC;
import org.apache.cassandra.concurrent.TPCTaskType;
import org.apache.cassandra.concurrent.TracingAwareExecutor;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.exceptions.UnknownTableException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.TrackedDataInputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public abstract class HintMessage implements SchedulableMessage {
   public static final Versioned<HintsVerbs.HintsVersion, Serializer<HintMessage>> serializers = HintsVerbs.HintsVersion.versioned((x$0) -> {
      return new HintMessage.HintSerializer(x$0);
   });
   final UUID hostId;
   final transient StagedScheduler scheduler;
   final transient TracingAwareExecutor requestExecutor;
   final transient TracingAwareExecutor responseExecutor;

   HintMessage(UUID hostId, StagedScheduler scheduler) {
      this.hostId = hostId;
      this.scheduler = scheduler;
      this.requestExecutor = scheduler.forTaskType(TPCTaskType.HINT_DISPATCH);
      this.responseExecutor = scheduler.forTaskType(TPCTaskType.HINT_RESPONSE);
   }

   public static HintMessage create(UUID hostId, Hint hint) {
      return new HintMessage.Simple(hostId, hint);
   }

   static HintMessage createEncoded(UUID hostId, ByteBuffer hint, HintsVerbs.HintsVersion version) {
      return new HintMessage.Encoded(hostId, hint, version);
   }

   public StagedScheduler getScheduler() {
      return this.scheduler;
   }

   public TracingAwareExecutor getRequestExecutor() {
      return this.requestExecutor;
   }

   public TracingAwareExecutor getResponseExecutor() {
      return this.responseExecutor;
   }

   public abstract Hint hint() throws UnknownTableException;

   abstract long getHintCreationTime();

   protected abstract long serializedSize(HintsVerbs.HintsVersion var1);

   protected abstract void serialize(DataOutputPlus var1, HintsVerbs.HintsVersion var2) throws IOException;

   private static class HintSerializer extends VersionDependent<HintsVerbs.HintsVersion> implements Serializer<HintMessage> {
      private HintSerializer(HintsVerbs.HintsVersion version) {
         super(version);
      }

      public long serializedSize(HintMessage message) {
         return UUIDSerializer.serializer.serializedSize(message.hostId) + message.serializedSize((HintsVerbs.HintsVersion)this.version);
      }

      public void serialize(HintMessage message, DataOutputPlus out) throws IOException {
         UUIDSerializer.serializer.serialize(message.hostId, out);
         message.serialize(out, (HintsVerbs.HintsVersion)this.version);
      }

      public HintMessage deserialize(DataInputPlus in) throws IOException {
         UUID hostId = UUIDSerializer.serializer.deserialize(in);
         return HintMessage.Simple.deserialize(hostId, in, (HintsVerbs.HintsVersion)this.version);
      }
   }

   private static class Encoded extends HintMessage {
      private final ByteBuffer hint;
      private final HintsVerbs.HintsVersion version;

      private Encoded(UUID hostId, ByteBuffer hint, HintsVerbs.HintsVersion version) {
         super(hostId, TPC.bestTPCScheduler());
         this.hint = hint;
         this.version = version;
      }

      public Hint hint() {
         throw new UnsupportedOperationException();
      }

      long getHintCreationTime() {
         return ((Hint.HintSerializer)Hint.serializers.get(this.version)).getHintCreationTime(this.hint);
      }

      protected long serializedSize(HintsVerbs.HintsVersion version) {
         if(this.version != version) {
            throw new IllegalArgumentException("serializedSize() called with non-matching version " + version);
         } else {
            return (long)(TypeSizes.sizeofUnsignedVInt((long)this.hint.remaining()) + this.hint.remaining());
         }
      }

      protected void serialize(DataOutputPlus out, HintsVerbs.HintsVersion version) throws IOException {
         if(this.version != version) {
            throw new IllegalArgumentException("serialize() called with non-matching version " + version);
         } else {
            out.writeUnsignedVInt((long)this.hint.remaining());
            out.write(this.hint);
         }
      }
   }

   private static class Simple extends HintMessage {
      @Nullable
      private final Hint hint;
      @Nullable
      private final TableId unknownTableID;

      private Simple(UUID hostId, Hint hint) {
         super(hostId, hint.mutation.getScheduler());
         this.hint = hint;
         this.unknownTableID = null;
      }

      private Simple(UUID hostId, TableId unknownTableID) {
         super(hostId, TPC.bestTPCScheduler());
         this.hint = null;
         this.unknownTableID = unknownTableID;
      }

      public Hint hint() throws UnknownTableException {
         if(this.unknownTableID != null) {
            throw new UnknownTableException(this.unknownTableID);
         } else {
            return this.hint;
         }
      }

      long getHintCreationTime() {
         return this.hint.creationTime;
      }

      protected long serializedSize(HintsVerbs.HintsVersion version) {
         Objects.requireNonNull(this.hint);
         long hintSize = ((Hint.HintSerializer)Hint.serializers.get(version)).serializedSize(this.hint);
         return (long)TypeSizes.sizeofUnsignedVInt(hintSize) + hintSize;
      }

      protected void serialize(DataOutputPlus out, HintsVerbs.HintsVersion version) throws IOException {
         Objects.requireNonNull(this.hint);
         out.writeUnsignedVInt(((Hint.HintSerializer)Hint.serializers.get(version)).serializedSize(this.hint));
         ((Hint.HintSerializer)Hint.serializers.get(version)).serialize(this.hint, out);
      }

      static HintMessage.Simple deserialize(UUID hostId, DataInputPlus in, HintsVerbs.HintsVersion version) throws IOException {
         long hintSize = in.readUnsignedVInt();
         TrackedDataInputPlus countingIn = new TrackedDataInputPlus(in);

         try {
            return new HintMessage.Simple(hostId, ((Hint.HintSerializer)Hint.serializers.get(version)).deserialize(countingIn));
         } catch (UnknownTableException var7) {
            in.skipBytes(Ints.checkedCast(hintSize - countingIn.getBytesRead()));
            return new HintMessage.Simple(hostId, var7.id);
         }
      }
   }
}
