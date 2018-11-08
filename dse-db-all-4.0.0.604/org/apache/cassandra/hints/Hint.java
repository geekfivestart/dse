package org.apache.cassandra.hints;

import com.google.common.base.Throwables;
import com.google.common.primitives.Ints;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import javax.annotation.Nullable;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.SystemKeyspace;
import org.apache.cassandra.db.TypeSizes;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public final class Hint {
   public static final Versioned<HintsVerbs.HintsVersion, Hint.HintSerializer> serializers = HintsVerbs.HintsVersion.versioned((x$0) -> {
      return new Hint.HintSerializer(x$0);
   });
   static final int maxHintTTL = Integer.getInteger("cassandra.maxHintTTL", 2147483647).intValue();
   final Mutation mutation;
   final long creationTime;
   final int gcgs;

   private Hint(Mutation mutation, long creationTime, int gcgs) {
      this.mutation = mutation;
      this.creationTime = creationTime;
      this.gcgs = gcgs;
   }

   public static Hint create(Mutation mutation, long creationTime) {
      return new Hint(mutation, creationTime, mutation.smallestGCGS());
   }

   public static Hint create(Mutation mutation, long creationTime, int gcgs) {
      return new Hint(mutation, creationTime, gcgs);
   }

   CompletableFuture<?> applyFuture() {
      if(this.isLive()) {
         Mutation filtered = this.mutation;
         Iterator var2 = this.mutation.getTableIds().iterator();

         while(var2.hasNext()) {
            TableId id = (TableId)var2.next();
            if(this.creationTime <= SystemKeyspace.getTruncatedAt(id)) {
               filtered = filtered.without(id);
            }
         }

         if(!filtered.isEmpty()) {
            return filtered.applyFuture();
         }
      }

      return CompletableFuture.completedFuture((Object)null);
   }

   void apply() {
      try {
         this.applyFuture().get();
      } catch (Exception var2) {
         throw Throwables.propagate(var2.getCause());
      }
   }

   int ttl() {
      return Math.min(this.gcgs, this.mutation.smallestGCGS());
   }

   boolean isLive() {
      return isLive(this.creationTime, System.currentTimeMillis(), this.ttl());
   }

   static boolean isLive(long creationTime, long now, int hintTTL) {
      long expirationTime = creationTime + TimeUnit.SECONDS.toMillis((long)Math.min(hintTTL, maxHintTTL));
      return expirationTime > now;
   }

   static final class HintSerializer extends VersionDependent<HintsVerbs.HintsVersion> implements Serializer<Hint> {
      private HintSerializer(HintsVerbs.HintsVersion version) {
         super(version);
      }

      public long serializedSize(Hint hint) {
         long size = (long)TypeSizes.sizeof(hint.creationTime);
         size += (long)TypeSizes.sizeofUnsignedVInt((long)hint.gcgs);
         size += ((Mutation.MutationSerializer)Mutation.rawSerializers.get(((HintsVerbs.HintsVersion)this.version).encodingVersion)).serializedSize(hint.mutation);
         return size;
      }

      public void serialize(Hint hint, DataOutputPlus out) throws IOException {
         out.writeLong(hint.creationTime);
         out.writeUnsignedVInt((long)hint.gcgs);
         ((Mutation.MutationSerializer)Mutation.rawSerializers.get(((HintsVerbs.HintsVersion)this.version).encodingVersion)).serialize(hint.mutation, out);
      }

      public Hint deserialize(DataInputPlus in) throws IOException {
         long creationTime = in.readLong();
         int gcgs = (int)in.readUnsignedVInt();
         return new Hint(((Mutation.MutationSerializer)Mutation.rawSerializers.get(((HintsVerbs.HintsVersion)this.version).encodingVersion)).deserialize(in), creationTime, gcgs);
      }

      long getHintCreationTime(ByteBuffer hintBuffer) {
         return hintBuffer.getLong(0);
      }

      @Nullable
      Hint deserializeIfLive(DataInputPlus in, long now, long size) throws IOException {
         long creationTime = in.readLong();
         int gcgs = (int)in.readUnsignedVInt();
         int bytesRead = TypeSizes.sizeof(creationTime) + TypeSizes.sizeofUnsignedVInt((long)gcgs);
         if(Hint.isLive(creationTime, now, gcgs)) {
            return new Hint(((Mutation.MutationSerializer)Mutation.rawSerializers.get(((HintsVerbs.HintsVersion)this.version).encodingVersion)).deserialize(in), creationTime, gcgs);
         } else {
            in.skipBytesFully(Ints.checkedCast(size) - bytesRead);
            return null;
         }
      }

      @Nullable
      ByteBuffer readBufferIfLive(DataInputPlus in, long now, int size) throws IOException {
         int maxHeaderSize = Math.min(TypeSizes.sizeof(9223372036854775807L) + 10, size);
         byte[] header = new byte[maxHeaderSize];
         in.readFully(header);
         DataInputBuffer input = new DataInputBuffer(header);
         Throwable var8 = null;

         try {
            long creationTime = input.readLong();
            int gcgs = (int)input.readUnsignedVInt();
            if(!Hint.isLive(creationTime, now, gcgs)) {
               in.skipBytesFully(size - maxHeaderSize);
               Object var12 = null;
               return (ByteBuffer)var12;
            }
         } catch (Throwable var22) {
            var8 = var22;
            throw var22;
         } finally {
            if(input != null) {
               if(var8 != null) {
                  try {
                     input.close();
                  } catch (Throwable var21) {
                     var8.addSuppressed(var21);
                  }
               } else {
                  input.close();
               }
            }

         }

         byte[] bytes = new byte[size];
         System.arraycopy(header, 0, bytes, 0, header.length);
         in.readFully(bytes, header.length, size - header.length);
         return ByteBuffer.wrap(bytes);
      }
   }
}
