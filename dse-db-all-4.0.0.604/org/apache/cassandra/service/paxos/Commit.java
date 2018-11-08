package org.apache.cassandra.service.paxos;

import java.io.IOException;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.partitions.PartitionUpdate;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public class Commit {
   public static final Versioned<LWTVerbs.LWTVersion, Commit.CommitSerializer> serializers = LWTVerbs.LWTVersion.versioned((x$0) -> {
      return new Commit.CommitSerializer(x$0);
   });
   public final UUID ballot;
   public final PartitionUpdate update;

   public Commit(UUID ballot, PartitionUpdate update) {
      assert ballot != null;

      assert update != null;

      this.ballot = ballot;
      this.update = update;
   }

   public static Commit newPrepare(DecoratedKey key, TableMetadata metadata, UUID ballot) {
      return new Commit(ballot, PartitionUpdate.emptyUpdate(metadata, key));
   }

   public static Commit newProposal(UUID ballot, PartitionUpdate update) {
      update.updateAllTimestamp(UUIDGen.microsTimestamp(ballot));
      return new Commit(ballot, update);
   }

   public static Commit emptyCommit(DecoratedKey key, TableMetadata metadata) {
      return new Commit(UUIDGen.minTimeUUID(0L), PartitionUpdate.emptyUpdate(metadata, key));
   }

   public boolean isAfter(Commit other) {
      return this.ballot.timestamp() > other.ballot.timestamp();
   }

   public boolean hasBallot(UUID ballot) {
      return this.ballot.equals(ballot);
   }

   public Mutation makeMutation() {
      return new Mutation(this.update);
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         Commit commit = (Commit)o;
         return this.ballot.equals(commit.ballot) && this.update.equals(commit.update);
      } else {
         return false;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.ballot, this.update});
   }

   public String toString() {
      return String.format("Commit(%s, %s)", new Object[]{this.ballot, this.update});
   }

   public static class CommitSerializer extends VersionDependent<LWTVerbs.LWTVersion> implements Serializer<Commit> {
      private CommitSerializer(LWTVerbs.LWTVersion version) {
         super(version);
      }

      public void serialize(Commit commit, DataOutputPlus out) throws IOException {
         UUIDSerializer.serializer.serialize(commit.ballot, out);
         ((PartitionUpdate.PartitionUpdateSerializer)PartitionUpdate.serializers.get(((LWTVerbs.LWTVersion)this.version).encodingVersion)).serialize(commit.update, out);
      }

      public Commit deserialize(DataInputPlus in) throws IOException {
         UUID ballot = UUIDSerializer.serializer.deserialize(in);
         PartitionUpdate update = ((PartitionUpdate.PartitionUpdateSerializer)PartitionUpdate.serializers.get(((LWTVerbs.LWTVersion)this.version).encodingVersion)).deserialize(in, SerializationHelper.Flag.LOCAL);
         return new Commit(ballot, update);
      }

      public long serializedSize(Commit commit) {
         return UUIDSerializer.serializer.serializedSize(commit.ballot) + ((PartitionUpdate.PartitionUpdateSerializer)PartitionUpdate.serializers.get(((LWTVerbs.LWTVersion)this.version).encodingVersion)).serializedSize(commit.update);
      }
   }
}
