package com.datastax.bdp.db.nodesync;

import com.google.common.collect.Sets;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Date;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;
import org.apache.cassandra.db.marshal.UserType;
import org.apache.cassandra.repair.SystemDistributedKeyspace;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.units.TimeValue;

public class ValidationInfo {
   final long startedAt;
   final ValidationOutcome outcome;
   @Nullable
   final Set<InetAddress> missingNodes;

   ValidationInfo(long startedAt, ValidationOutcome outcome, Set<InetAddress> missingNodes) {
      this.startedAt = startedAt;
      this.outcome = outcome;
      this.missingNodes = outcome.wasPartial()?(missingNodes == null?Collections.emptySet():missingNodes):null;
   }

   public boolean wasSuccessful() {
      return this.outcome.wasSuccessful();
   }

   public static ValidationInfo fromBytes(ByteBuffer bytes) {
      UserType type = SystemDistributedKeyspace.NodeSyncValidation;
      ByteBuffer[] values = type.split(bytes);
      if(values.length < type.size()) {
         throw new IllegalArgumentException(String.format("Invalid number of components for nodesync_validation, expected %d but got %d", new Object[]{Integer.valueOf(type.size()), Integer.valueOf(values.length)}));
      } else {
         try {
            long startedAt = ((Date)type.composeField(0, values[0])).getTime();
            ValidationOutcome outcome = ValidationOutcome.fromCode(((Byte)type.composeField(1, values[1])).byteValue());
            Set<InetAddress> missingNodes = values[2] == null?null:(Set)type.composeField(2, values[2]);
            return new ValidationInfo(startedAt, outcome, missingNodes);
         } catch (MarshalException var7) {
            throw new IllegalArgumentException("Error deserializing nodesync_validation from " + ByteBufferUtil.toDebugHexString(bytes), var7);
         }
      }
   }

   public ByteBuffer toBytes() {
      UserType type = SystemDistributedKeyspace.NodeSyncValidation;
      ByteBuffer[] values = new ByteBuffer[type.size()];
      values[0] = type.decomposeField(0, new Date(this.startedAt));
      values[1] = type.decomposeField(1, Byte.valueOf(this.outcome.code()));
      values[2] = this.missingNodes == null?null:type.decomposeField(2, this.missingNodes);
      return UserType.buildValue(values);
   }

   public boolean isMoreRecentThan(ValidationInfo other) {
      return this.startedAt >= other.startedAt;
   }

   ValidationInfo composeWith(ValidationInfo other) {
      long composedStartTime = Math.min(this.startedAt, other.startedAt);
      ValidationOutcome composedOutcome = this.outcome.composeWith(other.outcome);
      Set<InetAddress> composedMissingNodes = null;
      if(composedOutcome.wasPartial()) {
         composedMissingNodes = this.outcome.wasPartial()?(other.outcome.wasPartial()?Sets.union(this.missingNodes, other.missingNodes):this.missingNodes):other.missingNodes;
      }

      return new ValidationInfo(composedStartTime, composedOutcome, (Set)composedMissingNodes);
   }

   public final int hashCode() {
      return Objects.hash(new Object[]{Long.valueOf(this.startedAt), this.outcome, this.missingNodes});
   }

   public boolean equals(Object o) {
      if(!(o instanceof ValidationInfo)) {
         return false;
      } else {
         ValidationInfo that = (ValidationInfo)o;
         return this.startedAt == that.startedAt && this.outcome == that.outcome && Objects.equals(this.missingNodes, that.missingNodes);
      }
   }

   public String toString() {
      long now = NodeSyncHelpers.time().currentTimeMillis();
      TimeValue age = TimeValue.of(now - this.startedAt, TimeUnit.MILLISECONDS);
      return String.format("%s=%s ago%s", new Object[]{this.outcome, age, this.missingNodes == null?"":String.format(" (missing: %s)", new Object[]{this.missingNodes})});
   }
}
