package org.apache.cassandra.repair.consistent;

import com.google.common.base.Function;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import java.net.InetAddress;
import java.util.Collection;
import java.util.EnumMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.schema.TableId;

public abstract class ConsistentSession {
   private volatile ConsistentSession.State state;
   public final UUID sessionID;
   public final InetAddress coordinator;
   public final ImmutableSet<TableId> tableIds;
   public final long repairedAt;
   public final ImmutableSet<Range<Token>> ranges;
   public final ImmutableSet<InetAddress> participants;

   ConsistentSession(ConsistentSession.AbstractBuilder builder) {
      builder.validate();
      this.state = builder.state;
      this.sessionID = builder.sessionID;
      this.coordinator = builder.coordinator;
      this.tableIds = ImmutableSet.copyOf(builder.ids);
      this.repairedAt = builder.repairedAt;
      this.ranges = ImmutableSet.copyOf(builder.ranges);
      this.participants = ImmutableSet.copyOf(builder.participants);
   }

   public ConsistentSession.State getState() {
      return this.state;
   }

   public void setState(ConsistentSession.State state) {
      this.state = state;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         ConsistentSession that = (ConsistentSession)o;
         return this.repairedAt != that.repairedAt?false:(this.state != that.state?false:(!this.sessionID.equals(that.sessionID)?false:(!this.coordinator.equals(that.coordinator)?false:(!this.tableIds.equals(that.tableIds)?false:(!this.ranges.equals(that.ranges)?false:this.participants.equals(that.participants))))));
      } else {
         return false;
      }
   }

   public int hashCode() {
      int result = this.state.hashCode();
      result = 31 * result + this.sessionID.hashCode();
      result = 31 * result + this.coordinator.hashCode();
      result = 31 * result + this.tableIds.hashCode();
      result = 31 * result + (int)(this.repairedAt ^ this.repairedAt >>> 32);
      result = 31 * result + this.ranges.hashCode();
      result = 31 * result + this.participants.hashCode();
      return result;
   }

   public String toString() {
      return "ConsistentSession{state=" + this.state + ", sessionID=" + this.sessionID + ", coordinator=" + this.coordinator + ", tableIds=" + this.tableIds + ", repairedAt=" + this.repairedAt + ", ranges=" + this.ranges + ", participants=" + this.participants + '}';
   }

   abstract static class AbstractBuilder<T extends ConsistentSession.AbstractBuilder<T>> {
      private ConsistentSession.State state;
      private UUID sessionID;
      private InetAddress coordinator;
      private Set<TableId> ids;
      private long repairedAt;
      private Collection<Range<Token>> ranges;
      private Set<InetAddress> participants;

      AbstractBuilder() {
      }

      T withState(ConsistentSession.State state) {
         this.state = state;
         return this;
      }

      T withSessionID(UUID sessionID) {
         this.sessionID = sessionID;
         return this;
      }

      T withCoordinator(InetAddress coordinator) {
         this.coordinator = coordinator;
         return this;
      }

      T withUUIDTableIds(Iterable<UUID> ids) {
         this.ids = ImmutableSet.copyOf(Iterables.transform(ids, TableId::fromUUID));
         return this;
      }

      T withTableIds(Set<TableId> ids) {
         this.ids = ids;
         return this;
      }

      T withRepairedAt(long repairedAt) {
         this.repairedAt = repairedAt;
         return this;
      }

      T withRanges(Collection<Range<Token>> ranges) {
         this.ranges = ranges;
         return this;
      }

      T withParticipants(Set<InetAddress> peers) {
         this.participants = peers;
         return this;
      }

      void validate() {
         Preconditions.checkArgument(this.state != null);
         Preconditions.checkArgument(this.sessionID != null);
         Preconditions.checkArgument(this.coordinator != null);
         Preconditions.checkArgument(this.ids != null);
         Preconditions.checkArgument(!this.ids.isEmpty());
         Preconditions.checkArgument(this.repairedAt > 0L || this.repairedAt == 0L);
         Preconditions.checkArgument(this.ranges != null);
         Preconditions.checkArgument(!this.ranges.isEmpty());
         Preconditions.checkArgument(this.participants != null);
         Preconditions.checkArgument(!this.participants.isEmpty());
         Preconditions.checkArgument(this.participants.contains(this.coordinator));
      }

      public abstract ConsistentSession build();
   }

   public static enum State {
      PREPARING(0),
      PREPARED(1),
      REPAIRING(2),
      FINALIZED(3),
      FAILED(4),
      UNKNOWN(5);

      private static final Map<ConsistentSession.State, Set<ConsistentSession.State>> transitions = new EnumMap<ConsistentSession.State, Set<ConsistentSession.State>>(ConsistentSession.State.class) {
         {
            this.put(ConsistentSession.State.PREPARING, ImmutableSet.of(ConsistentSession.State.PREPARED, ConsistentSession.State.FAILED));
            this.put(ConsistentSession.State.PREPARED, ImmutableSet.of(ConsistentSession.State.REPAIRING, ConsistentSession.State.FAILED));
            this.put(ConsistentSession.State.REPAIRING, ImmutableSet.of(ConsistentSession.State.FINALIZED, ConsistentSession.State.FAILED));
            this.put(ConsistentSession.State.FINALIZED, ImmutableSet.of());
            this.put(ConsistentSession.State.FAILED, ImmutableSet.of());
         }
      };

      private State(int expectedOrdinal) {
         assert this.ordinal() == expectedOrdinal;

      }

      public boolean canTransitionTo(ConsistentSession.State state) {
         return state == this || ((Set)transitions.get(this)).contains(state);
      }

      public static ConsistentSession.State valueOf(int ordinal) {
         return values()[ordinal];
      }
   }
}
