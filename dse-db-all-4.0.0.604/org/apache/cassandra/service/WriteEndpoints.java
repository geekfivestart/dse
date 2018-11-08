package org.apache.cassandra.service;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.Mutation;
import org.apache.cassandra.db.view.ViewUtils;
import org.apache.cassandra.dht.RingPosition;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.gms.FailureDetector;
import org.apache.cassandra.service.paxos.Commit;
import org.apache.cassandra.utils.FBUtilities;

public class WriteEndpoints implements Iterable<InetAddress> {
   private final Keyspace keyspace;
   private final List<InetAddress> natural;
   private final Collection<InetAddress> pending;
   private final List<InetAddress> live;
   private final List<InetAddress> dead;

   private WriteEndpoints(Keyspace keyspace, List<InetAddress> natural, Collection<InetAddress> pending, List<InetAddress> live, List<InetAddress> dead) {
      this.keyspace = keyspace;
      this.natural = natural;
      this.pending = pending;
      this.live = live;
      this.dead = dead;
   }

   private WriteEndpoints(Keyspace keyspace, List<InetAddress> natural, Collection<InetAddress> pending) {
      this.keyspace = keyspace;
      this.natural = natural;
      this.pending = pending;
      List<InetAddress> tmpLive = new ArrayList(natural.size() + pending.size());
      List<InetAddress> tmpDead = null;
      Iterator var6 = Iterables.concat(natural, pending).iterator();

      while(var6.hasNext()) {
         InetAddress endpoint = (InetAddress)var6.next();
         if(FailureDetector.instance.isAlive(endpoint)) {
            tmpLive.add(endpoint);
         } else {
            if(tmpDead == null) {
               tmpDead = new ArrayList(2);
            }

            tmpDead.add(endpoint);
         }
      }

      this.live = Collections.unmodifiableList(tmpLive);
      this.dead = tmpDead == null?Collections.emptyList():Collections.unmodifiableList(tmpDead);
   }

   public static WriteEndpoints compute(String keyspace, DecoratedKey partitionKey) {
      Token tk = partitionKey.getToken();
      List<InetAddress> natural = StorageService.instance.getNaturalEndpoints((String)keyspace, (RingPosition)tk);
      Collection<InetAddress> pending = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspace);
      return new WriteEndpoints(Keyspace.open(keyspace), natural, pending);
   }

   public static WriteEndpoints compute(IMutation mutation) {
      return compute(mutation.getKeyspaceName(), mutation.key());
   }

   public static WriteEndpoints compute(Commit commit) {
      return compute(commit.update.metadata().keyspace, commit.update.partitionKey());
   }

   public static WriteEndpoints withLive(Keyspace keyspace, Iterable<InetAddress> live) {
      List<InetAddress> copy = ImmutableList.copyOf(live);
      return new WriteEndpoints(keyspace, copy, Collections.emptyList(), copy, Collections.emptyList());
   }

   public static WriteEndpoints computeForView(Token baseToken, Mutation mutation) {
      String keyspace = mutation.getKeyspaceName();
      Token tk = mutation.key().getToken();
      Optional<InetAddress> pairedEndpoint = ViewUtils.getViewNaturalEndpoint(keyspace, baseToken, tk);
      List<InetAddress> natural = pairedEndpoint.isPresent()?Collections.singletonList(pairedEndpoint.get()):Collections.emptyList();
      Collection<InetAddress> pending = StorageService.instance.getTokenMetadata().pendingEndpointsFor(tk, keyspace);
      return new WriteEndpoints(Keyspace.open(keyspace), natural, pending);
   }

   public Keyspace keyspace() {
      return this.keyspace;
   }

   public WriteEndpoints restrictToLocalDC() {
      Predicate<InetAddress> isLocalDc = (host) -> {
         return DatabaseDescriptor.getEndpointSnitch().isInLocalDatacenter(host);
      };
      return new WriteEndpoints(this.keyspace, ImmutableList.copyOf(Iterables.filter(this.natural, isLocalDc)), (Collection)(this.pending.isEmpty()?Collections.emptyList():ImmutableList.copyOf(Iterables.filter(this.pending, isLocalDc))));
   }

   public WriteEndpoints withoutLocalhost(boolean requireLocalhost) {
      InetAddress localhost = FBUtilities.getBroadcastAddress();
      if(requireLocalhost && this.natural.size() == 1 && this.pending.isEmpty()) {
         assert ((InetAddress)this.natural.get(0)).equals(localhost);

         return new WriteEndpoints(this.keyspace, Collections.emptyList(), Collections.emptyList(), Collections.emptyList(), Collections.emptyList());
      } else {
         Predicate<InetAddress> notLocalhost = (host) -> {
            return !host.equals(localhost);
         };
         List<InetAddress> newNatural = new ArrayList(this.natural.size() - 1);
         Iterables.addAll(newNatural, Iterables.filter(this.natural, notLocalhost));
         List<InetAddress> newLive = new ArrayList(this.live.size() - 1);
         Iterables.addAll(newLive, Iterables.filter(this.live, notLocalhost));
         return new WriteEndpoints(this.keyspace, newNatural, this.pending, newLive, this.dead);
      }
   }

   public void checkAvailability(ConsistencyLevel consistency) throws UnavailableException {
      consistency.assureSufficientLiveNodes(this.keyspace, this.live);
   }

   public boolean isEmpty() {
      return this.count() == 0;
   }

   public int count() {
      return this.natural.size() + this.pending.size();
   }

   public int naturalCount() {
      return this.natural.size();
   }

   public int pendingCount() {
      return this.pending.size();
   }

   public int liveCount() {
      return this.live.size();
   }

   public Iterator<InetAddress> iterator() {
      return Iterators.concat(this.natural.iterator(), this.pending.iterator());
   }

   public List<InetAddress> natural() {
      return this.natural;
   }

   public Collection<InetAddress> pending() {
      return this.pending;
   }

   public List<InetAddress> live() {
      return this.live;
   }

   public List<InetAddress> dead() {
      return this.dead;
   }

   public String toString() {
      return this.live.toString();
   }
}
