package org.apache.cassandra.db.lifecycle;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import java.io.File;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiPredicate;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Directories;
import org.apache.cassandra.db.Memtable;
import org.apache.cassandra.db.compaction.OperationType;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Throwables;
import org.apache.cassandra.utils.concurrent.Refs;
import org.apache.cassandra.utils.concurrent.Transactional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LifecycleTransaction extends Transactional.AbstractTransactional {
   private static final Logger logger = LoggerFactory.getLogger(LifecycleTransaction.class);
   public final Tracker tracker;
   private final LogTransaction log;
   private final Set<SSTableReader> originals;
   private final Set<SSTableReader> marked;
   private final Set<SSTableReader.UniqueIdentifier> identities;
   private final LifecycleTransaction.State logged;
   private final LifecycleTransaction.State staged;
   private List<LogTransaction.Obsoletion> obsoletions;

   public static LifecycleTransaction offline(OperationType operationType, SSTableReader reader) {
      return offline(operationType, (Iterable)Collections.singleton(reader));
   }

   public static LifecycleTransaction offline(OperationType operationType, Iterable<SSTableReader> readers) {
      Tracker dummy = new Tracker((Memtable)null, false);
      dummy.addInitialSSTables(readers);
      dummy.apply(View.updateCompacting(Helpers.emptySet(), readers));
      return new LifecycleTransaction(dummy, operationType, readers);
   }

   public static LifecycleTransaction offline(OperationType operationType) {
      Tracker dummy = new Tracker((Memtable)null, false);
      return new LifecycleTransaction(dummy, new LogTransaction(operationType, dummy), Collections.emptyList());
   }

   LifecycleTransaction(Tracker tracker, OperationType operationType, Iterable<SSTableReader> readers) {
      this(tracker, new LogTransaction(operationType, tracker), readers);
   }

   LifecycleTransaction(Tracker tracker, LogTransaction log, Iterable<SSTableReader> readers) {
      this.originals = new HashSet();
      this.marked = Collections.newSetFromMap(new ConcurrentHashMap());
      this.identities = Collections.newSetFromMap(new IdentityHashMap());
      this.logged = new LifecycleTransaction.State();
      this.staged = new LifecycleTransaction.State();
      this.tracker = tracker;
      this.log = log;
      Iterator var4 = readers.iterator();

      while(var4.hasNext()) {
         SSTableReader reader = (SSTableReader)var4.next();
         this.originals.add(reader);
         this.marked.add(reader);
         this.identities.add(reader.instanceId);
      }

   }

   public LogTransaction log() {
      return this.log;
   }

   public OperationType opType() {
      return this.log.type();
   }

   public UUID opId() {
      return this.log.id();
   }

   public void doPrepare() {
      this.checkpoint();
      Throwables.maybeFail(Helpers.prepareForObsoletion(Helpers.filterIn(this.logged.obsolete, new Set[]{this.originals}), this.log, this.obsoletions = new ArrayList(), (Throwable)null));
      this.log.prepareToCommit();
   }

   public Throwable doCommit(Throwable accumulate) {
      assert this.staged.isEmpty() : "must be no actions introduced between prepareToCommit and a commit";

      if(logger.isTraceEnabled()) {
         logger.trace("Committing transaction over {} staged: {}, logged: {}", new Object[]{this.originals, this.staged, this.logged});
      }

      Throwables.maybeFail(accumulate);
      Throwables.maybeFail(this.log.commit((Throwable)null));
      accumulate = Helpers.markObsolete(this.obsoletions, accumulate);
      accumulate = this.tracker.updateSizeTracking(this.logged.obsolete, this.logged.update, accumulate);
      accumulate = Refs.release(Refs.selfRefs(this.logged.obsolete), accumulate);
      accumulate = this.tracker.notifySSTablesChanged(this.originals, this.logged.update, this.log.type(), accumulate);
      return accumulate;
   }

   public Throwable doAbort(Throwable accumulate) {
      if(logger.isTraceEnabled()) {
         logger.trace("Aborting transaction over {} staged: {}, logged: {}", new Object[]{this.originals, this.staged, this.logged});
      }

      accumulate = Helpers.abortObsoletion(this.obsoletions, accumulate);
      if(this.logged.isEmpty() && this.staged.isEmpty()) {
         return this.log.abort(accumulate);
      } else {
         Iterable<SSTableReader> obsolete = Helpers.filterOut(Helpers.concatUniq(new Set[]{this.staged.update, this.logged.update}), new Set[]{this.originals});
         logger.trace("Obsoleting {}", obsolete);
         accumulate = Helpers.prepareForObsoletion(obsolete, this.log, this.obsoletions = new ArrayList(), accumulate);
         accumulate = this.log.abort(accumulate);
         accumulate = Helpers.markObsolete(this.obsoletions, accumulate);
         List<SSTableReader> restored = this.restoreUpdatedOriginals();
         List<SSTableReader> invalid = Lists.newArrayList(Iterables.concat(this.logged.update, this.logged.obsolete));
         accumulate = this.tracker.apply(View.updateLiveSet(this.logged.update, restored), accumulate);
         accumulate = this.tracker.notifySSTablesChanged(invalid, restored, OperationType.COMPACTION, accumulate);
         accumulate = Helpers.setReplaced(this.logged.update, accumulate);
         accumulate = Refs.release(Refs.selfRefs(Iterables.concat(this.staged.update, this.logged.update, this.logged.obsolete)), accumulate);
         this.logged.clear();
         this.staged.clear();
         return accumulate;
      }
   }

   protected Throwable doPostCleanup(Throwable accumulate) {
      this.log.close();
      Throwable throwable = this.unmarkCompacting(this.marked, accumulate);
      this.tracker.finishTransaction(this.opId());
      return throwable;
   }

   public boolean isOffline() {
      return this.tracker.isDummy();
   }

   public void permitRedundantTransitions() {
      super.permitRedundantTransitions();
   }

   public void checkpoint() {
      Throwables.maybeFail(this.checkpoint((Throwable)null));
   }

   private Throwable checkpoint(Throwable accumulate) {
      if(logger.isTraceEnabled()) {
         logger.trace("Checkpointing staged {}", this.staged);
      }

      if(this.staged.isEmpty()) {
         return accumulate;
      } else {
         Set<SSTableReader> toUpdate = this.toUpdate();
         Set<SSTableReader> fresh = ImmutableSet.copyOf(this.fresh());
         Helpers.checkNotReplaced(Helpers.filterIn(toUpdate, new Set[]{this.staged.update}));
         this.tracker.apply(Functions.compose(View.updateCompacting(Helpers.emptySet(), fresh), View.updateLiveSet(toUpdate, this.staged.update)));
         this.marked.addAll(fresh);
         this.logged.log(this.staged);
         accumulate = Helpers.setReplaced(Helpers.filterOut(toUpdate, new Set[]{this.staged.obsolete}), accumulate);
         accumulate = Refs.release(Refs.selfRefs(Helpers.filterOut(toUpdate, new Set[]{this.staged.obsolete})), accumulate);
         this.staged.clear();
         return accumulate;
      }
   }

   public void update(SSTableReader reader, boolean original) {
      assert !this.staged.update.contains(reader) : "each reader may only be updated once per checkpoint: " + reader;

      assert !this.identities.contains(reader.instanceId) : "each reader instance may only be provided as an update once: " + reader;

      assert !this.logged.obsolete.contains(reader) && !this.staged.obsolete.contains(reader) : "may not update a reader that has been obsoleted";

      assert original == this.originals.contains(reader) : String.format("the 'original' indicator was incorrect (%s provided): %s", new Object[]{Boolean.valueOf(original), reader});

      this.staged.update.add(reader);
      this.identities.add(reader.instanceId);
      if(!this.isOffline()) {
         reader.setupOnline();
      }

   }

   public void update(Collection<SSTableReader> readers, boolean original) {
      Iterator var3 = readers.iterator();

      while(var3.hasNext()) {
         SSTableReader reader = (SSTableReader)var3.next();
         this.update(reader, original);
      }

   }

   public void obsolete(SSTableReader reader) {
      logger.trace("Staging for obsolescence {}", reader);

      assert this.identities.contains(reader.instanceId) : "only reader instances that have previously been provided may be obsoleted: " + reader;

      assert this.originals.contains(reader) : "only readers in the 'original' set may be obsoleted: " + reader + " vs " + this.originals;

      assert !this.logged.obsolete.contains(reader) && !this.staged.obsolete.contains(reader) : "may not obsolete a reader that has already been obsoleted: " + reader;

      assert !this.staged.update.contains(reader) : "may not obsolete a reader that has a staged update (must checkpoint first): " + reader;

      assert this.current(reader) == reader : "may only obsolete the latest version of the reader: " + reader;

      this.staged.obsolete.add(reader);
   }

   public void obsoleteOriginals() {
      logger.trace("Staging for obsolescence {}", this.originals);

      assert Iterables.isEmpty(Helpers.filterIn(this.staged.update, new Set[]{this.originals})) : this.staged.update;

      Iterables.addAll(this.staged.obsolete, Helpers.filterIn(this.current(), new Set[]{this.originals}));
   }

   private Set<SSTableReader> toUpdate() {
      return ImmutableSet.copyOf(Helpers.filterIn(this.current(), new Set[]{this.staged.obsolete, this.staged.update}));
   }

   private Iterable<SSTableReader> fresh() {
      return Helpers.filterOut(this.staged.update, new Set[]{this.originals, this.logged.update});
   }

   public Iterable<SSTableReader> current() {
      return Iterables.concat(this.logged.update, Helpers.filterOut(this.originals, new Set[]{this.logged.update, this.logged.obsolete}));
   }

   private List<SSTableReader> restoreUpdatedOriginals() {
      Iterable<SSTableReader> torestore = Helpers.filterIn(this.originals, new Set[]{this.logged.update, this.logged.obsolete});
      return ImmutableList.copyOf(Iterables.transform(torestore, (reader) -> {
         return this.current(reader).cloneWithRestoredStart(reader.first);
      }));
   }

   public Set<SSTableReader> originals() {
      return Collections.unmodifiableSet(this.originals);
   }

   public Set<SSTableReader> compactingSet() {
      return Collections.unmodifiableSet(this.marked);
   }

   public boolean isObsolete(SSTableReader reader) {
      return this.logged.obsolete.contains(reader) || this.staged.obsolete.contains(reader);
   }

   public SSTableReader current(SSTableReader reader) {
      Set container;
      if(this.staged.contains(reader)) {
         container = this.staged.update.contains(reader)?this.staged.update:this.staged.obsolete;
      } else if(this.logged.contains(reader)) {
         container = this.logged.update.contains(reader)?this.logged.update:this.logged.obsolete;
      } else {
         if(!this.originals.contains(reader)) {
            throw new AssertionError();
         }

         container = this.originals;
      }

      return (SSTableReader)Helpers.select(reader, container);
   }

   public void cancel(SSTableReader cancel) {
      logger.trace("Cancelling {} from transaction", cancel);

      assert this.originals.contains(cancel) : "may only cancel a reader in the 'original' set: " + cancel + " vs " + this.originals;

      assert !this.staged.contains(cancel) && !this.logged.contains(cancel) : "may only cancel a reader that has not been updated or obsoleted in this transaction: " + cancel;

      this.originals.remove(cancel);
      this.marked.remove(cancel);
      this.identities.remove(cancel.instanceId);
      Throwables.maybeFail(this.unmarkCompacting(Collections.singleton(cancel), (Throwable)null));
   }

   public void cancel(Iterable<SSTableReader> cancels) {
      Iterator var2 = cancels.iterator();

      while(var2.hasNext()) {
         SSTableReader cancel = (SSTableReader)var2.next();
         this.cancel(cancel);
      }

   }

   public LifecycleTransaction split(Collection<SSTableReader> readers) {
      logger.trace("Splitting {} into new transaction", readers);
      this.checkUnused();
      Iterator var2 = readers.iterator();

      SSTableReader reader;
      while(var2.hasNext()) {
         reader = (SSTableReader)var2.next();

         assert this.identities.contains(reader.instanceId) : "may only split the same reader instance the transaction was opened with: " + reader;
      }

      var2 = readers.iterator();

      while(var2.hasNext()) {
         reader = (SSTableReader)var2.next();
         this.identities.remove(reader.instanceId);
         this.originals.remove(reader);
         this.marked.remove(reader);
      }

      return new LifecycleTransaction(this.tracker, this.log.type(), readers);
   }

   private void checkUnused() {
      assert this.logged.isEmpty();

      assert this.staged.isEmpty();

      assert this.identities.size() == this.originals.size();

      assert this.originals.size() == this.marked.size();

   }

   private Throwable unmarkCompacting(Set<SSTableReader> unmark, Throwable accumulate) {
      accumulate = this.tracker.apply(View.updateCompacting(unmark, Helpers.emptySet()), accumulate);
      accumulate = this.tracker.dropSSTablesIfInvalid(accumulate);
      return accumulate;
   }

   public SSTableReader onlyOne() {
      assert this.originals.size() == 1;

      return (SSTableReader)Iterables.getFirst(this.originals, (Object)null);
   }

   public void trackNew(SSTable table) {
      this.log.trackNew(table);
   }

   public void untrackNew(SSTable table) {
      this.log.untrackNew(table);
   }

   public static boolean removeUnfinishedLeftovers(ColumnFamilyStore cfs) {
      return LogTransaction.removeUnfinishedLeftovers(cfs.getDirectories().getCFDirectories());
   }

   public static boolean removeUnfinishedLeftovers(TableMetadata metadata) {
      return LogTransaction.removeUnfinishedLeftovers(metadata);
   }

   public static List<File> getFiles(Path folder, BiPredicate<File, Directories.FileType> filter, Directories.OnTxnErr onTxnErr) {
      return (new LogAwareFileLister(folder, filter, onTxnErr)).list();
   }

   public static void rescheduleFailedDeletions() {
      LogTransaction.rescheduleFailedDeletions();
   }

   public static void waitForDeletions() {
      LogTransaction.waitForDeletions();
   }

   @VisibleForTesting
   public LifecycleTransaction.ReaderState state(SSTableReader reader) {
      SSTableReader currentlyVisible = LifecycleTransaction.ReaderState.visible(reader, Predicates.in(this.logged.obsolete), new Collection[]{this.logged.update, this.originals});
      SSTableReader nextVisible = LifecycleTransaction.ReaderState.visible(reader, Helpers.orIn(new Collection[]{this.staged.obsolete, this.logged.obsolete}), new Collection[]{this.staged.update, this.logged.update, this.originals});
      return new LifecycleTransaction.ReaderState(LifecycleTransaction.ReaderState.Action.get(this.logged.update.contains(reader), this.logged.obsolete.contains(reader)), LifecycleTransaction.ReaderState.Action.get(this.staged.update.contains(reader), this.staged.obsolete.contains(reader)), currentlyVisible, nextVisible, this.originals.contains(reader));
   }

   public String toString() {
      return this.originals.toString();
   }

   @VisibleForTesting
   public static class ReaderState {
      final LifecycleTransaction.ReaderState.Action staged;
      final LifecycleTransaction.ReaderState.Action logged;
      final SSTableReader nextVisible;
      final SSTableReader currentlyVisible;
      final boolean original;

      public ReaderState(LifecycleTransaction.ReaderState.Action logged, LifecycleTransaction.ReaderState.Action staged, SSTableReader currentlyVisible, SSTableReader nextVisible, boolean original) {
         this.staged = staged;
         this.logged = logged;
         this.currentlyVisible = currentlyVisible;
         this.nextVisible = nextVisible;
         this.original = original;
      }

      public boolean equals(Object that) {
         return that instanceof LifecycleTransaction.ReaderState && this.equals((LifecycleTransaction.ReaderState)that);
      }

      public boolean equals(LifecycleTransaction.ReaderState that) {
         return this.staged == that.staged && this.logged == that.logged && this.original == that.original && this.currentlyVisible == that.currentlyVisible && this.nextVisible == that.nextVisible;
      }

      public String toString() {
         return String.format("[logged=%s staged=%s original=%s]", new Object[]{this.logged, this.staged, Boolean.valueOf(this.original)});
      }

      public static SSTableReader visible(SSTableReader reader, Predicate<SSTableReader> obsolete, Collection... selectFrom) {
         return obsolete.apply(reader)?null:(SSTableReader)Helpers.selectFirst(reader, selectFrom);
      }

      public static enum Action {
         UPDATED,
         OBSOLETED,
         NONE;

         private Action() {
         }

         public static LifecycleTransaction.ReaderState.Action get(boolean updated, boolean obsoleted) {
            assert !updated || !obsoleted;

            return updated?UPDATED:(obsoleted?OBSOLETED:NONE);
         }
      }
   }

   private static class State {
      final Set<SSTableReader> update;
      final Set<SSTableReader> obsolete;

      private State() {
         this.update = new HashSet();
         this.obsolete = new HashSet();
      }

      void log(LifecycleTransaction.State staged) {
         this.update.removeAll(staged.obsolete);
         this.update.removeAll(staged.update);
         this.update.addAll(staged.update);
         this.obsolete.addAll(staged.obsolete);
      }

      boolean contains(SSTableReader reader) {
         return this.update.contains(reader) || this.obsolete.contains(reader);
      }

      boolean isEmpty() {
         return this.update.isEmpty() && this.obsolete.isEmpty();
      }

      void clear() {
         this.update.clear();
         this.obsolete.clear();
      }

      public String toString() {
         return String.format("[obsolete: %s, update: %s]", new Object[]{this.obsolete, this.update});
      }
   }
}
