package org.apache.cassandra.index.sasi.conf;

import java.io.File;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.SSTableIndex;
import org.apache.cassandra.index.sasi.conf.view.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataTracker {
   private static final Logger logger = LoggerFactory.getLogger(DataTracker.class);
   private final AbstractType<?> keyValidator;
   private final ColumnIndex columnIndex;
   private final AtomicReference<View> view = new AtomicReference();

   public DataTracker(AbstractType<?> keyValidator, ColumnIndex index) {
      this.keyValidator = keyValidator;
      this.columnIndex = index;
      this.view.set(new View(index, Collections.emptySet()));
   }

   public View getView() {
      return (View)this.view.get();
   }

   public Iterable<SSTableReader> update(Collection<SSTableReader> oldSSTables, Collection<SSTableReader> newSSTables) {
      Pair<Set<SSTableIndex>, Set<SSTableReader>> built = this.getBuiltIndexes(newSSTables);
      Set<SSTableIndex> newIndexes = (Set)built.left;
      Set indexedSSTables = (Set)built.right;

      View currentView;
      View newView;
      do {
         currentView = (View)this.view.get();
         newView = new View(this.columnIndex, currentView.getIndexes(), oldSSTables, newIndexes);
      } while(!this.view.compareAndSet(currentView, newView));

      return (Iterable)newSSTables.stream().filter((sstable) -> {
         return !indexedSSTables.contains(sstable);
      }).collect(Collectors.toList());
   }

   public boolean hasSSTable(SSTableReader sstable) {
      View currentView = (View)this.view.get();
      Iterator var3 = currentView.iterator();

      SSTableIndex index;
      do {
         if(!var3.hasNext()) {
            return false;
         }

         index = (SSTableIndex)var3.next();
      } while(!index.getSSTable().equals(sstable));

      return true;
   }

   public void dropData(Collection<SSTableReader> sstablesToRebuild) {
      View currentView = (View)this.view.get();
      if(currentView != null) {
         Set<SSTableReader> toRemove = new HashSet(sstablesToRebuild);
         Iterator var4 = currentView.iterator();

         while(var4.hasNext()) {
            SSTableIndex index = (SSTableIndex)var4.next();
            SSTableReader sstable = index.getSSTable();
            if(sstablesToRebuild.contains(sstable)) {
               index.markObsolete();
            }
         }

         this.update(toRemove, Collections.emptyList());
      }
   }

   public void dropData(long truncateUntil) {
      View currentView = (View)this.view.get();
      if(currentView != null) {
         Set<SSTableReader> toRemove = new HashSet();
         Iterator var5 = currentView.iterator();

         while(var5.hasNext()) {
            SSTableIndex index = (SSTableIndex)var5.next();
            SSTableReader sstable = index.getSSTable();
            if(sstable.getMaxTimestamp() <= truncateUntil) {
               index.markObsolete();
               toRemove.add(sstable);
            }
         }

         this.update(toRemove, Collections.emptyList());
      }
   }

   private Pair<Set<SSTableIndex>, Set<SSTableReader>> getBuiltIndexes(Collection<SSTableReader> sstables) {
      Set<SSTableIndex> indexes = new HashSet(sstables.size());
      Set<SSTableReader> builtSSTables = new HashSet(sstables.size());
      Iterator var4 = sstables.iterator();

      while(true) {
         while(true) {
            SSTableReader sstable;
            File indexFile;
            do {
               do {
                  if(!var4.hasNext()) {
                     return Pair.create(indexes, builtSSTables);
                  }

                  sstable = (SSTableReader)var4.next();
               } while(sstable.isMarkedCompacted());

               indexFile = new File(sstable.descriptor.filenameFor(this.columnIndex.getComponent()));
            } while(!indexFile.exists());

            if(indexFile.length() == 0L) {
               builtSSTables.add(sstable);
            } else {
               SSTableIndex index = null;

               try {
                  index = new SSTableIndex(this.columnIndex, indexFile, sstable);
                  logger.info("SSTableIndex.open(column: {}, minTerm: {}, maxTerm: {}, minKey: {}, maxKey: {}, sstable: {})", new Object[]{this.columnIndex.getColumnName(), this.columnIndex.getValidator().getString(index.minTerm()), this.columnIndex.getValidator().getString(index.maxTerm()), this.keyValidator.getString(index.minKey()), this.keyValidator.getString(index.maxKey()), index.getSSTable()});
                  if(indexes.add(index)) {
                     builtSSTables.add(sstable);
                  } else {
                     index.release();
                  }
               } catch (Throwable var9) {
                  logger.error("Can't open index file at " + indexFile.getAbsolutePath() + ", skipping.", var9);
                  if(index != null) {
                     index.release();
                  }
               }
            }
         }
      }
   }
}
