package org.apache.cassandra.db.lifecycle;

import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.ImmutableMap.Builder;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.utils.Throwables;

class Helpers {
   Helpers() {
   }

   static <T> Set<T> replace(Set<T> original, Set<T> remove, Iterable<T> add) {
      return ImmutableSet.copyOf(replace(identityMap(original), remove, add).keySet());
   }

   static <T> Map<T, T> replace(Map<T, T> original, Set<T> remove, Iterable<T> add) {
      for (T reader : remove) {
         assert (original.get(reader) == reader);
      }
      assert (!Iterables.any(add, (Predicate)Predicates.and((Predicate)Predicates.not((Predicate)Predicates.in(remove)), (Predicate)Predicates.in(original.keySet()))));
      Map<T, T> result = Helpers.identityMap(Iterables.concat(add, (Iterable)Iterables.filter(original.keySet(), (Predicate)Predicates.not((Predicate)Predicates.in(remove)))));
      assert (result.size() == original.size() - remove.size() + Iterables.size(add));
      return result;
   }

   static void setupOnline(Iterable<SSTableReader> readers) {
      Iterator var1 = readers.iterator();

      while(var1.hasNext()) {
         SSTableReader reader = (SSTableReader)var1.next();
         reader.setupOnline();
      }

   }

   static Throwable setReplaced(Iterable<SSTableReader> readers, Throwable accumulate) {
      Iterator var2 = readers.iterator();

      while(var2.hasNext()) {
         SSTableReader reader = (SSTableReader)var2.next();

         try {
            reader.setReplaced();
         } catch (Throwable var5) {
            accumulate = Throwables.merge(accumulate, var5);
         }
      }

      return accumulate;
   }

    static void checkNotReplaced(Iterable<SSTableReader> readers) {
        for (SSTableReader reader : readers) {
            assert (!reader.isReplaced());
        }
    }

   static Throwable markObsolete(List<LogTransaction.Obsoletion> obsoletions, Throwable accumulate) {
      if(obsoletions != null && !obsoletions.isEmpty()) {
         Iterator var2 = obsoletions.iterator();

         while(var2.hasNext()) {
            LogTransaction.Obsoletion obsoletion = (LogTransaction.Obsoletion)var2.next();

            try {
               obsoletion.reader.markObsolete(obsoletion.tidier);
            } catch (Throwable var5) {
               accumulate = Throwables.merge(accumulate, var5);
            }
         }

         return accumulate;
      } else {
         return accumulate;
      }
   }

   static Throwable prepareForObsoletion(Iterable<SSTableReader> readers, LogTransaction txnLogs, List<LogTransaction.Obsoletion> obsoletions, Throwable accumulate) {
      Map<SSTable, LogRecord> logRecords = txnLogs.makeRemoveRecords(readers);
      Iterator var5 = readers.iterator();

      while(var5.hasNext()) {
         SSTableReader reader = (SSTableReader)var5.next();

         try {
            obsoletions.add(new LogTransaction.Obsoletion(reader, txnLogs.obsoleted(reader, (LogRecord)logRecords.get(reader))));
         } catch (Throwable var8) {
            accumulate = Throwables.merge(accumulate, var8);
         }
      }

      return accumulate;
   }

   static Throwable abortObsoletion(List<LogTransaction.Obsoletion> obsoletions, Throwable accumulate) {
      if(obsoletions != null && !obsoletions.isEmpty()) {
         Iterator var2 = obsoletions.iterator();

         while(var2.hasNext()) {
            LogTransaction.Obsoletion obsoletion = (LogTransaction.Obsoletion)var2.next();

            try {
               obsoletion.tidier.abort();
            } catch (Throwable var5) {
               accumulate = Throwables.merge(accumulate, var5);
            }
         }

         return accumulate;
      } else {
         return accumulate;
      }
   }

   static <T> Map<T, T> identityMap(Iterable<T> values) {
      ImmutableMap.Builder builder = ImmutableMap.builder();
      for (T t : values) {
         builder.put(t, t);
      }
      return builder.build();
   }

   static <T> Iterable<T> concatUniq(Set... sets) {
      List<Predicate<T>> notIn = new ArrayList(sets.length);
      Set[] var2 = sets;
      int i = sets.length;

      for(int var4 = 0; var4 < i; ++var4) {
         Set<T> set = var2[var4];
         notIn.add(Predicates.not(Predicates.in(set)));
      }

      List<Iterable<T>> results = new ArrayList(sets.length);

      for(i = 0; i < sets.length; ++i) {
         results.add(Iterables.filter(sets[i], Predicates.and(notIn.subList(0, i))));
      }

      return Iterables.concat(results);
   }

   static <T> Predicate<T> notIn(Set... sets) {
      return Predicates.not(orIn(sets));
   }

   static <T> Predicate<T> orIn(Collection... sets) {
      Predicate<T>[] orIn = new Predicate[sets.length];

      for(int i = 0; i < orIn.length; ++i) {
         orIn[i] = Predicates.in(sets[i]);
      }

      return Predicates.or(orIn);
   }

   static <T> Iterable<T> filterOut(Iterable<T> filter, Set... inNone) {
      return Iterables.filter(filter, notIn(inNone));
   }

   static <T> Iterable<T> filterIn(Iterable<T> filter, Set... inAny) {
      return Iterables.filter(filter, orIn(inAny));
   }

   static Set<SSTableReader> emptySet() {
      return Collections.emptySet();
   }

   static <T> T select(T t, Collection<T> col) {
      if (col instanceof Set && !col.contains(t)) {
         return null;
      }
      return (T)Iterables.getFirst((Iterable)Iterables.filter(col, (Predicate)Predicates.equalTo(t)), null);
   }

   static <T> T selectFirst(T t, Collection... sets) {
      Collection[] var2 = sets;
      int var3 = sets.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         Collection<T> set = var2[var4];
         T select = select(t, set);
         if(select != null) {
            return select;
         }
      }

      return null;
   }

   static <T> Predicate<T> idIn(Set<T> set) {
      return idIn(identityMap(set));
   }

   static <T> Predicate<T> idIn(final Map<T, T> identityMap) {
      return new Predicate<T>() {
         public boolean apply(T t) {
            return identityMap.get(t) == t;
         }
      };
   }
}
