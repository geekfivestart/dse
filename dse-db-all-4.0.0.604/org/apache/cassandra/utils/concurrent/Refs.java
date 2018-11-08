package org.apache.cassandra.utils.concurrent;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
import java.util.AbstractCollection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nullable;
import org.apache.cassandra.utils.Throwables;

public final class Refs<T extends RefCounted<T>> extends AbstractCollection<T> implements AutoCloseable {
   private final Map<T, Ref<T>> references;

   public Refs() {
      this.references = new HashMap();
   }

   public Refs(Map<T, Ref<T>> references) {
      this.references = new HashMap(references);
   }

   public void release() {
      try {
         release((Iterable)this.references.values());
      } finally {
         this.references.clear();
      }

   }

   public void close() {
      this.release();
   }

   public Ref<T> get(T referenced) {
      return (Ref)this.references.get(referenced);
   }

   public void release(T referenced) {
      Ref ref = (Ref)this.references.remove(referenced);
      if(ref == null) {
         throw new IllegalStateException("This Refs collection does not hold a reference to " + referenced);
      } else {
         ref.release();
      }
   }

   public boolean releaseIfHolds(T referenced) {
      Ref ref = (Ref)this.references.remove(referenced);
      if(ref != null) {
         ref.release();
      }

      return ref != null;
   }

   public void relaseAllExcept(Collection<T> keep) {
      Collection<T> release = new ArrayList(this.references.keySet());
      release.retainAll(keep);
      this.release((Collection)release);
   }

   public void release(Collection<T> release) {
      List<Ref<T>> refs = new ArrayList();
      List<T> notPresent = null;
      Iterator var4 = release.iterator();

      while(var4.hasNext()) {
         T obj = (RefCounted)var4.next();
         Ref<T> ref = (Ref)this.references.remove(obj);
         if(ref == null) {
            if(notPresent == null) {
               notPresent = new ArrayList();
            }

            notPresent.add(obj);
         } else {
            refs.add(ref);
         }
      }

      IllegalStateException notPresentFail = null;
      if(notPresent != null) {
         notPresentFail = new IllegalStateException("Could not release references to " + notPresent + " as references to these objects were not held");
         notPresentFail.fillInStackTrace();
      }

      try {
         release((Iterable)refs);
      } catch (Throwable var7) {
         if(notPresentFail != null) {
            var7.addSuppressed(notPresentFail);
         }
      }

      if(notPresentFail != null) {
         throw notPresentFail;
      }
   }

   public boolean tryRef(T t) {
      Ref<T> ref = t.tryRef();
      if(ref == null) {
         return false;
      } else {
         ref = (Ref)this.references.put(t, ref);
         if(ref != null) {
            ref.release();
         }

         return true;
      }
   }

   public Iterator<T> iterator() {
      return Iterators.unmodifiableIterator(this.references.keySet().iterator());
   }

   public int size() {
      return this.references.size();
   }

   public Refs<T> addAll(Refs<T> add) {
      List<Ref<T>> overlap = new ArrayList();
      Iterator var3 = add.references.entrySet().iterator();

      while(var3.hasNext()) {
         Entry<T, Ref<T>> e = (Entry)var3.next();
         if(this.references.containsKey(e.getKey())) {
            overlap.add(e.getValue());
         } else {
            this.references.put(e.getKey(), e.getValue());
         }
      }

      add.references.clear();
      release((Iterable)overlap);
      return this;
   }

   public static <T extends RefCounted<T>> Refs<T> tryRef(Iterable<T> reference) {
      HashMap<T, Ref<T>> refs = new HashMap();
      Iterator var2 = reference.iterator();

      while(var2.hasNext()) {
         T rc = (RefCounted)var2.next();
         Ref<T> ref = rc.tryRef();
         if(ref == null) {
            release((Iterable)refs.values());
            return null;
         }

         refs.put(rc, ref);
      }

      return new Refs(refs);
   }

   public static <T extends RefCounted<T>> Refs<T> ref(Iterable<T> reference) {
      Refs<T> refs = tryRef(reference);
      if(refs != null) {
         return refs;
      } else {
         throw new IllegalStateException();
      }
   }

   public static void release(Iterable<? extends Ref<?>> refs) {
      Throwables.maybeFail(release(refs, (Throwable)null));
   }

   public static Throwable release(Iterable<? extends Ref<?>> refs, Throwable accumulate) {
      Iterator var2 = refs.iterator();

      while(var2.hasNext()) {
         Ref ref = (Ref)var2.next();

         try {
            ref.release();
         } catch (Throwable var5) {
            accumulate = Throwables.merge(accumulate, var5);
         }
      }

      return accumulate;
   }

   public static <T extends SelfRefCounted<T>> Iterable<Ref<T>> selfRefs(Iterable<T> refs) {
      return Iterables.transform(refs, new Function<T, Ref<T>>() {
         @Nullable
         public Ref<T> apply(T t) {
            return t.selfRef();
         }
      });
   }
}
