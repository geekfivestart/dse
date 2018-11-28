package org.apache.cassandra.utils.concurrent;

import com.google.common.annotations.VisibleForTesting;
import io.netty.channel.epoll.EpollEventLoop;
import java.lang.ref.PhantomReference;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.lang.ref.WeakReference;
import java.lang.reflect.Field;
import java.lang.reflect.Member;
import java.lang.reflect.Modifier;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.concurrent.NamedThreadFactory;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.Keyspace;
import org.apache.cassandra.db.lifecycle.View;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.Memory;
import org.apache.cassandra.io.util.SafeMemory;
import org.apache.cassandra.utils.NoSpamLogger;
import org.apache.cassandra.utils.Pair;
import org.apache.cassandra.utils.Throwables;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Ref<T> implements RefCounted<T> {
   static final Logger logger = LoggerFactory.getLogger(Ref.class);
   public static final boolean DEBUG_ENABLED = System.getProperty("cassandra.debugrefcount", "false").equalsIgnoreCase("true");
   final Ref.State state;
   final T referent;
   private static final Class<?>[] concurrentIterableClasses = new Class[]{ConcurrentLinkedQueue.class, ConcurrentLinkedDeque.class, ConcurrentSkipListSet.class, CopyOnWriteArrayList.class, CopyOnWriteArraySet.class, DelayQueue.class, NonBlockingHashMap.class};
   static final Set<Class<?>> concurrentIterables = Collections.newSetFromMap(new IdentityHashMap());
   private static final Set<Ref.GlobalState> globallyExtant = Collections.newSetFromMap(new ConcurrentHashMap());
   static final ReferenceQueue<Object> referenceQueue = new ReferenceQueue();
   private static final ExecutorService EXEC = Executors.newFixedThreadPool(1, new NamedThreadFactory("Reference-Reaper"));
   static final ScheduledExecutorService STRONG_LEAK_DETECTOR;
   static final Deque<Ref.InProgressVisit> inProgressVisitPool;
   static final Map<Class<?>, List<Field>> fieldMap;

   public Ref(T referent, RefCounted.Tidy tidy) {
      this.state = new Ref.State(new Ref.GlobalState(tidy), this, referenceQueue);
      this.referent = referent;
   }

   Ref(T referent, Ref.GlobalState state) {
      this.state = new Ref.State(state, this, referenceQueue);
      this.referent = referent;
   }

   public void release() {
      this.state.release(false);
   }

   public Throwable ensureReleased(Throwable accumulate) {
      return this.state.ensureReleased(accumulate);
   }

   public void ensureReleased() {
      Throwables.maybeFail(this.state.ensureReleased((Throwable)null));
   }

   public void close() {
      this.ensureReleased();
   }

   public T get() {
      this.state.assertNotReleased();
      return this.referent;
   }

   public Ref<T> tryRef() {
      return this.state.globalState.ref()?new Ref(this.referent, this.state.globalState):null;
   }

   public Ref<T> ref() {
      Ref<T> ref = this.tryRef();
      if(ref == null) {
         this.state.assertNotReleased();
      }

      return ref;
   }

   public String printDebugInfo() {
      if(DEBUG_ENABLED) {
         this.state.debug.log(this.state.toString());
         return "Memory was freed by " + this.state.debug.deallocateThread;
      } else {
         return "Memory was freed";
      }
   }

   public int globalCount() {
      return this.state.globalState.count();
   }

   static Ref.InProgressVisit newInProgressVisit(Object o, List<Field> fields, Field field, String name) {
      Objects.requireNonNull(o);
      Ref.InProgressVisit ipv = (Ref.InProgressVisit)inProgressVisitPool.pollLast();
      if(ipv == null) {
         ipv = new Ref.InProgressVisit();
      }

      ipv.o = o;
      if(o instanceof Object[]) {
         ipv.collectionIterator = Arrays.asList((Object[])((Object[])o)).iterator();
      } else if(o instanceof ConcurrentMap) {
         ipv.isMapIterator = true;
         ipv.collectionIterator = ((Map)o).entrySet().iterator();
      } else if(concurrentIterables.contains(o.getClass()) | o instanceof BlockingQueue) {
         ipv.collectionIterator = ((Iterable)o).iterator();
      }

      ipv.fields = fields;
      ipv.field = field;
      ipv.name = name;
      return ipv;
   }

   static void returnInProgressVisit(Ref.InProgressVisit ipv) {
      if(inProgressVisitPool.size() <= 1024) {
         ipv.name = null;
         ipv.fields = null;
         ipv.o = null;
         ipv.fieldIndex = 0;
         ipv.field = null;
         ipv.collectionIterator = null;
         ipv.mapEntryValue = null;
         ipv.isMapIterator = false;
         inProgressVisitPool.offer(ipv);
      }
   }

   static List<Field> getFields(Class<?> clazz) {
      if(clazz != null && clazz != PhantomReference.class && clazz != Class.class && !Member.class.isAssignableFrom(clazz)) {
         if(!ThreadGroup.class.isAssignableFrom(clazz) && !EpollEventLoop.class.isAssignableFrom(clazz) && !ThreadPoolExecutor.class.isAssignableFrom(clazz) && !CompletableFuture.class.isAssignableFrom(clazz)) {
            List<Field> fields = (List)fieldMap.get(clazz);
            if(fields != null) {
               return fields;
            } else {
               fieldMap.put(clazz, fields = new ArrayList());
               Field[] var2 = clazz.getDeclaredFields();
               int var3 = var2.length;

               for(int var4 = 0; var4 < var3; ++var4) {
                  Field field = var2[var4];
                  if(!field.getType().isPrimitive() && !Modifier.isStatic(field.getModifiers())) {
                     field.setAccessible(true);
                     fields.add(field);
                  }
               }

               fields.addAll(getFields(clazz.getSuperclass()));
               return fields;
            }
         } else {
            return Collections.emptyList();
         }
      } else {
         return Collections.emptyList();
      }
   }

   static {
      STRONG_LEAK_DETECTOR = !DEBUG_ENABLED?null:Executors.newScheduledThreadPool(1, new NamedThreadFactory("Strong-Reference-Leak-Detector"));
      EXEC.execute(new Ref.ReferenceReaper());
      if(DEBUG_ENABLED) {
         STRONG_LEAK_DETECTOR.scheduleAtFixedRate(new Ref.Visitor(), 1L, 15L, TimeUnit.MINUTES);
         STRONG_LEAK_DETECTOR.scheduleAtFixedRate(new Ref.StrongLeakDetector(), 2L, 15L, TimeUnit.MINUTES);
      }

      concurrentIterables.addAll(Arrays.asList(concurrentIterableClasses));
      inProgressVisitPool = new ArrayDeque();
      fieldMap = new HashMap();
   }

   private static class StrongLeakDetector implements Runnable {
      Set<RefCounted.Tidy> candidates;

      private StrongLeakDetector() {
         this.candidates = new HashSet();
      }

      public void run() {
         Set<RefCounted.Tidy> candidates = Collections.newSetFromMap(new IdentityHashMap());
         Iterator var2 = Ref.globallyExtant.iterator();

         while(var2.hasNext()) {
            Ref.GlobalState state = (Ref.GlobalState)var2.next();
            candidates.add(state.tidy);
         }

         this.removeExpected(candidates);
         this.candidates.retainAll(candidates);
         if(!this.candidates.isEmpty()) {
            List<String> names = new ArrayList(this.candidates.size());
            Iterator var6 = this.candidates.iterator();

            while(var6.hasNext()) {
               RefCounted.Tidy tidy = (RefCounted.Tidy)var6.next();
               names.add(tidy.name());
            }

            Ref.logger.warn("Strong reference leak candidates detected: {}", names);
         }

         this.candidates = candidates;
      }

      private void removeExpected(Set<RefCounted.Tidy> candidates) {
         Ref.IdentityCollection expected = new Ref.IdentityCollection(candidates);
         Iterator var3 = Keyspace.all().iterator();

         while(var3.hasNext()) {
            Keyspace ks = (Keyspace)var3.next();
            Iterator var5 = ks.getColumnFamilyStores().iterator();

            while(var5.hasNext()) {
               ColumnFamilyStore cfs = (ColumnFamilyStore)var5.next();
               View view = cfs.getTracker().getView();
               Iterator var8 = view.allKnownSSTables().iterator();

               while(var8.hasNext()) {
                  SSTableReader reader = (SSTableReader)var8.next();
                  reader.addTo(expected);
               }
            }
         }

      }
   }

   public static class IdentityCollection {
      final Set<RefCounted.Tidy> candidates;

      public IdentityCollection(Set<RefCounted.Tidy> candidates) {
         this.candidates = candidates;
      }

      public void add(Ref<?> ref) {
         this.candidates.remove(ref.state.globalState.tidy);
      }

      public void add(SelfRefCounted<?> ref) {
         this.add(ref.selfRef());
      }

      public void add(SharedCloseable ref) {
         if(ref instanceof SharedCloseableImpl) {
            this.add((SharedCloseableImpl)ref);
         }

      }

      public void add(SharedCloseableImpl ref) {
         this.add(ref.ref);
      }

      public void add(Memory memory) {
         if(memory instanceof SafeMemory) {
            ((SafeMemory)memory).addTo(this);
         }

      }
   }

   static class Visitor implements Runnable {
      final Deque<Ref.InProgressVisit> path = new ArrayDeque();
      final Set<Object> visited = Collections.newSetFromMap(new IdentityHashMap());
      @VisibleForTesting
      int lastVisitedCount;
      @VisibleForTesting
      long iterations = 0L;
      Ref.GlobalState visiting;
      Set<Ref.GlobalState> haveLoops;

      Visitor() {
      }

      public void run() {
         try {
            Iterator var1 = Ref.globallyExtant.iterator();

            while(var1.hasNext()) {
               Ref.GlobalState globalState = (Ref.GlobalState)var1.next();
               if(globalState.tidy != null) {
                  this.path.clear();
                  this.visited.clear();
                  this.lastVisitedCount = 0;
                  this.iterations = 0L;
                  this.visited.add(globalState);
                  this.visiting = globalState;
                  this.traverse(globalState.tidy);
               }
            }
         } catch (Throwable var6) {
            var6.printStackTrace();
         } finally {
            this.lastVisitedCount = this.visited.size();
            this.path.clear();
            this.visited.clear();
         }

      }

      void traverse(RefCounted.Tidy rootObject) {
         this.path.offer(Ref.newInProgressVisit(rootObject, Ref.getFields(rootObject.getClass()), (Field)null, rootObject.name()));
         Ref.InProgressVisit inProgress = null;

         while(inProgress != null || !this.path.isEmpty()) {
            if(inProgress == null) {
               inProgress = (Ref.InProgressVisit)this.path.pollLast();
            }

            try {
               Pair<Object, Field> p = inProgress.nextChild();
               Object child = null;
               Field field = null;
               if(p != null) {
                  ++this.iterations;
                  child = p.left;
                  field = (Field)p.right;
               }

               if(child != null && this.visited.add(child)) {
                  this.path.offer(inProgress);
                  inProgress = Ref.newInProgressVisit(child, Ref.getFields(child.getClass()), field, (String)null);
               } else if(this.visiting == child) {
                  if(this.haveLoops != null) {
                     this.haveLoops.add(this.visiting);
                  }

                  NoSpamLogger.log(Ref.logger, NoSpamLogger.Level.ERROR, rootObject.getClass().getName(), 1L, TimeUnit.SECONDS, "Strong self-ref loop detected, {} references itself:\n {}", new Object[]{this.visiting, String.join("\n", (Iterable)this.path.stream().map(Object::toString).collect(Collectors.toList()))});
               } else if(child == null) {
                  Ref.returnInProgressVisit(inProgress);
                  inProgress = null;
               }
            } catch (IllegalAccessException var6) {
               NoSpamLogger.log(Ref.logger, NoSpamLogger.Level.ERROR, 5L, TimeUnit.MINUTES, "Could not fully check for self-referential leaks", new Object[]{var6});
            }
         }

      }
   }

   static class InProgressVisit {
      String name;
      List<Field> fields;
      Object o;
      int fieldIndex = 0;
      Field field;
      boolean isMapIterator;
      Iterator<Object> collectionIterator;
      Object mapEntryValue;

      InProgressVisit() {
      }

      private Field nextField() {
         if(this.fields.isEmpty()) {
            return null;
         } else if(this.fieldIndex >= this.fields.size()) {
            return null;
         } else {
            Field retval = (Field)this.fields.get(this.fieldIndex);
            ++this.fieldIndex;
            return retval;
         }
      }

      Pair<Object, Field> nextChild() throws IllegalAccessException {
         if(this.mapEntryValue != null) {
            Pair<Object, Field> retval = Pair.create(this.mapEntryValue, this.field);
            this.mapEntryValue = null;
            return retval;
         } else if(this.collectionIterator == null) {
            while(true) {
               Field nextField = this.nextField();
               if(nextField == null) {
                  return null;
               }

               if(!(this.o instanceof WeakReference & nextField.getDeclaringClass() == Reference.class)) {
                  Object nextObject = nextField.get(this.o);
                  if(nextObject != null) {
                     return Pair.create(nextField.get(this.o), nextField);
                  }
               }
            }
         } else if(!this.collectionIterator.hasNext()) {
            return null;
         } else {
            Object nextItem = null;

            while(this.collectionIterator.hasNext() && (nextItem = this.collectionIterator.next()) == null) {
               ;
            }

            if(nextItem != null) {
               if(this.isMapIterator & nextItem instanceof Entry) {
                  Entry entry = (Entry)nextItem;
                  this.mapEntryValue = entry.getValue();
                  return Pair.create(entry.getKey(), this.field);
               } else {
                  return Pair.create(nextItem, this.field);
               }
            } else {
               return null;
            }
         }
      }

      public String toString() {
         return this.field == null?String.format("%s.%s", new Object[]{this.o.getClass().getName(), this.name}):this.field.toString();
      }
   }

   static final class ReferenceReaper implements Runnable {
      ReferenceReaper() {
      }

      public void run() {
         try {
            while(true) {
               Object obj = Ref.referenceQueue.remove();
               if(obj instanceof Ref.State) {
                  ((Ref.State)obj).release(true);
               }
            }
         } catch (InterruptedException var5) {
            ;
         } finally {
            Ref.EXEC.execute(this);
         }

      }
   }

   static final class GlobalState {
      private final Collection<Ref.State> locallyExtant = new ConcurrentLinkedDeque();
      private final AtomicInteger counts = new AtomicInteger();
      private final RefCounted.Tidy tidy;

      GlobalState(RefCounted.Tidy tidy) {
         this.tidy = tidy;
         Ref.globallyExtant.add(this);
      }

      void register(Ref.State ref) {
         this.locallyExtant.add(ref);
      }

      boolean ref() {
         int cur;
         do {
            cur = this.counts.get();
            if(cur < 0) {
               return false;
            }
         } while(!this.counts.compareAndSet(cur, cur + 1));

         return true;
      }

      Throwable release(Ref.State ref, Throwable accumulate) {
         this.locallyExtant.remove(ref);
         if(-1 == this.counts.decrementAndGet()) {
            Ref.globallyExtant.remove(this);

            try {
               if(this.tidy != null) {
                  this.tidy.tidy();
               }
            } catch (Throwable var4) {
               accumulate = Throwables.merge(accumulate, var4);
            }
         }

         return accumulate;
      }

      int count() {
         return 1 + this.counts.get();
      }

      public String toString() {
         return this.tidy != null?this.tidy.getClass() + "@" + System.identityHashCode(this.tidy) + ":" + this.tidy.name():"@" + System.identityHashCode(this);
      }
   }

   static final class Debug {
      String allocateThread;
      String deallocateThread;
      StackTraceElement[] allocateTrace;
      StackTraceElement[] deallocateTrace;

      Debug() {
         Thread thread = Thread.currentThread();
         this.allocateThread = thread.toString();
         this.allocateTrace = thread.getStackTrace();
      }

      synchronized void deallocate() {
         Thread thread = Thread.currentThread();
         this.deallocateThread = thread.toString();
         this.deallocateTrace = thread.getStackTrace();
      }

      synchronized void log(String id) {
         Ref.logger.error("Allocate trace {}:\n{}", id, this.print(this.allocateThread, this.allocateTrace));
         if(this.deallocateThread != null) {
            Ref.logger.error("Deallocate trace {}:\n{}", id, this.print(this.deallocateThread, this.deallocateTrace));
         }

      }

      String print(String thread, StackTraceElement[] trace) {
         StringBuilder sb = new StringBuilder();
         sb.append(thread);
         sb.append("\n");
         StackTraceElement[] var4 = trace;
         int var5 = trace.length;

         for(int var6 = 0; var6 < var5; ++var6) {
            StackTraceElement element = var4[var6];
            sb.append("\tat ");
            sb.append(element);
            sb.append("\n");
         }

         return sb.toString();
      }
   }

   static final class State extends PhantomReference<Ref> {
      final Ref.Debug debug;
      final Ref.GlobalState globalState;
      private volatile int released;
      private static final AtomicIntegerFieldUpdater<Ref.State> releasedUpdater = AtomicIntegerFieldUpdater.newUpdater(Ref.State.class, "released");

      public State(Ref.GlobalState globalState, Ref reference, ReferenceQueue<? super Ref> q) {
         super(reference, q);
         this.debug = Ref.DEBUG_ENABLED?new Ref.Debug():null;
         this.globalState = globalState;
         globalState.register(this);
      }

      void assertNotReleased() {
         if(Ref.DEBUG_ENABLED && this.released == 1) {
            this.debug.log(this.toString());
         }

         assert this.released == 0;

      }

      Throwable ensureReleased(Throwable accumulate) {
         if(releasedUpdater.getAndSet(this, 1) == 0) {
            accumulate = this.globalState.release(this, accumulate);
            if(Ref.DEBUG_ENABLED) {
               this.debug.deallocate();
            }
         }

         return accumulate;
      }

      void release(boolean leak) {
         if(!releasedUpdater.compareAndSet(this, 0, 1)) {
            if(!leak) {
               String id = this.toString();
               Ref.logger.error("BAD RELEASE: attempted to release a reference ({}) that has already been released", id);
               if(Ref.DEBUG_ENABLED) {
                  this.debug.log(id);
               }

               throw new IllegalStateException("Attempted to release a reference that has already been released");
            }
         } else {
            Throwable fail = this.globalState.release(this, (Throwable)null);
            if(leak) {
               String id = this.toString();
               Ref.logger.error("LEAK DETECTED: a reference ({}) to {} was not released before the reference was garbage collected", id, this.globalState);
               if(Ref.DEBUG_ENABLED) {
                  this.debug.log(id);
               }
            } else if(Ref.DEBUG_ENABLED) {
               this.debug.deallocate();
            }

            if(fail != null) {
               Ref.logger.error("Error when closing {}", this.globalState, fail);
            }

         }
      }
   }
}
