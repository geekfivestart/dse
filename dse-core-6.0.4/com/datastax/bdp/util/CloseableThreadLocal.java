package com.datastax.bdp.util;

import java.io.Closeable;
import java.lang.ref.WeakReference;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CloseableThreadLocal<T> implements Closeable {
   private ThreadLocal<WeakReference<T>> t = new ThreadLocal();
   private Map<Thread, T> hardRefs = new HashMap();

   public CloseableThreadLocal() {
   }

   protected T initialValue() {
      return null;
   }

   public T get() {
      WeakReference<T> weakRef = (WeakReference)this.t.get();
      if(weakRef == null) {
         T iv = this.initialValue();
         if(iv != null) {
            this.set(iv);
            return iv;
         } else {
            return null;
         }
      } else {
         return weakRef.get();
      }
   }

   public void set(T object) {
      this.t.set(new WeakReference(object));
      Map var2 = this.hardRefs;
      synchronized(this.hardRefs) {
         this.hardRefs.put(Thread.currentThread(), object);
         Iterator it = this.hardRefs.keySet().iterator();

         while(it.hasNext()) {
            Thread t = (Thread)it.next();
            if(!t.isAlive()) {
               it.remove();
            }
         }

      }
   }

   public void close() {
      this.hardRefs = null;
      if(this.t != null) {
         this.t.remove();
      }

      this.t = null;
   }
}
