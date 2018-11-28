package org.apache.cassandra.auth.enums;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableList.Builder;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Stream;
import javax.annotation.Nullable;

public class PartitionedEnumSet<E extends PartitionedEnum> implements Set<E> {
   private final Domains<E> registry;
   private final BitSet bits;
   private final Class<E> elementType;
   private final boolean isImmutable;

   public static <E extends PartitionedEnum> PartitionedEnumSet<E> allOf(Class<E> elementType, Class<? extends E> domain) {
      return of(elementType, (E[])domain.getEnumConstants());
   }

   public static <E extends PartitionedEnum> PartitionedEnumSet<E> immutableSetOfAll(Class<E> elementType, Class<? extends E> domain) {
      return immutableSetOf(elementType, (E[])domain.getEnumConstants());
   }

   public static <E extends PartitionedEnum> PartitionedEnumSet<E> of(Class<E> elementType, E... elements) {
      Domains<E> domains = Domains.getDomains(elementType);
      if(domains == null) {
         throw new IllegalArgumentException("Unknown PartitionedEnumType " + elementType.getName());
      } else {
         return new PartitionedEnumSet(domains, Arrays.asList(elements), false);
      }
   }

   public static <E extends PartitionedEnum> PartitionedEnumSet<E> of(Class<E> elementType, Iterable<E> elements) {
      Domains<E> domains = Domains.getDomains(elementType);
      if(domains == null) {
         throw new IllegalArgumentException("Unknown PartitionedEnumType " + elementType.getName());
      } else {
         return new PartitionedEnumSet(domains, elements, false);
      }
   }

   public static <E extends PartitionedEnum> PartitionedEnumSet<E> immutableSetOf(Class<E> elementType, E... elements) {
      Domains<E> domains = Domains.getDomains(elementType);
      if(domains == null) {
         throw new IllegalArgumentException("Unknown PartitionedEnumType " + elementType.getName());
      } else {
         return new PartitionedEnumSet(domains, Arrays.asList(elements), true);
      }
   }

   public static <E extends PartitionedEnum> PartitionedEnumSet<E> immutableSetOf(Class<E> elementType, Iterable<E> elements) {
      Domains<E> domains = Domains.getDomains(elementType);
      if(domains == null) {
         throw new IllegalArgumentException("Unknown PartitionedEnumType " + elementType.getName());
      } else {
         return new PartitionedEnumSet(domains, elements, true);
      }
   }

   public static <E extends PartitionedEnum> PartitionedEnumSet<E> noneOf(Class<E> elementType) {
      Domains<E> domains = Domains.getDomains(elementType);
      if(domains == null) {
         throw new IllegalArgumentException("Unknown PartitionedEnumType " + elementType.getName());
      } else {
         return new PartitionedEnumSet(domains, Collections.emptyList(), false);
      }
   }

   public static <E extends PartitionedEnum> PartitionedEnumSet<E> immutableEmptySetOf(Class<E> elementType) {
      Domains<E> domains = Domains.getDomains(elementType);
      if(domains == null) {
         throw new IllegalArgumentException("Unknown PartitionedEnumType " + elementType.getName());
      } else {
         return new PartitionedEnumSet(domains, Collections.emptyList(), true);
      }
   }

   private PartitionedEnumSet(Domains<E> registry, Iterable<E> elements, boolean isImmutable) {
      this.registry = registry;
      this.elementType = registry.getType();
      if(elements instanceof PartitionedEnumSet) {
         PartitionedEnumSet src = (PartitionedEnumSet)elements;
         this.bits = (BitSet)src.bits.clone();
      } else {
         this.bits = new BitSet();
         elements.forEach(this::add);
      }

      this.isImmutable = isImmutable;
   }

   public int size() {
      return this.bits.isEmpty()?0:this.bits.cardinality();
   }

   public boolean contains(Object o) {
      if(o == null) {
         return false;
      } else if(!(o instanceof PartitionedEnum)) {
         return false;
      } else {
         PartitionedEnum element = (PartitionedEnum)o;
         int bit = this.checkElementGetBit(element);
         return this.bits.get(bit);
      }
   }

   public boolean intersects(PartitionedEnumSet<E> other) {
      return this.bits.intersects(other.bits);
   }

   public boolean isEmpty() {
      return this.bits.isEmpty();
   }

   public Iterator<E> iterator() {
      Builder<E> builder = ImmutableList.builder();
      this.forEach(builder::add);
      return builder.build().iterator();
   }

   public void forEach(Consumer<? super E> action) {
      for (Domains.Domain d : this.registry.domains()) {
         int off = d.bitOffset;
         for (Enum e : d.enumType.getEnumConstants()) {
            if (!this.bits.get(off + e.ordinal())) continue;
            action.accept((E)e);
         }
      }
   }

   public Stream<E> stream() {
      List<E> lst = new ArrayList();
      this.forEach(lst::add);
      return lst.stream();
   }

   public String toString() {
      StringBuilder builder = new StringBuilder();
      this.forEach((e) -> {
         builder.append(e.getFullName());
         builder.append(", ");
      });
      if(builder.length() >= 2) {
         builder.setLength(builder.length() - 2);
      }

      return String.format("PartitionedEnumSet [%s]", new Object[]{builder.toString()});
   }

   private ImmutableSet<E> asImmutableSet() {
      com.google.common.collect.ImmutableSet.Builder<E> builder = ImmutableSet.builder();
      this.forEach(builder::add);
      return builder.build();
   }

   private int checkElementGetBit(PartitionedEnum element) {
      Domains.Domain d = this.registry.domain(element.domain());
      if(d != null && ((Enum[])d.enumType.getEnumConstants())[element.ordinal()] == element) {
         return d.bitOffset + element.ordinal();
      } else {
         throw new IllegalArgumentException(String.format("Supplied and registered elements are not equal (%s)", new Object[]{element}));
      }
   }

   private boolean intersect(PartitionedEnumSet<E> other) {
      boolean modified = false;
      if(other.elementType != this.elementType) {
         modified = !this.isEmpty();
         this.clear();
         return modified;
      } else {
         long[] before = this.bits.toLongArray();
         this.bits.and(other.bits);
         return !Arrays.equals(before, this.bits.toLongArray());
      }
   }

   private boolean union(PartitionedEnumSet<E> other) {
      long[] before = this.bits.toLongArray();
      this.bits.or(other.bits);
      return !Arrays.equals(before, this.bits.toLongArray());
   }

   private boolean contains(PartitionedEnumSet<E> other) {
      BitSet otherBits = other.bits;
      BitSet otherCopy = new BitSet();
      otherCopy.or(otherBits);
      otherCopy.and(this.bits);
      return otherCopy.equals(otherBits);
   }

   private boolean remove(PartitionedEnumSet<E> other) {
      long[] before = this.bits.toLongArray();
      this.bits.andNot(other.bits);
      return !Arrays.equals(before, this.bits.toLongArray());
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o instanceof PartitionedEnumSet) {
         PartitionedEnumSet<?> other = (PartitionedEnumSet)o;
         return this.elementType == other.elementType && this.bits.equals(other.bits);
      } else {
         return o instanceof Set && this.asImmutableSet().equals(o);
      }
   }

   public int hashCode() {
      return (new Consumer<E>() {
         int sum;

         {
            PartitionedEnumSet.this.forEach(this);
         }

         public void accept(E e) {
            this.sum += e.hashCode();
         }
      }).sum;
   }

   public Object[] toArray() {
      Object[] r = new Object[this.bits.cardinality()];
      return this.toArrayInt(r);
   }

   private Object[] toArrayInt(Object[] r) {
      int i = 0;
      Iterator var3 = this.registry.domains().iterator();

      while(var3.hasNext()) {
         Domains.Domain d = (Domains.Domain)var3.next();
         Enum[] enums = (Enum[])d.enumType.getEnumConstants();

         for(int n = 0; n < enums.length; ++n) {
            if(this.bits.get(d.bitOffset + n)) {
               r[i++] = enums[n];
            }
         }
      }

      return r;
   }

   public <T> T[] toArray(T[] a) {
      int size = this.size();
      T[] array = a.length >= size?a:(T[])((Object[])Array.newInstance(a.getClass().getComponentType(), size));
      T[] r = (T[])this.toArrayInt(array);

      for(int i = size; i < a.length; ++i) {
         r[i] = null;
      }

      return r;
   }

   private void checkIsMutable() {
      if(this.isImmutable) {
         throw new UnsupportedOperationException("This PartitionedEnumSet is immutable");
      }
   }

   public boolean add(E element) {
      this.checkIsMutable();
      int bit = this.checkElementGetBit(element);
      boolean present = this.bits.get(bit);
      this.bits.set(bit);
      return !present;
   }

   public boolean remove(Object o) {
      this.checkIsMutable();
      if(!this.contains(o)) {
         return false;
      } else {
         PartitionedEnum element = (PartitionedEnum)o;
         int bit = this.checkElementGetBit(element);
         boolean present = this.bits.get(bit);
         if(!present) {
            return false;
         } else {
            this.bits.clear(bit);
            return true;
         }
      }
   }

   public boolean containsAll(@Nullable Collection<?> c) {
      if(c == null) {
         throw new NullPointerException();
      } else if(!(c instanceof PartitionedEnumSet)) {
         Iterator var4 = c.iterator();

         Object o;
         do {
            if(!var4.hasNext()) {
               return true;
            }

            o = var4.next();
         } while(this.contains(o));

         return false;
      } else {
         PartitionedEnumSet p = (PartitionedEnumSet)c;
         return p.elementType == this.elementType && this.contains(p);
      }
   }

   public boolean addAll(@Nullable Collection<? extends E> c) {
      this.checkIsMutable();
      if(c == null) {
         throw new NullPointerException();
      } else if(!(c instanceof PartitionedEnumSet)) {
         boolean added = false;
         Iterator var3 = c.iterator();

         while(var3.hasNext()) {
            E e = (E)var3.next();
            if(this.add(e)) {
               added = true;
            }
         }

         return added;
      } else {
         PartitionedEnumSet p = (PartitionedEnumSet)c;
         return p.elementType == this.elementType && this.union(p);
      }
   }

   public boolean retainAll(@Nullable Collection<?> c) {
      this.checkIsMutable();
      if(c == null) {
         throw new NullPointerException();
      } else if(c.isEmpty()) {
         if(this.isEmpty()) {
            return false;
         } else {
            this.clear();
            return true;
         }
      } else if(!(c instanceof PartitionedEnumSet)) {
         boolean removed = false;
         Iterator iter = this.iterator();

         while(iter.hasNext()) {
            PartitionedEnum e = (PartitionedEnum)iter.next();
            if(!c.contains(e)) {
               removed = this.remove((Object)e);
            }
         }

         return removed;
      } else {
         PartitionedEnumSet p = (PartitionedEnumSet)c;
         return p.elementType == this.elementType && this.intersect(p);
      }
   }

   public boolean removeAll(@Nullable Collection<?> c) {
      this.checkIsMutable();
      if(c == null) {
         throw new NullPointerException();
      } else if(!(c instanceof PartitionedEnumSet)) {
         boolean removed = false;
         Iterator var3 = c.iterator();

         while(var3.hasNext()) {
            Object o = var3.next();
            if(this.remove(o)) {
               removed = true;
            }
         }

         return removed;
      } else {
         PartitionedEnumSet p = (PartitionedEnumSet)c;
         return p.elementType == this.elementType && this.remove((PartitionedEnumSet)c);
      }
   }

   public void clear() {
      this.checkIsMutable();
      this.bits.clear();
   }
}
