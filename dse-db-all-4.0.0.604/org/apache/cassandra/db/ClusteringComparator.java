package org.apache.cassandra.db;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteSource;

public class ClusteringComparator implements Comparator<Clusterable> {
   private final List<AbstractType<?>> clusteringTypes;
   private final Comparator<Clusterable> reverseComparator;

   public ClusteringComparator(AbstractType... clusteringTypes) {
      this((Iterable)ImmutableList.copyOf(clusteringTypes));
   }

   public ClusteringComparator(Iterable<AbstractType<?>> clusteringTypes) {
      this.clusteringTypes = ImmutableList.copyOf(clusteringTypes);
      this.reverseComparator = (c1, c2) -> {
         return this.compare(c2, c1);
      };
      Iterator var2 = clusteringTypes.iterator();

      while(var2.hasNext()) {
         AbstractType<?> type = (AbstractType)var2.next();
         type.checkComparable();
      }

   }

   public int size() {
      return this.clusteringTypes.size();
   }

   public List<AbstractType<?>> subtypes() {
      return this.clusteringTypes;
   }

   public AbstractType<?> subtype(int i) {
      return (AbstractType)this.clusteringTypes.get(i);
   }

   public Clustering make(Object... values) {
      if(values.length != this.size()) {
         throw new IllegalArgumentException(String.format("Invalid number of components, expecting %d but got %d", new Object[]{Integer.valueOf(this.size()), Integer.valueOf(values.length)}));
      } else {
         CBuilder builder = CBuilder.create(this);
         Object[] var3 = values;
         int var4 = values.length;

         for(int var5 = 0; var5 < var4; ++var5) {
            Object val = var3[var5];
            if(val instanceof ByteBuffer) {
               builder.add((ByteBuffer)val);
            } else {
               builder.add(val);
            }
         }

         return builder.build();
      }
   }

   public int compare(Clusterable c1, Clusterable c2) {
      return this.compare(c1.clustering(), c2.clustering());
   }

   public int compare(ClusteringPrefix c1, ClusteringPrefix c2) {
      int s1 = c1.size();
      int s2 = c2.size();
      int minSize = Math.min(s1, s2);

      for(int i = 0; i < minSize; ++i) {
         int cmp = this.compareComponent(i, c1.get(i), c2.get(i));
         if(cmp != 0) {
            return cmp;
         }
      }

      if(s1 == s2) {
         return ClusteringPrefix.Kind.compare(c1.kind(), c2.kind());
      } else {
         return s1 < s2?c1.kind().comparedToClustering:-c2.kind().comparedToClustering;
      }
   }

   public int compare(Clustering c1, Clustering c2) {
      return this.compare(c1, c2, this.size());
   }

   public int compare(Clustering c1, Clustering c2, int size) {
      for(int i = 0; i < size; ++i) {
         int cmp = this.compareComponent(i, c1.get(i), c2.get(i));
         if(cmp != 0) {
            return cmp;
         }
      }

      return 0;
   }

   public int compareComponent(int i, ByteBuffer v1, ByteBuffer v2) {
      return v1 == null?(v2 == null?0:-1):(v2 == null?1:((AbstractType)this.clusteringTypes.get(i)).compare(v1, v2));
   }

   public boolean isCompatibleWith(ClusteringComparator previous) {
      if(this == previous) {
         return true;
      } else if(this.size() < previous.size()) {
         return false;
      } else {
         for(int i = 0; i < previous.size(); ++i) {
            AbstractType<?> tprev = previous.subtype(i);
            AbstractType<?> tnew = this.subtype(i);
            if(!tnew.isCompatibleWith(tprev)) {
               return false;
            }
         }

         return true;
      }
   }

   public void validate(ClusteringPrefix clustering) {
      for(int i = 0; i < clustering.size(); ++i) {
         ByteBuffer value = clustering.get(i);
         if(value != null) {
            this.subtype(i).validate(value);
         }
      }

   }

   public ByteSource asByteComparableSource(ClusteringPrefix clustering) {
      return new ClusteringComparator.ClusteringSource(clustering);
   }

   public Comparator<Clusterable> reversed() {
      return this.reverseComparator;
   }

   public String toString() {
      return String.format("comparator(%s)", new Object[]{Joiner.on(", ").join(this.clusteringTypes)});
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof ClusteringComparator)) {
         return false;
      } else {
         ClusteringComparator that = (ClusteringComparator)o;
         return this.clusteringTypes.equals(that.clusteringTypes);
      }
   }

   public int hashCode() {
      return Objects.hashCode(this.clusteringTypes);
   }

   class ClusteringSource extends ByteSource.WithToString {
      final ClusteringPrefix src;
      ByteSource current = null;
      int srcnum = -1;

      ClusteringSource(ClusteringPrefix src) {
         this.src = src;
      }

      public int next() {
         int sz;
         if(this.current != null) {
            sz = this.current.next();
            if(sz > -1) {
               return sz;
            }

            this.current = null;
         }

         sz = this.src.size();
         if(this.srcnum == sz) {
            return -1;
         } else {
            ++this.srcnum;
            if(this.srcnum == sz) {
               return this.src.kind().asByteComparableValue();
            } else {
               this.current = ClusteringComparator.this.subtype(this.srcnum).asByteComparableSource(this.src.get(this.srcnum));
               if(this.current == null) {
                  return ClusteringComparator.this.subtype(this.srcnum).isReversed()?65:63;
               } else {
                  this.current.reset();
                  return 64;
               }
            }
         }
      }

      public void reset() {
         this.srcnum = -1;
         this.current = null;
      }
   }
}
