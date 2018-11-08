package org.apache.cassandra.utils.obs;

import java.io.Closeable;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.cassandra.utils.concurrent.Ref;

public interface IBitSet extends Closeable {
   long capacity();

   boolean get(long var1);

   void set(long var1);

   void clear(long var1);

   void serialize(DataOutput var1) throws IOException;

   long serializedSize();

   void clear();

   void close();

   long offHeapSize();

   void addTo(Ref.IdentityCollection var1);
}
