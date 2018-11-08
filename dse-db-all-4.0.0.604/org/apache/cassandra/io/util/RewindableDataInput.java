package org.apache.cassandra.io.util;

import java.io.IOException;

public interface RewindableDataInput extends DataInputPlus {
   DataPosition mark();

   void reset(DataPosition var1) throws IOException;

   long bytesPastMark(DataPosition var1);
}
