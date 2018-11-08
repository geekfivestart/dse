package org.apache.cassandra.io.util;

public interface RebuffererFactory extends ReaderFileProxy {
   default Rebufferer instantiateRebufferer() {
      return this.instantiateRebufferer(FileAccessType.RANDOM);
   }

   Rebufferer instantiateRebufferer(FileAccessType var1);
}
