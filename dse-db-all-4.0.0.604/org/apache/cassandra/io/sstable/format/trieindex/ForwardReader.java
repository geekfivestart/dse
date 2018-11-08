package org.apache.cassandra.io.sstable.format.trieindex;

import java.io.IOException;
import org.apache.cassandra.db.Slices;
import org.apache.cassandra.db.rows.RangeTombstoneMarker;
import org.apache.cassandra.db.rows.SerializationHelper;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.io.sstable.format.AbstractReader;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileDataInput;

class ForwardReader extends AbstractReader {
   ForwardReader(SSTableReader sstable, Slices slices, FileDataInput file, boolean shouldCloseFile, SerializationHelper helper) {
      super(sstable, slices, file, shouldCloseFile, helper, false);
   }

   protected RangeTombstoneMarker sliceStartMarker() {
      return markerFrom(this.start, this.openMarker);
   }

   protected Unfiltered nextInSlice() throws IOException {
      return this.readUnfiltered();
   }

   protected RangeTombstoneMarker sliceEndMarker() {
      return markerFrom(this.end, this.openMarker);
   }
}
