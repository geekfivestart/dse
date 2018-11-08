package org.apache.cassandra.utils;

import java.io.IOException;
import org.apache.cassandra.io.util.DataOutputPlus;

public interface PageAware {
   int PAGE_SIZE = 4096;

   static default long pageLimit(long dstPos) {
      return (dstPos | 4095L) + 1L;
   }

   static default long pageStart(long dstPos) {
      return dstPos & -4096L;
   }

   static default long padded(long dstPos) {
      return pageLimit(dstPos - 1L);
   }

   static default void pad(DataOutputPlus dest) throws IOException {
      long position = dest.position();
      long bytesLeft = pageLimit(position) - position;
      if(bytesLeft < 4096L) {
         dest.write(PageAware.EmptyPage.EMPTY_PAGE, 0, (int)bytesLeft);
      }

   }

   public static class EmptyPage {
      static final byte[] EMPTY_PAGE = new byte[4096];

      public EmptyPage() {
      }
   }
}
