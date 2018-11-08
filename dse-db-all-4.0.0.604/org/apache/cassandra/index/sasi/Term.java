package org.apache.cassandra.index.sasi;

import java.nio.ByteBuffer;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.index.sasi.disk.OnDiskIndexBuilder;
import org.apache.cassandra.index.sasi.utils.MappedBuffer;

public class Term {
   protected final MappedBuffer content;
   protected final OnDiskIndexBuilder.TermSize termSize;
   private final boolean hasMarkedPartials;

   public Term(MappedBuffer content, OnDiskIndexBuilder.TermSize size, boolean hasMarkedPartials) {
      this.content = content;
      this.termSize = size;
      this.hasMarkedPartials = hasMarkedPartials;
   }

   public ByteBuffer getTerm() {
      long offset = this.termSize.isConstant()?this.content.position():this.content.position() + 2L;
      int length = this.termSize.isConstant()?this.termSize.size:this.readLength(this.content.position());
      return this.content.getPageRegion(offset, length);
   }

   public boolean isPartial() {
      return !this.termSize.isConstant() && this.hasMarkedPartials && (this.content.getShort(this.content.position()) & '耀') != 0;
   }

   public long getDataOffset() {
      long position = this.content.position();
      return position + (long)(this.termSize.isConstant()?this.termSize.size:2 + this.readLength(position));
   }

   public int compareTo(AbstractType<?> comparator, ByteBuffer query) {
      return this.compareTo(comparator, query, true);
   }

   public int compareTo(AbstractType<?> comparator, ByteBuffer query, boolean checkFully) {
      long position = this.content.position();
      int padding = this.termSize.isConstant()?0:2;
      int len = this.termSize.isConstant()?this.termSize.size:this.readLength(position);
      return this.content.comparePageTo(position + (long)padding, checkFully?len:Math.min(len, query.remaining()), comparator, query);
   }

   private short readLength(long position) {
      return (short)(this.content.getShort(position) & -32769);
   }
}
