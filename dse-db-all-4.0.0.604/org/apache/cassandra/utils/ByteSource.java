package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public interface ByteSource {
   int END_OF_STREAM = -1;
   int ESCAPE = 0;
   int ESCAPED_0_CONT = 254;
   int ESCAPED_0_DONE = 255;
   int NEXT_COMPONENT = 64;
   int NEXT_COMPONENT_NULL = 63;
   int NEXT_COMPONENT_NULL_REVERSED = 65;
   int LT_NEXT_COMPONENT = 32;
   int GT_NEXT_COMPONENT = 96;
   ByteSource MAX;
   ByteSource EMPTY;
   int NONE = -2;

   int next();

   void reset();

   static default ByteSource of(ByteBuffer buf) {
      return new ByteSource.Reinterpreter(buf);
   }

   static default ByteSource of(byte[] buf) {
      return new ByteSource.ReinterpreterArray(buf);
   }

   static default ByteSource of(ByteSource... srcs) {
      return new ByteSource.Multi(srcs);
   }

   static default ByteSource withTerminator(int terminator, ByteSource... srcs) {
      return new ByteSource.Multi(srcs, terminator);
   }

   static default ByteSource of(String s) {
      return new ByteSource.ReinterpreterArray(s.getBytes(StandardCharsets.UTF_8));
   }

   static default ByteSource of(long value) {
      return new ByteSource.Number(value ^ -9223372036854775808L, 8);
   }

   static default ByteSource of(int value) {
      return new ByteSource.Number((long)value ^ 2147483648L, 4);
   }

   static default ByteSource optionalSignedFixedLengthNumber(ByteBuffer b) {
      return b.hasRemaining()?signedFixedLengthNumber(b):null;
   }

   static default ByteSource signedFixedLengthNumber(ByteBuffer b) {
      return new ByteSource.SignedFixedLengthNumber(b);
   }

   static default ByteSource optionalSignedFixedLengthFloat(ByteBuffer b) {
      return b.hasRemaining()?signedFixedLengthFloat(b):null;
   }

   static default ByteSource signedFixedLengthFloat(ByteBuffer b) {
      return new ByteSource.SignedFixedLengthFloat(b);
   }

   static default ByteSource empty() {
      return EMPTY;
   }

   static default ByteSource separatorPrefix(ByteSource prevMax, ByteSource currMin) {
      return new ByteSource.Separator(prevMax, currMin, true);
   }

   static default ByteSource separatorGt(ByteSource prevMax, ByteSource currMin) {
      return new ByteSource.Separator(prevMax, currMin, false);
   }

   static default int compare(ByteSource bs1, ByteSource bs2) {
      if(bs1 != null && bs2 != null) {
         bs1.reset();
         bs2.reset();

         int b1;
         do {
            b1 = bs1.next();
            int b2 = bs2.next();
            int cmp = Integer.compare(b1, b2);
            if(cmp != 0) {
               return cmp;
            }
         } while(b1 != -1);

         return 0;
      } else {
         return Boolean.compare(bs1 != null, bs2 != null);
      }
   }

   static default ByteSource oneByte(final int i) {
      if(null.$assertionsDisabled || i >= 0 && i <= 255) {
         return new ByteSource.WithToString() {
            boolean given = false;

            public int next() {
               if(this.given) {
                  return -1;
               } else {
                  this.given = true;
                  return i;
               }
            }

            public void reset() {
               this.given = false;
            }
         };
      } else {
         throw new AssertionError();
      }
   }

   static default ByteSource cut(final ByteSource src, final int cutoff) {
      return new ByteSource.WithToString() {
         int pos = 0;

         public int next() {
            return this.pos++ < cutoff?src.next():-1;
         }

         public void reset() {
            src.reset();
            this.pos = 0;
         }
      };
   }

   static default int diffPoint(ByteSource s1, ByteSource s2) {
      s1.reset();
      s2.reset();

      int pos;
      int b;
      for(pos = 1; (b = s1.next()) == s2.next() && b != -1; ++pos) {
         ;
      }

      return pos;
   }

   static default ByteSource max() {
      return MAX;
   }

   static default ByteSource optionalFixedLength(ByteBuffer b) {
      return b.hasRemaining()?fixedLength(b):null;
   }

   static default ByteSource fixedLength(final ByteBuffer b) {
      return new ByteSource.WithToString() {
         int pos = b.position() - 1;

         public int next() {
            return ++this.pos < b.limit()?b.get(this.pos) & 255:-1;
         }

         public void reset() {
            this.pos = b.position() - 1;
         }
      };
   }

   static default ByteSource fourBit(final ByteSource s) {
      return new ByteSource.WithToString() {
         int pos = 0;
         int v = 0;

         public int next() {
            if((this.pos++ & 1) == 0) {
               this.v = s.next();
               return this.v == -1?-1:this.v >> 4 & 15;
            } else {
               return this.v & 15;
            }
         }

         public void reset() {
            s.reset();
            this.pos = 0;
         }
      };
   }

   static default ByteSource splitBytes(final ByteSource s, final int bitCount) {
      return new ByteSource.WithToString() {
         int pos = 8;
         int v = 0;
         int mask = (1 << bitCount) - 1;

         public int next() {
            if((this.pos += bitCount) >= 8) {
               this.pos = 0;
               this.v = s.next();
               if(this.v == -1) {
                  return -1;
               }
            }

            this.v <<= bitCount;
            return this.v >> 8 & this.mask;
         }

         public void reset() {
            s.reset();
            this.pos = 8;
         }
      };
   }

   static default {
      if(null.$assertionsDisabled) {
         ;
      }

      MAX = new ByteSource() {
         public int next() {
            return 255;
         }

         public void reset() {
         }

         public String toString() {
            return "MAX";
         }
      };
      EMPTY = new ByteSource.WithToString() {
         public int next() {
            return -1;
         }

         public void reset() {
         }
      };
   }

   public static class Separator extends ByteSource.WithToString {
      final ByteSource prev;
      final ByteSource curr;
      boolean done = false;
      final boolean useCurr;

      Separator(ByteSource prevMax, ByteSource currMin, boolean useCurr) {
         this.prev = prevMax;
         this.curr = currMin;
         this.useCurr = useCurr;
      }

      public int next() {
         if(this.done) {
            return -1;
         } else {
            int p = this.prev.next();
            int c = this.curr.next();

            assert p <= c : this.prev + " not less than " + this.curr;

            if(p == c) {
               return c;
            } else {
               this.done = true;
               return this.useCurr?c:p + 1;
            }
         }
      }

      public void reset() {
         this.done = false;
         this.prev.reset();
         this.curr.reset();
      }
   }

   public static class Multi extends ByteSource.WithToString {
      final ByteSource[] srcs;
      int srcnum;
      int terminator;

      Multi(ByteSource[] srcs) {
         this(srcs, -1);
      }

      Multi(ByteSource[] srcs, int terminator) {
         this.srcnum = -1;
         this.srcs = srcs;
         this.terminator = terminator;
      }

      public int next() {
         if(this.srcnum == this.srcs.length) {
            return -1;
         } else {
            int b = -1;
            if(this.srcnum >= 0 && this.srcs[this.srcnum] != null) {
               b = this.srcs[this.srcnum].next();
            }

            if(b > -1) {
               return b;
            } else {
               ++this.srcnum;
               if(this.srcnum == this.srcs.length) {
                  return this.terminator;
               } else if(this.srcs[this.srcnum] == null) {
                  return 63;
               } else {
                  this.srcs[this.srcnum].reset();
                  return 64;
               }
            }
         }
      }

      public void reset() {
         this.srcnum = -1;
      }
   }

   public static class SignedFixedLengthFloat extends ByteSource.WithToString {
      ByteBuffer buf;
      int bufpos;
      boolean invert;

      public SignedFixedLengthFloat(ByteBuffer buf) {
         this.buf = buf;
         this.bufpos = buf.position();
      }

      public int next() {
         if(this.bufpos >= this.buf.limit()) {
            return -1;
         } else {
            int v = this.buf.get(this.bufpos) & 255;
            if(this.bufpos == this.buf.position()) {
               this.invert = v >= 128;
               v |= 128;
            }

            if(this.invert) {
               v ^= 255;
            }

            ++this.bufpos;
            return v;
         }
      }

      public void reset() {
         this.bufpos = this.buf.position();
      }
   }

   public static class Number extends ByteSource.WithToString {
      final int length;
      final long value;
      int pos;

      public Number(long value, int length) {
         this.length = length;
         this.value = value;
         this.reset();
      }

      public void reset() {
         this.pos = this.length;
      }

      public int next() {
         return this.pos == 0?-1:(int)(this.value >> --this.pos * 8 & 255L);
      }
   }

   public static class SignedFixedLengthNumber extends ByteSource.WithToString {
      ByteBuffer buf;
      int bufpos;

      public SignedFixedLengthNumber(ByteBuffer buf) {
         this.buf = buf;
         this.bufpos = buf.position();
      }

      public int next() {
         if(this.bufpos >= this.buf.limit()) {
            return -1;
         } else {
            int v = this.buf.get(this.bufpos) & 255;
            if(this.bufpos == this.buf.position()) {
               v ^= 128;
            }

            ++this.bufpos;
            return v;
         }
      }

      public void reset() {
         this.bufpos = this.buf.position();
      }
   }

   public static class ReinterpreterArray extends ByteSource.WithToString {
      byte[] buf;
      int bufpos;
      boolean escaped;

      ReinterpreterArray(byte[] buf) {
         this.buf = buf;
         this.reset();
      }

      public int next() {
         if(this.bufpos >= this.buf.length) {
            if(this.escaped) {
               this.escaped = false;
               return 254;
            } else {
               return this.bufpos++ > this.buf.length?-1:0;
            }
         } else {
            int b = this.buf[this.bufpos++] & 255;
            if(!this.escaped) {
               if(b == 0) {
                  this.escaped = true;
               }

               return b;
            } else if(b == 0) {
               return 254;
            } else {
               --this.bufpos;
               this.escaped = false;
               return 255;
            }
         }
      }

      public void reset() {
         this.bufpos = 0;
         this.escaped = false;
      }
   }

   public static class Reinterpreter extends ByteSource.WithToString {
      ByteBuffer buf;
      int bufpos;
      boolean escaped;

      Reinterpreter(ByteBuffer buf) {
         this.buf = buf;
         this.reset();
      }

      public int next() {
         if(this.bufpos >= this.buf.limit()) {
            if(this.escaped) {
               this.escaped = false;
               return 254;
            } else {
               return this.bufpos++ > this.buf.limit()?-1:0;
            }
         } else {
            int b = this.buf.get(this.bufpos++) & 255;
            if(!this.escaped) {
               if(b == 0) {
                  this.escaped = true;
               }

               return b;
            } else if(b == 0) {
               return 254;
            } else {
               --this.bufpos;
               this.escaped = false;
               return 255;
            }
         }
      }

      public void reset() {
         this.bufpos = this.buf.position();
         this.escaped = false;
      }
   }

   public abstract static class WithToString implements ByteSource {
      public WithToString() {
      }

      public String toString() {
         StringBuilder builder = new StringBuilder();
         this.reset();

         for(int b = this.next(); b != -1; b = this.next()) {
            builder.append(Integer.toHexString(b >> 4 & 15)).append(Integer.toHexString(b & 15));
         }

         return builder.toString();
      }
   }
}
