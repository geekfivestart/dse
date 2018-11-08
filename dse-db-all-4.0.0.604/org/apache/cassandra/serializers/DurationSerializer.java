package org.apache.cassandra.serializers;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.cassandra.cql3.Duration;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBufferFixed;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.vint.VIntCoding;

public final class DurationSerializer implements TypeSerializer<Duration> {
   public static final DurationSerializer instance = new DurationSerializer();

   public DurationSerializer() {
   }

   public ByteBuffer serialize(Duration duration) {
      if(duration == null) {
         return ByteBufferUtil.EMPTY_BYTE_BUFFER;
      } else {
         long months = (long)duration.getMonths();
         long days = (long)duration.getDays();
         long nanoseconds = duration.getNanoseconds();
         int size = VIntCoding.computeVIntSize(months) + VIntCoding.computeVIntSize(days) + VIntCoding.computeVIntSize(nanoseconds);

         try {
            DataOutputBufferFixed output = new DataOutputBufferFixed(size);
            Throwable var10 = null;

            ByteBuffer var11;
            try {
               output.writeVInt(months);
               output.writeVInt(days);
               output.writeVInt(nanoseconds);
               var11 = output.buffer();
            } catch (Throwable var21) {
               var10 = var21;
               throw var21;
            } finally {
               if(output != null) {
                  if(var10 != null) {
                     try {
                        output.close();
                     } catch (Throwable var20) {
                        var10.addSuppressed(var20);
                     }
                  } else {
                     output.close();
                  }
               }

            }

            return var11;
         } catch (IOException var23) {
            throw new AssertionError("Unexpected error", var23);
         }
      }
   }

   public Duration deserialize(ByteBuffer bytes) {
      if(bytes.remaining() == 0) {
         return null;
      } else {
         try {
            DataInputBuffer in = new DataInputBuffer(bytes, true);
            Throwable var3 = null;

            Duration var8;
            try {
               int months = (int)in.readVInt();
               int days = (int)in.readVInt();
               long nanoseconds = in.readVInt();
               var8 = Duration.newInstance(months, days, nanoseconds);
            } catch (Throwable var18) {
               var3 = var18;
               throw var18;
            } finally {
               if(in != null) {
                  if(var3 != null) {
                     try {
                        in.close();
                     } catch (Throwable var17) {
                        var3.addSuppressed(var17);
                     }
                  } else {
                     in.close();
                  }
               }

            }

            return var8;
         } catch (IOException var20) {
            throw new AssertionError("Unexpected error", var20);
         }
      }
   }

   public void validate(ByteBuffer bytes) throws MarshalException {
      if(bytes.remaining() < 3) {
         throw new MarshalException(String.format("Expected at least 3 bytes for a duration (%d)", new Object[]{Integer.valueOf(bytes.remaining())}));
      } else {
         try {
            DataInputBuffer in = new DataInputBuffer(bytes, true);
            Throwable var3 = null;

            try {
               long monthsAsLong = in.readVInt();
               long daysAsLong = in.readVInt();
               long nanoseconds = in.readVInt();
               if(!this.canBeCastToInt(monthsAsLong)) {
                  throw new MarshalException(String.format("The duration months must be a 32 bits integer but was: %d", new Object[]{Long.valueOf(monthsAsLong)}));
               }

               if(!this.canBeCastToInt(daysAsLong)) {
                  throw new MarshalException(String.format("The duration days must be a 32 bits integer but was: %d", new Object[]{Long.valueOf(daysAsLong)}));
               }

               int months = (int)monthsAsLong;
               int days = (int)daysAsLong;
               if((months < 0 || days < 0 || nanoseconds < 0L) && (months > 0 || days > 0 || nanoseconds > 0L)) {
                  throw new MarshalException(String.format("The duration months, days and nanoseconds must be all of the same sign (%d, %d, %d)", new Object[]{Integer.valueOf(months), Integer.valueOf(days), Long.valueOf(nanoseconds)}));
               }
            } catch (Throwable var20) {
               var3 = var20;
               throw var20;
            } finally {
               if(in != null) {
                  if(var3 != null) {
                     try {
                        in.close();
                     } catch (Throwable var19) {
                        var3.addSuppressed(var19);
                     }
                  } else {
                     in.close();
                  }
               }

            }

         } catch (IOException var22) {
            throw new AssertionError("Unexpected error", var22);
         }
      }
   }

   private boolean canBeCastToInt(long l) {
      return (long)((int)l) == l;
   }

   public String toString(Duration duration) {
      return duration == null?"":duration.toString();
   }

   public Class<Duration> getType() {
      return Duration.class;
   }
}
