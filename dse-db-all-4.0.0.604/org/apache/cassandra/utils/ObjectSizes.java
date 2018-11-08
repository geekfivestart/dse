package org.apache.cassandra.utils;

import java.nio.ByteBuffer;
import org.github.jamm.MemoryLayoutSpecification;
import org.github.jamm.MemoryMeter;
import org.github.jamm.MemoryMeter.Guess;

public class ObjectSizes {
   private static final MemoryMeter meter;
   private static final long BUFFER_EMPTY_SIZE;
   private static final long STRING_EMPTY_SIZE;

   public ObjectSizes() {
   }

   public static long sizeOfArray(byte[] bytes) {
      return sizeOfArray(bytes.length, 1L);
   }

   public static long sizeOfArray(long[] longs) {
      return sizeOfArray(longs.length, 8L);
   }

   public static long sizeOfArray(int[] ints) {
      return sizeOfArray(ints.length, 4L);
   }

   public static long sizeOfReferenceArray(int length) {
      return sizeOfArray(length, (long)MemoryLayoutSpecification.SPEC.getReferenceSize());
   }

   public static long sizeOfArray(Object[] objects) {
      return sizeOfReferenceArray(objects.length);
   }

   private static long sizeOfArray(int length, long elementSize) {
      return MemoryLayoutSpecification.sizeOfArray(length, elementSize);
   }

   public static long sizeOnHeapOf(ByteBuffer[] array) {
      long allElementsSize = 0L;

      for(int i = 0; i < array.length; ++i) {
         if(array[i] != null) {
            allElementsSize += sizeOnHeapOf(array[i]);
         }
      }

      return allElementsSize + sizeOfArray((Object[])array);
   }

   public static long sizeOnHeapExcludingData(ByteBuffer[] array) {
      return BUFFER_EMPTY_SIZE * (long)array.length + sizeOfArray((Object[])array);
   }

   public static long sizeOnHeapOf(ByteBuffer buffer) {
      return buffer.isDirect()?BUFFER_EMPTY_SIZE:(buffer.capacity() > buffer.remaining()?(long)buffer.remaining():BUFFER_EMPTY_SIZE + sizeOfArray(buffer.capacity(), 1L));
   }

   public static long sizeOnHeapExcludingData(ByteBuffer buffer) {
      return BUFFER_EMPTY_SIZE;
   }

   public static long sizeOf(String str) {
      return STRING_EMPTY_SIZE + sizeOfArray(str.length(), 2L);
   }

   public static long measureDeep(Object pojo) {
      return meter.measureDeep(pojo);
   }

   public static long measure(Object pojo) {
      return meter.measure(pojo);
   }

   static {
      meter = (new MemoryMeter()).omitSharedBufferOverhead().withGuessing(Guess.FALLBACK_UNSAFE).ignoreKnownSingletons();
      BUFFER_EMPTY_SIZE = measure(ByteBufferUtil.EMPTY_BYTE_BUFFER);
      STRING_EMPTY_SIZE = measure("");
   }
}
