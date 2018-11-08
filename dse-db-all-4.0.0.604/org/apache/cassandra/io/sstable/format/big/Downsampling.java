package org.apache.cassandra.io.sstable.format.big;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class Downsampling {
   public static final int BASE_SAMPLING_LEVEL = 128;
   private static final Map<Integer, List<Integer>> samplePatternCache = new HashMap();
   private static final Map<Integer, List<Integer>> originalIndexCache = new HashMap();

   public Downsampling() {
   }

   public static List<Integer> getSamplingPattern(int samplingLevel) {
      List<Integer> pattern = (List)samplePatternCache.get(Integer.valueOf(samplingLevel));
      if(pattern != null) {
         return pattern;
      } else if(samplingLevel <= 1) {
         return Arrays.asList(new Integer[]{Integer.valueOf(0)});
      } else {
         int[] odds = new int[samplingLevel / 2];
         int[] evens = new int[samplingLevel / 2];

         int i;
         for(i = 1; i < samplingLevel; i += 2) {
            odds[i / 2] = i;
         }

         for(i = 0; i < samplingLevel; i += 2) {
            evens[i / 2] = i;
         }

         List<Integer> ordering = getSamplingPattern(samplingLevel / 2);
         List<Integer> startIndices = new ArrayList(samplingLevel);
         Iterator var6 = ordering.iterator();

         Integer index;
         while(var6.hasNext()) {
            index = (Integer)var6.next();
            startIndices.add(Integer.valueOf(odds[index.intValue()]));
         }

         var6 = ordering.iterator();

         while(var6.hasNext()) {
            index = (Integer)var6.next();
            startIndices.add(Integer.valueOf(evens[index.intValue()]));
         }

         samplePatternCache.put(Integer.valueOf(samplingLevel), startIndices);
         return startIndices;
      }
   }

   public static List<Integer> getOriginalIndexes(int samplingLevel) {
      List<Integer> originalIndexes = (List)originalIndexCache.get(Integer.valueOf(samplingLevel));
      if(originalIndexes != null) {
         return originalIndexes;
      } else {
         List<Integer> pattern = getSamplingPattern(128).subList(0, 128 - samplingLevel);
         List<Integer> originalIndexes = new ArrayList(samplingLevel);

         for(int j = 0; j < 128; ++j) {
            if(!pattern.contains(Integer.valueOf(j))) {
               originalIndexes.add(Integer.valueOf(j));
            }
         }

         originalIndexCache.put(Integer.valueOf(samplingLevel), originalIndexes);
         return originalIndexes;
      }
   }

   public static int getEffectiveIndexIntervalAfterIndex(int index, int samplingLevel, int minIndexInterval) {
      assert index >= 0;

      index %= samplingLevel;
      List<Integer> originalIndexes = getOriginalIndexes(samplingLevel);
      int nextEntryOriginalIndex = index == originalIndexes.size() - 1?128:((Integer)originalIndexes.get(index + 1)).intValue();
      return (nextEntryOriginalIndex - ((Integer)originalIndexes.get(index)).intValue()) * minIndexInterval;
   }

   public static int[] getStartPoints(int currentSamplingLevel, int newSamplingLevel) {
      List<Integer> allStartPoints = getSamplingPattern(128);
      int initialRound = 128 - currentSamplingLevel;
      int numRounds = Math.abs(currentSamplingLevel - newSamplingLevel);
      int[] startPoints = new int[numRounds];

      for(int i = 0; i < numRounds; ++i) {
         int start = ((Integer)allStartPoints.get(initialRound + i)).intValue();
         int adjustment = 0;

         for(int j = 0; j < initialRound; ++j) {
            if(((Integer)allStartPoints.get(j)).intValue() < start) {
               ++adjustment;
            }
         }

         startPoints[i] = start - adjustment;
      }

      return startPoints;
   }
}
