package org.apache.cassandra.utils;

public class BloomCalculations {
   private static final int minBuckets = 2;
   private static final int minK = 1;
   private static final int EXCESS = 20;
   static final double[][] probs = new double[][]{{1.0D}, {1.0D, 1.0D}, {1.0D, 0.393D, 0.4D}, {1.0D, 0.283D, 0.237D, 0.253D}, {1.0D, 0.221D, 0.155D, 0.147D, 0.16D}, {1.0D, 0.181D, 0.109D, 0.092D, 0.092D, 0.101D}, {1.0D, 0.154D, 0.0804D, 0.0609D, 0.0561D, 0.0578D, 0.0638D}, {1.0D, 0.133D, 0.0618D, 0.0423D, 0.0359D, 0.0347D, 0.0364D}, {1.0D, 0.118D, 0.0489D, 0.0306D, 0.024D, 0.0217D, 0.0216D, 0.0229D}, {1.0D, 0.105D, 0.0397D, 0.0228D, 0.0166D, 0.0141D, 0.0133D, 0.0135D, 0.0145D}, {1.0D, 0.0952D, 0.0329D, 0.0174D, 0.0118D, 0.00943D, 0.00844D, 0.00819D, 0.00846D}, {1.0D, 0.0869D, 0.0276D, 0.0136D, 0.00864D, 0.0065D, 0.00552D, 0.00513D, 0.00509D}, {1.0D, 0.08D, 0.0236D, 0.0108D, 0.00646D, 0.00459D, 0.00371D, 0.00329D, 0.00314D}, {1.0D, 0.074D, 0.0203D, 0.00875D, 0.00492D, 0.00332D, 0.00255D, 0.00217D, 0.00199D, 0.00194D}, {1.0D, 0.0689D, 0.0177D, 0.00718D, 0.00381D, 0.00244D, 0.00179D, 0.00146D, 0.00129D, 0.00121D, 0.0012D}, {1.0D, 0.0645D, 0.0156D, 0.00596D, 0.003D, 0.00183D, 0.00128D, 0.001D, 8.52E-4D, 7.75E-4D, 7.44E-4D}, {1.0D, 0.0606D, 0.0138D, 0.005D, 0.00239D, 0.00139D, 9.35E-4D, 7.02E-4D, 5.74E-4D, 5.05E-4D, 4.7E-4D, 4.59E-4D}, {1.0D, 0.0571D, 0.0123D, 0.00423D, 0.00193D, 0.00107D, 6.92E-4D, 4.99E-4D, 3.94E-4D, 3.35E-4D, 3.02E-4D, 2.87E-4D, 2.84E-4D}, {1.0D, 0.054D, 0.0111D, 0.00362D, 0.00158D, 8.39E-4D, 5.19E-4D, 3.6E-4D, 2.75E-4D, 2.26E-4D, 1.98E-4D, 1.83E-4D, 1.76E-4D}, {1.0D, 0.0513D, 0.00998D, 0.00312D, 0.0013D, 6.63E-4D, 3.94E-4D, 2.64E-4D, 1.94E-4D, 1.55E-4D, 1.32E-4D, 1.18E-4D, 1.11E-4D, 1.09E-4D}, {1.0D, 0.0488D, 0.00906D, 0.0027D, 0.00108D, 5.3E-4D, 3.03E-4D, 1.96E-4D, 1.4E-4D, 1.08E-4D, 8.89E-5D, 7.77E-5D, 7.12E-5D, 6.79E-5D, 6.71E-5D}};
   private static final int[] optKPerBuckets;

   public BloomCalculations() {
   }

   public static BloomCalculations.BloomSpecification computeBloomSpec(int bucketsPerElement) {
      assert bucketsPerElement >= 1;

      assert bucketsPerElement <= probs.length - 1;

      return new BloomCalculations.BloomSpecification(optKPerBuckets[bucketsPerElement], bucketsPerElement);
   }

   public static BloomCalculations.BloomSpecification computeBloomSpec(int maxBucketsPerElement, double maxFalsePosProb) {
      assert maxBucketsPerElement >= 1;

      assert maxBucketsPerElement <= probs.length - 1;

      int maxK = probs[maxBucketsPerElement].length - 1;
      if(maxFalsePosProb >= probs[2][1]) {
         return new BloomCalculations.BloomSpecification(2, optKPerBuckets[2]);
      } else if(maxFalsePosProb < probs[maxBucketsPerElement][maxK]) {
         throw new UnsupportedOperationException(String.format("Unable to satisfy %s with %s buckets per element", new Object[]{Double.valueOf(maxFalsePosProb), Integer.valueOf(maxBucketsPerElement)}));
      } else {
         int bucketsPerElement = 2;

         int K;
         for(K = optKPerBuckets[2]; probs[bucketsPerElement][K] > maxFalsePosProb; K = optKPerBuckets[bucketsPerElement]) {
            ++bucketsPerElement;
         }

         while(probs[bucketsPerElement][K - 1] <= maxFalsePosProb) {
            --K;
         }

         return new BloomCalculations.BloomSpecification(K, bucketsPerElement);
      }
   }

   public static int maxBucketsPerElement(long numElements) {
      numElements = Math.max(1L, numElements);
      double v = 9.223372036854776E18D / (double)numElements;
      if(v < 1.0D) {
         throw new UnsupportedOperationException("Cannot compute probabilities for " + numElements + " elements.");
      } else {
         return Math.min(probs.length - 1, (int)v);
      }
   }

   public static double minSupportedBloomFilterFpChance() {
      int maxBuckets = probs.length - 1;
      int maxK = probs[maxBuckets].length - 1;
      return probs[maxBuckets][maxK];
   }

   static {
      optKPerBuckets = new int[probs.length];

      for(int i = 0; i < probs.length; ++i) {
         double min = 1.7976931348623157E308D;
         double[] prob = probs[i];

         for(int j = 0; j < prob.length; ++j) {
            if(prob[j] < min) {
               min = prob[j];
               optKPerBuckets[i] = Math.max(1, j);
            }
         }
      }

   }

   public static class BloomSpecification {
      final int K;
      final int bucketsPerElement;

      public BloomSpecification(int k, int bucketsPerElement) {
         this.K = k;
         this.bucketsPerElement = bucketsPerElement;
      }

      public String toString() {
         return String.format("BloomSpecification(K=%d, bucketsPerElement=%d)", new Object[]{Integer.valueOf(this.K), Integer.valueOf(this.bucketsPerElement)});
      }
   }
}
