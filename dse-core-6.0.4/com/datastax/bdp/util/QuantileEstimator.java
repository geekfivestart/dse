package com.datastax.bdp.util;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.SortedMap;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

public class QuantileEstimator {
   public static final int DEFAULT_M = 40;
   public static final double DEFAULT_WEIGHT = 0.08D;
   public static SortedSet<Double> DEFAULT_QUANTILES = makeQuantiles(new double[]{0.05D, 0.25D, 0.5D, 0.75D, 0.95D});
   public static final Map<Integer, Double> c1 = new TreeMap();
   public static final Map<Integer, Double> cnot1 = new TreeMap();
   private final SortedSet<Double> quantiles;
   private final float[] sn;
   private final float[] fn;
   private final float[] samples;
   private AtomicInteger pos;
   private float w;
   private boolean firstIteration;
   private int totalCount;

   public QuantileEstimator(SortedSet<Double> quantiles, double w, int m) {
      assert m > 0;

      assert w > 0.0D;

      assert quantiles != null;

      assert quantiles.contains(Double.valueOf(0.25D));

      assert quantiles.contains(Double.valueOf(0.75D));

      this.quantiles = quantiles;
      this.sn = new float[quantiles.size()];
      this.fn = new float[quantiles.size()];
      this.samples = new float[m];
      this.w = (float)w;
      this.pos = new AtomicInteger(0);
      this.firstIteration = true;
      this.totalCount = 0;
   }

   public QuantileEstimator() {
      this(DEFAULT_QUANTILES);
   }

   public QuantileEstimator(double[] quantiles) {
      this(makeQuantiles(quantiles));
   }

   public QuantileEstimator(SortedSet<Double> quantiles) {
      this(quantiles, 0.08D, recommendedM(quantiles));
   }

   public static int recommendedM(SortedSet<Double> quantiles) {
      return quantiles != null && !quantiles.isEmpty()?2 * (int)Math.max(Math.ceil(1.0D / (1.0D - ((Double)quantiles.last()).doubleValue())), Math.ceil(1.0D / ((Double)quantiles.first()).doubleValue())):40;
   }

   public static int compareByMaxQuantile(SortedMap<Double, Double> q1, SortedMap<Double, Double> q2) {
      boolean q1empty = q1 == null || q1.isEmpty();
      boolean q2empty = q2 == null || q2.isEmpty();
      return q1empty && !q2empty?-1:(!q1empty && q2empty?1:(q1empty && q2empty?0:Double.compare(((Double)q1.get(q1.lastKey())).doubleValue(), ((Double)q2.get(q2.lastKey())).doubleValue())));
   }

   public static SortedSet<Double> makeQuantiles(double[] quantiles) {
      assert quantiles != null;

      SortedSet<Double> quants = new TreeSet();
      double[] var2 = quantiles;
      int var3 = quantiles.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         double q = var2[var4];
         quants.add(Double.valueOf(q));
      }

      return quants;
   }

   public static SortedMap<Double, Double> makeOutputQuantiles(double[] quantiles, double[] values) {
      assert quantiles != null;

      assert values != null;

      assert quantiles.length == values.length;

      SortedMap<Double, Double> testQuantiles = new TreeMap();

      for(int i = 0; i < quantiles.length; ++i) {
         testQuantiles.put(Double.valueOf(quantiles[i]), Double.valueOf(values[i]));
      }

      return testQuantiles;
   }

   public String toString() {
      return String.format("QE(m=%d, w=%.2f, state: %s, buffer: %d/%d/%d, Sn: %s, Fn: %s)", new Object[]{Integer.valueOf(this.m()), Float.valueOf(this.w), this.firstIteration?"init":"update", Integer.valueOf(this.pos.get()), Integer.valueOf(this.m()), Integer.valueOf(this.totalCount), this.getQuantiles().toString(), this.getDensities().toString()});
   }

   public int m() {
      return this.samples.length;
   }

   public double w() {
      return this.totalCount < 300?Math.max(0.5D, (double)(this.w * 3.0F)):(this.totalCount < 1000?Math.max(0.25D, (double)(this.w * 2.0F)):(double)this.w);
   }

   public SortedMap<Double, Double> getQuantiles() {
      return this.getArray(this.sn);
   }

   public SortedMap<Double, Double> getDensities() {
      return this.getArray(this.fn);
   }

   private SortedMap<Double, Double> getArray(float[] it) {
      SortedMap<Double, Double> q = new TreeMap();
      if(!this.firstIteration) {
         int index = 0;
         Iterator var4 = this.quantiles.iterator();

         while(var4.hasNext()) {
            double quantile = ((Double)var4.next()).doubleValue();
            q.put(Double.valueOf(quantile), Double.valueOf(round((double)it[index++], 3)));
         }
      }

      return q;
   }

   public static double round(double value, int places) {
      long digits = Math.round(Math.log10(value) + 0.5D);
      double mult = Math.pow(10.0D, (double)(digits - (long)places));
      value /= mult;
      value = (double)Math.round(value);
      value *= mult;
      return value;
   }

   public void update(float val) {
      int position = this.pos.getAndIncrement();
      if(position < this.m()) {
         this.samples[position] = val;
         if(position + 1 == this.m()) {
            Arrays.sort(this.samples);
            if(this.firstIteration) {
               this.initialEstimates(this.m());
            } else {
               this.updateEstimates(this.w(), this.m());
            }

            this.firstIteration = false;
            this.totalCount += position;
            this.pos.set(0);
         }

      }
   }

   private void initialEstimates(int m) {
      int index = 0;
      double cs0 = this.cs0(m);

      for(Iterator var5 = this.quantiles.iterator(); var5.hasNext(); ++index) {
         double q = ((Double)var5.next()).doubleValue();
         this.sn[index] = (float)this.s0(q, m);
         this.fn[index] = (float)this.f0((double)this.sn[index], cs0, m, 1);
      }

   }

   private void updateEstimates(double weight, int m) {
      int index = 0;
      double csn = this.csn(m);

      for(Iterator var7 = this.quantiles.iterator(); var7.hasNext(); ++index) {
         double q = ((Double)var7.next()).doubleValue();
         double snm1 = (double)this.sn[index];
         double fnm1 = (double)this.fn[index];
         this.sn[index] = (float)this.sn(q, snm1, fnm1, m);
         this.fn[index] = (float)this.fn(fnm1, snm1, csn, weight, m);
      }

   }

   private double s0(double q, int m) {
      return (double)this.samples[(int)Math.round((double)(m - 1) * q)];
   }

   private double sn(double q, double snm1, double fnm1, int m) {
      int less;
      for(less = 0; less < m && (double)this.samples[less] < snm1; ++less) {
         ;
      }

      return snm1 + (double)this.w / fnm1 * (q - (double)less * 1.0D / (double)m);
   }

   private double f0(double s0, double cs0, int m, int minCount) {
      int count = 0;
      float[] var8 = this.samples;
      int var9 = var8.length;

      for(int var10 = 0; var10 < var9; ++var10) {
         float sample = var8[var10];
         if(Math.abs((double)sample - s0) < cs0) {
            ++count;
         }
      }

      count = Math.max(count, minCount);
      return (double)count / (2.0D * cs0 * (double)m);
   }

   private double fn(double fnm1, double snm1, double csnm1, double weight, int m) {
      return Math.max(5.0E-4D, (1.0D - weight) * fnm1 + weight * this.f0(snm1, Math.max(csnm1, 0.03D / fnm1), m, 0));
   }

   private double cs0(int m) {
      return this.r0(m) * this.c0(m);
   }

   private double csn(int m) {
      return this.rn() * this.cn(m);
   }

   private double r0(int m) {
      return (double)(this.samples[(int)Math.round(0.75D * (double)(m - 1))] - this.samples[(int)Math.round(0.25D * (double)(m - 1))]);
   }

   private double rn() {
      float q25 = 0.0F;
      float q75 = 0.0F;
      int i = 0;

      for(Iterator var4 = this.quantiles.iterator(); var4.hasNext(); ++i) {
         double quantile = ((Double)var4.next()).doubleValue();
         if(quantile == 0.25D) {
            q25 = this.sn[i];
         } else if(quantile == 0.75D) {
            q75 = this.sn[i];
         }
      }

      return (double)(q75 - q25);
   }

   private double c0(int m) {
      Double c1m = (Double)c1.get(Integer.valueOf(m));
      if(c1m != null) {
         return c1m.doubleValue();
      } else {
         double sum = 0.0D;

         for(int i = 1; i <= m; ++i) {
            sum += Math.pow((double)i, -0.5D);
         }

         sum /= (double)m;
         c1.put(Integer.valueOf(m), Double.valueOf(sum));
         return sum;
      }
   }

   private double cn(int m) {
      Double cnot1m = (Double)cnot1.get(Integer.valueOf(m));
      if(cnot1m != null) {
         return cnot1m.doubleValue();
      } else {
         double sum = 0.0D;

         for(int i = m + 1; i <= 2 * m; ++i) {
            sum += Math.pow((double)i, -0.5D);
         }

         sum /= (double)m;
         cnot1.put(Integer.valueOf(m), Double.valueOf(sum));
         return sum;
      }
   }
}
