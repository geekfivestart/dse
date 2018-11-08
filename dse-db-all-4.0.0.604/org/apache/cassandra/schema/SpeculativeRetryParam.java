package org.apache.cassandra.schema;

import java.text.DecimalFormat;
import java.util.Locale;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.cassandra.exceptions.ConfigurationException;

public final class SpeculativeRetryParam {
   public static final SpeculativeRetryParam NONE = none();
   public static final SpeculativeRetryParam ALWAYS = always();
   public static final SpeculativeRetryParam DEFAULT = percentile(99.0D);
   private final SpeculativeRetryParam.Kind kind;
   private final double value;
   private final double threshold;

   private SpeculativeRetryParam(SpeculativeRetryParam.Kind kind, double value) {
      this.kind = kind;
      this.value = value;
      if(kind == SpeculativeRetryParam.Kind.PERCENTILE) {
         this.threshold = value / 100.0D;
      } else if(kind == SpeculativeRetryParam.Kind.CUSTOM) {
         this.threshold = (double)TimeUnit.MILLISECONDS.toNanos((long)value);
      } else {
         this.threshold = value;
      }

   }

   public SpeculativeRetryParam.Kind kind() {
      return this.kind;
   }

   public double threshold() {
      return this.threshold;
   }

   public static SpeculativeRetryParam none() {
      return new SpeculativeRetryParam(SpeculativeRetryParam.Kind.NONE, 0.0D);
   }

   public static SpeculativeRetryParam always() {
      return new SpeculativeRetryParam(SpeculativeRetryParam.Kind.ALWAYS, 0.0D);
   }

   public static SpeculativeRetryParam custom(double value) {
      return new SpeculativeRetryParam(SpeculativeRetryParam.Kind.CUSTOM, value);
   }

   public static SpeculativeRetryParam percentile(double value) {
      return new SpeculativeRetryParam(SpeculativeRetryParam.Kind.PERCENTILE, value);
   }

   public static SpeculativeRetryParam fromString(String value) {
      String upperCaseValue = value.toUpperCase(Locale.ENGLISH);
      if(upperCaseValue.endsWith("MS")) {
         try {
            return custom(Double.parseDouble(value.substring(0, value.length() - "ms".length())));
         } catch (IllegalArgumentException var6) {
            throw new ConfigurationException(String.format("Invalid value %s for option '%s'", new Object[]{value, TableParams.Option.SPECULATIVE_RETRY}));
         }
      } else if(!upperCaseValue.endsWith(SpeculativeRetryParam.Kind.PERCENTILE.toString()) && !upperCaseValue.endsWith("P")) {
         if(upperCaseValue.equals(SpeculativeRetryParam.Kind.NONE.toString())) {
            return NONE;
         } else if(upperCaseValue.equals(SpeculativeRetryParam.Kind.ALWAYS.toString())) {
            return ALWAYS;
         } else {
            throw new ConfigurationException(String.format("Invalid value %s for option '%s'", new Object[]{value, TableParams.Option.SPECULATIVE_RETRY}));
         }
      } else {
         int suffixLength = upperCaseValue.endsWith("P")?1:SpeculativeRetryParam.Kind.PERCENTILE.toString().length();

         double threshold;
         try {
            threshold = Double.parseDouble(value.substring(0, value.length() - suffixLength));
         } catch (IllegalArgumentException var7) {
            throw new ConfigurationException(String.format("Invalid value %s for option '%s'", new Object[]{value, TableParams.Option.SPECULATIVE_RETRY}));
         }

         if(threshold >= 0.0D && threshold <= 100.0D) {
            return percentile(threshold);
         } else {
            throw new ConfigurationException(String.format("Invalid value %s for PERCENTILE option '%s': must be between 0.0 and 100.0", new Object[]{value, TableParams.Option.SPECULATIVE_RETRY}));
         }
      }
   }

   public boolean equals(Object o) {
      if(!(o instanceof SpeculativeRetryParam)) {
         return false;
      } else {
         SpeculativeRetryParam srp = (SpeculativeRetryParam)o;
         return this.kind == srp.kind && this.threshold == srp.threshold;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.kind, Double.valueOf(this.threshold)});
   }

   public String toString() {
      switch(null.$SwitchMap$org$apache$cassandra$schema$SpeculativeRetryParam$Kind[this.kind.ordinal()]) {
      case 1:
         return String.format("%sms", new Object[]{Double.valueOf(this.value)});
      case 2:
         return String.format("%sPERCENTILE", new Object[]{(new DecimalFormat("#.#####")).format(this.value)});
      default:
         return this.kind.toString();
      }
   }

   public static enum Kind {
      NONE,
      CUSTOM,
      PERCENTILE,
      ALWAYS;

      private Kind() {
      }
   }
}
