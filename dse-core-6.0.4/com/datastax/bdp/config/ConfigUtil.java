package com.datastax.bdp.config;

import com.google.common.base.Optional;
import com.google.common.collect.Sets;
import java.io.File;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Iterator;
import java.util.Set;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.configuration.ConfigurationRuntimeException;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigUtil {
   private static final Logger logger = LoggerFactory.getLogger(ConfigUtil.class);

   public ConfigUtil() {
   }

   public static class MemoryParamResolver extends ConfigUtil.ParamResolver<Long> {
      private Long lowerBound;
      private Long upperBound;

      public MemoryParamResolver(String name) {
         super(name);
      }

      public MemoryParamResolver(String name, Long defaultValue) {
         super(name, defaultValue);
      }

      public ConfigUtil.MemoryParamResolver withLowerBound(long lowerBound) {
         this.lowerBound = Long.valueOf(lowerBound);
         return this;
      }

      public ConfigUtil.MemoryParamResolver withUpperBound(long upperBound) {
         this.upperBound = Long.valueOf(upperBound);
         return this;
      }

      protected boolean isNotEmpty(Object rawParam) {
         return StringUtils.isNotEmpty(StringUtils.stripToNull((String)rawParam));
      }

      protected Pair<Long, Boolean> convert(Object rawParam) throws ConfigurationException {
         try {
            long shifter = 0L;
            String str = ((String)rawParam).toLowerCase().trim();
            String var7 = str.substring(Math.max(0, str.length() - 1));
            byte var8 = -1;
            switch(var7.hashCode()) {
            case 103:
               if(var7.equals("g")) {
                  var8 = 1;
               }
               break;
            case 107:
               if(var7.equals("k")) {
                  var8 = 3;
               }
               break;
            case 109:
               if(var7.equals("m")) {
                  var8 = 2;
               }
               break;
            case 116:
               if(var7.equals("t")) {
                  var8 = 0;
               }
            }

            long value;
            switch(var8) {
            case 0:
               shifter += 10L;
            case 1:
               shifter += 10L;
            case 2:
               shifter += 10L;
            case 3:
               shifter += 10L;
               value = (new BigDecimal(str.substring(0, str.length() - 1))).multiply(new BigDecimal(1L << (int)shifter)).setScale(0, RoundingMode.FLOOR).longValueExact();
               break;
            default:
               value = Long.parseLong(str);
            }

            return Pair.of(Long.valueOf(value), Boolean.valueOf(false));
         } catch (Exception var9) {
            return (Pair)this.fail(String.format("%s is not a valid number or amount of memory specification (%s).", new Object[]{rawParam, var9.getMessage()}));
         }
      }

      protected Pair<Long, Boolean> validateOrGetDefault(Pair<Long, Boolean> value) throws ConfigurationException {
         value = this.validateLowerBound(value);
         value = this.validateUpperBound(value);
         return value;
      }

      private Pair<Long, Boolean> validateUpperBound(Pair<Long, Boolean> value) throws ConfigurationException {
         return this.upperBound != null && value.getLeft() != null && !((Boolean)value.getRight()).booleanValue() && ((Long)value.getLeft()).longValue() > this.upperBound.longValue()?(Pair)this.fail(String.format("%d is greater than %d", new Object[]{value.getLeft(), this.upperBound})):value;
      }

      private Pair<Long, Boolean> validateLowerBound(Pair<Long, Boolean> value) throws ConfigurationException {
         return this.lowerBound != null && value.getLeft() != null && !((Boolean)value.getRight()).booleanValue() && ((Long)value.getLeft()).longValue() < this.lowerBound.longValue()?(Pair)this.fail(String.format("%d is lower than %d", new Object[]{value.getLeft(), this.lowerBound})):value;
      }
   }

   public static class SetParamResolver extends ConfigUtil.ParamResolver<Set<String>> {
      private Set<String> allowedValues;

      public SetParamResolver(String name, Set<String> defaultValue) {
         super(name, defaultValue);
      }

      public ConfigUtil.SetParamResolver withAllowedValues(String... allowedValues) {
         this.allowedValues = Sets.newHashSet(allowedValues);
         return this;
      }

      protected boolean isNotEmpty(Object rawParam) {
         return rawParam != null?!((Set)rawParam).isEmpty():false;
      }

      protected Pair<Set<String>, Boolean> convert(Object rawParam) throws ConfigurationException {
         return rawParam instanceof Set?Pair.of((Set)rawParam, Boolean.valueOf(false)):(Pair)this.fail("A set must have a type of set");
      }

      protected Pair<Set<String>, Boolean> validateOrGetDefault(Pair<Set<String>, Boolean> value) throws ConfigurationException {
         value = this.validateValue(value);
         return value;
      }

      private Pair<Set<String>, Boolean> validateValue(Pair<Set<String>, Boolean> value) throws ConfigurationException {
         if(this.allowedValues != null && !((Boolean)value.getRight()).booleanValue()) {
            if(value.getLeft() == null) {
               return value;
            }

            Iterator var2 = ((Set)value.getLeft()).iterator();

            while(var2.hasNext()) {
               String setValue = (String)var2.next();
               if(!this.allowedValues.contains(setValue)) {
                  return (Pair)this.fail(String.format("%s does not belong to the set of valid values {%s}", new Object[]{setValue, StringUtils.join(this.allowedValues, ", ")}));
               }
            }
         }

         return value;
      }
   }

   public static class FileParamResolver extends ConfigUtil.ParamResolver<File> {
      public FileParamResolver(String name, File defaultValue) {
         super(name, defaultValue);
      }

      protected boolean isNotEmpty(Object rawParam) {
         return StringUtils.isNotEmpty(StringUtils.stripToNull((String)rawParam));
      }

      protected Pair<File, Boolean> convert(Object rawParam) {
         return rawParam instanceof String?Pair.of(new File((String)rawParam), Boolean.valueOf(false)):(Pair)this.fail("A file type must have a configuration type of string");
      }

      protected Pair<File, Boolean> validateOrGetDefault(Pair<File, Boolean> value) {
         value = this.validateValue(value);
         return value;
      }

      private Pair<File, Boolean> validateValue(Pair<File, Boolean> value) {
         return value;
      }
   }

   public static class StringParamResolver extends ConfigUtil.ParamResolver<String> {
      private String[] allowedValues;

      public StringParamResolver(String name) {
         super(name);
      }

      public StringParamResolver(String name, String defaultValue) {
         super(name, defaultValue);
      }

      public ConfigUtil.StringParamResolver withAllowedValues(String... allowedValues) {
         this.allowedValues = allowedValues;
         return this;
      }

      public ConfigUtil.StringParamResolver withAllowedValues(Set<String> allowedValues) {
         this.allowedValues = (String[])allowedValues.toArray(new String[allowedValues.size()]);
         return this;
      }

      protected boolean isNotEmpty(Object rawParam) {
         return StringUtils.isNotEmpty(StringUtils.stripToNull((String)rawParam));
      }

      protected Pair<String, Boolean> convert(Object rawParam) {
         return Pair.of(((String)rawParam).trim(), Boolean.valueOf(false));
      }

      protected Pair<String, Boolean> validateOrGetDefault(Pair<String, Boolean> value) {
         value = this.validateValue(value);
         return value;
      }

      private Pair<String, Boolean> validateValue(Pair<String, Boolean> value) {
         if(this.allowedValues != null && !((Boolean)value.getRight()).booleanValue()) {
            String[] var2 = this.allowedValues;
            int var3 = var2.length;

            for(int var4 = 0; var4 < var3; ++var4) {
               String allowedValue = var2[var4];
               if(allowedValue == null && value.getLeft() == null) {
                  return value;
               }

               if(allowedValue != null && allowedValue.equalsIgnoreCase((String)value.getLeft())) {
                  return Pair.of(allowedValue, Boolean.valueOf(false));
               }
            }

            return (Pair)this.fail(String.format("%s does not belong to the set of valid values {%s}", new Object[]{value.getLeft(), StringUtils.join(this.allowedValues, ", ")}));
         } else {
            return value;
         }
      }
   }

   public static class DoubleParamResolver extends ConfigUtil.ParamResolver<Double> {
      private Double lowerBound;
      private Double upperBound;

      public DoubleParamResolver(String name) {
         super(name);
      }

      public DoubleParamResolver(String name, Double defaultValue) {
         super(name, defaultValue);
      }

      public ConfigUtil.DoubleParamResolver withLowerBound(double lowerBound) {
         this.lowerBound = Double.valueOf(lowerBound);
         return this;
      }

      public ConfigUtil.DoubleParamResolver withUpperBound(double upperBound) {
         this.upperBound = Double.valueOf(upperBound);
         return this;
      }

      protected boolean isNotEmpty(Object rawParam) {
         return StringUtils.isNotEmpty(StringUtils.stripToNull((String)rawParam));
      }

      protected Pair<Double, Boolean> convert(Object rawParam) {
         try {
            return Pair.of(new Double(((String)rawParam).trim()), Boolean.valueOf(false));
         } catch (Exception var3) {
            return (Pair)this.fail(String.format("%s is not a valid number.", new Object[]{rawParam}));
         }
      }

      protected Pair<Double, Boolean> validateOrGetDefault(Pair<Double, Boolean> value) {
         value = this.validateLowerBound(value);
         value = this.validateUpperBound(value);
         return value;
      }

      private Pair<Double, Boolean> validateUpperBound(Pair<Double, Boolean> value) {
         return this.upperBound != null && value.getLeft() != null && !((Boolean)value.getRight()).booleanValue() && ((Double)value.getLeft()).doubleValue() > this.upperBound.doubleValue()?(Pair)this.fail(String.format("%f is greater than %f", new Object[]{value.getLeft(), this.upperBound})):value;
      }

      private Pair<Double, Boolean> validateLowerBound(Pair<Double, Boolean> value) {
         return this.lowerBound != null && value.getLeft() != null && !((Boolean)value.getRight()).booleanValue() && ((Double)value.getLeft()).doubleValue() < this.lowerBound.doubleValue()?(Pair)this.fail(String.format("%f is lower than %f", new Object[]{value.getLeft(), this.lowerBound})):value;
      }
   }

   public static class IntParamResolver extends ConfigUtil.ParamResolver<Integer> {
      private Integer lowerBound;
      private Integer upperBound;

      public IntParamResolver(String name) {
         super(name);
      }

      public IntParamResolver(String name, Integer defaultValue) {
         super(name, defaultValue);
      }

      public ConfigUtil.IntParamResolver withLowerBound(int lowerBound) {
         this.lowerBound = Integer.valueOf(lowerBound);
         return this;
      }

      public ConfigUtil.IntParamResolver withUpperBound(int upperBound) {
         this.upperBound = Integer.valueOf(upperBound);
         return this;
      }

      protected boolean isNotEmpty(Object rawParam) {
         return StringUtils.isNotEmpty(StringUtils.stripToNull((String)rawParam));
      }

      protected Pair<Integer, Boolean> convert(Object rawParam) {
         try {
            return Pair.of(new Integer(((String)rawParam).trim()), Boolean.valueOf(false));
         } catch (Exception var3) {
            return (Pair)this.fail(String.format("%s is not a valid number.", new Object[]{rawParam}));
         }
      }

      protected Pair<Integer, Boolean> validateOrGetDefault(Pair<Integer, Boolean> value) {
         value = this.validateLowerBound(value);
         value = this.validateUpperBound(value);
         return value;
      }

      private Pair<Integer, Boolean> validateUpperBound(Pair<Integer, Boolean> value) {
         return this.upperBound != null && value.getLeft() != null && !((Boolean)value.getRight()).booleanValue() && ((Integer)value.getLeft()).intValue() > this.upperBound.intValue()?(Pair)this.fail(String.format("%d is greater than %d", new Object[]{value.getLeft(), this.upperBound})):value;
      }

      private Pair<Integer, Boolean> validateLowerBound(Pair<Integer, Boolean> value) {
         return this.lowerBound != null && value.getLeft() != null && !((Boolean)value.getRight()).booleanValue() && ((Integer)value.getLeft()).intValue() < this.lowerBound.intValue()?(Pair)this.fail(String.format("%d is lower than %d", new Object[]{value.getLeft(), this.lowerBound})):value;
      }
   }

   public static class BooleanParamResolver extends ConfigUtil.ParamResolver<Boolean> {
      public BooleanParamResolver(String name) {
         super(name);
      }

      public BooleanParamResolver(String name, Boolean defaultValue) {
         super(name, defaultValue);
      }

      protected boolean isNotEmpty(Object rawParam) {
         return rawParam != null;
      }

      protected Pair<Boolean, Boolean> convert(Object rawParam) {
         return Pair.of((Boolean)rawParam, Boolean.valueOf(false));
      }

      protected Pair<Boolean, Boolean> validateOrGetDefault(Pair<Boolean, Boolean> value) {
         return value;
      }
   }

   public abstract static class ParamResolver<ParamType> {
      private Object rawParam;
      private Optional<ParamType> defaultValue;
      private boolean wasReported;
      protected String name;
      private boolean usedDuringInitialization = false;
      private volatile boolean configurationLoadComplete = false;

      public ParamResolver(String name) {
         this.name = name;
         this.defaultValue = null;
      }

      public ParamResolver(String name, ParamType defaultValue) {
         this.name = name;
         this.defaultValue = Optional.fromNullable(defaultValue);
      }

      public ConfigUtil.ParamResolver<ParamType> withRawParam(Object rawParam) {
         this.rawParam = rawParam;
         return this;
      }

      public ConfigUtil.ParamResolver<ParamType> withDefaultValue(ParamType defaultValue) {
         this.defaultValue = Optional.fromNullable(defaultValue);
         return this;
      }

      private Pair<ParamType, Boolean> initialize() {
         if(this.isNotEmpty(this.rawParam)) {
            return this.convert(this.rawParam);
         } else {
            ParamType result = this.getDefaultIfPresent();
            return Pair.of(result, Boolean.valueOf(true));
         }
      }

      public ConfigUtil.ParamResolver<ParamType> enable() {
         this.configurationLoadComplete = true;
         return this;
      }

      protected final ParamType getDefaultIfPresent() {
         if(this.defaultValue != null) {
            return this.defaultValue.orNull();
         } else {
            String msg;
            if(this.name != null) {
               msg = String.format("%s: %s is not valid or missing and there is not default value for it", new Object[]{this.name, this.rawParam});
            } else {
               msg = String.format("Parameter value %s is invalid or missing and there is not default value for it", new Object[]{this.rawParam});
            }

            throw new ConfigurationRuntimeException(msg);
         }
      }

      protected final <T> T fail(String msg) {
         String mainMessage = String.format("Parameter %s is invalid: %s", new Object[]{this.name != null?this.name:"", msg});
         throw new ConfigurationRuntimeException(mainMessage);
      }

      public void check() throws ConfigurationException {
         Pair<ParamType, Boolean> result = this.initialize();
         this.validateOrGetDefault(result);
      }

      public ConfigUtil.ParamResolver<ParamType> setUsedDuringInitialization(boolean usedDuringInitialization) {
         this.usedDuringInitialization = usedDuringInitialization;
         return this;
      }

      public final ParamType get() {
         if(!this.configurationLoadComplete && !this.usedDuringInitialization) {
            throw new ConfigurationRuntimeException("Configuration has not finished loading");
         } else {
            Pair<ParamType, Boolean> result = this.initialize();
            Pair<ParamType, Boolean> validatedResult = this.validateOrGetDefault(result);
            if(this.name != null && ((Boolean)validatedResult.getRight()).booleanValue() && !this.wasReported) {
               ConfigUtil.logger.debug(String.format("%s is missing; defaulting to %s", new Object[]{this.name, String.valueOf(result.getLeft())}));
            }

            ParamType value = validatedResult.getLeft();
            if(this.name != null && !this.wasReported) {
               ConfigUtil.logger.debug(String.format("Resolved configuration parameter %s = %s", new Object[]{this.name, String.valueOf(value)}));
            }

            this.wasReported = true;
            return value;
         }
      }

      protected abstract boolean isNotEmpty(Object var1);

      protected abstract Pair<ParamType, Boolean> convert(Object var1);

      protected abstract Pair<ParamType, Boolean> validateOrGetDefault(Pair<ParamType, Boolean> var1);
   }
}
