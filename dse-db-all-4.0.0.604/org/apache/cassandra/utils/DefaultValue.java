package org.apache.cassandra.utils;

public class DefaultValue<T> {
   private final T originalValue;
   private T currentValue;

   public DefaultValue(T value) {
      this.originalValue = value;
      this.currentValue = value;
   }

   public T value() {
      return this.currentValue;
   }

   public void set(T i) {
      this.currentValue = i;
   }

   public void reset() {
      this.currentValue = this.originalValue;
   }

   public boolean isModified() {
      return this.originalValue != this.currentValue;
   }
}
