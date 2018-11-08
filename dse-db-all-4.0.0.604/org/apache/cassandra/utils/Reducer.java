package org.apache.cassandra.utils;

public abstract class Reducer<In, Out> {
   Throwable errors = null;

   public Reducer() {
   }

   public boolean trivialReduceIsTrivial() {
      return false;
   }

   public abstract void reduce(int var1, In var2);

   public void error(Throwable error) {
      this.errors = Throwables.merge(this.errors, error);
   }

   public Throwable getErrors() {
      Throwable toReturn = this.errors;
      this.errors = null;
      return toReturn;
   }

   public abstract Out getReduced();

   public void onKeyChange() {
   }

   public void close() {
   }
}
