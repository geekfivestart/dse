package org.apache.cassandra.hadoop;

import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapreduce.StatusReporter;

public class ReporterWrapper extends StatusReporter implements Reporter {
   private Reporter wrappedReporter;

   public ReporterWrapper(Reporter reporter) {
      this.wrappedReporter = reporter;
   }

   public Counter getCounter(Enum<?> anEnum) {
      return this.wrappedReporter.getCounter(anEnum);
   }

   public Counter getCounter(String s, String s1) {
      return this.wrappedReporter.getCounter(s, s1);
   }

   public void incrCounter(Enum<?> anEnum, long l) {
      this.wrappedReporter.incrCounter(anEnum, l);
   }

   public void incrCounter(String s, String s1, long l) {
      this.wrappedReporter.incrCounter(s, s1, l);
   }

   public InputSplit getInputSplit() throws UnsupportedOperationException {
      return this.wrappedReporter.getInputSplit();
   }

   public void progress() {
      this.wrappedReporter.progress();
   }

   public float getProgress() {
      throw new UnsupportedOperationException();
   }

   public void setStatus(String s) {
      this.wrappedReporter.setStatus(s);
   }
}
