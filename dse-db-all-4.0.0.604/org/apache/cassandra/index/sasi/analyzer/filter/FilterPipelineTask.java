package org.apache.cassandra.index.sasi.analyzer.filter;

public abstract class FilterPipelineTask<F, T> {
   private String name;
   public FilterPipelineTask<?, ?> next;

   public FilterPipelineTask() {
   }

   protected <K, V> void setLast(String name, FilterPipelineTask<K, V> last) {
      if(last == this) {
         throw new IllegalArgumentException("provided last task [" + last.name + "] cannot be set to itself");
      } else {
         if(this.next == null) {
            this.next = last;
            this.name = name;
         } else {
            this.next.setLast(name, last);
         }

      }
   }

   public abstract T process(F var1) throws Exception;

   public String getName() {
      return this.name;
   }
}
