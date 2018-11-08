package org.apache.cassandra.index.sasi.analyzer.filter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilterPipelineExecutor {
   private static final Logger logger = LoggerFactory.getLogger(FilterPipelineExecutor.class);

   public FilterPipelineExecutor() {
   }

   public static <F, T> T execute(FilterPipelineTask<F, T> task, T initialInput) {
      FilterPipelineTask<?, ?> taskPtr = task;
      Object result = initialInput;

      try {
         do {
            result = taskPtr.process(result);
            taskPtr = taskPtr.next;
         } while(taskPtr != null);

         return result;
      } catch (Exception var5) {
         logger.info("An unhandled exception to occurred while processing pipeline [{}]", task.getName(), var5);
         return null;
      }
   }
}
