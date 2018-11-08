package org.apache.cassandra.hadoop;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.StatusReporter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskInputOutputContext;

public class HadoopCompat {
   private static final boolean useV21;
   private static final Constructor<?> JOB_CONTEXT_CONSTRUCTOR;
   private static final Constructor<?> TASK_CONTEXT_CONSTRUCTOR;
   private static final Constructor<?> MAP_CONTEXT_CONSTRUCTOR;
   private static final Constructor<?> GENERIC_COUNTER_CONSTRUCTOR;
   private static final Field READER_FIELD;
   private static final Field WRITER_FIELD;
   private static final Method GET_CONFIGURATION_METHOD;
   private static final Method SET_STATUS_METHOD;
   private static final Method GET_COUNTER_METHOD;
   private static final Method INCREMENT_COUNTER_METHOD;
   private static final Method GET_TASK_ATTEMPT_ID;
   private static final Method PROGRESS_METHOD;

   public HadoopCompat() {
   }

   public static boolean isVersion2x() {
      return useV21;
   }

   private static Object newInstance(Constructor<?> constructor, Object... args) {
      try {
         return constructor.newInstance(args);
      } catch (InstantiationException var3) {
         throw new IllegalArgumentException("Can't instantiate " + constructor, var3);
      } catch (IllegalAccessException var4) {
         throw new IllegalArgumentException("Can't instantiate " + constructor, var4);
      } catch (InvocationTargetException var5) {
         throw new IllegalArgumentException("Can't instantiate " + constructor, var5);
      }
   }

   public static JobContext newJobContext(Configuration conf, JobID jobId) {
      return (JobContext)newInstance(JOB_CONTEXT_CONSTRUCTOR, new Object[]{conf, jobId});
   }

   public static TaskAttemptContext newTaskAttemptContext(Configuration conf, TaskAttemptID taskAttemptId) {
      return (TaskAttemptContext)newInstance(TASK_CONTEXT_CONSTRUCTOR, new Object[]{conf, taskAttemptId});
   }

   public static MapContext newMapContext(Configuration conf, TaskAttemptID taskAttemptID, RecordReader recordReader, RecordWriter recordWriter, OutputCommitter outputCommitter, StatusReporter statusReporter, InputSplit inputSplit) {
      return (MapContext)newInstance(MAP_CONTEXT_CONSTRUCTOR, new Object[]{conf, taskAttemptID, recordReader, recordWriter, outputCommitter, statusReporter, inputSplit});
   }

   public static Counter newGenericCounter(String name, String displayName, long value) {
      try {
         return (Counter)GENERIC_COUNTER_CONSTRUCTOR.newInstance(new Object[]{name, displayName, Long.valueOf(value)});
      } catch (IllegalAccessException | InvocationTargetException | InstantiationException var5) {
         throw new IllegalArgumentException("Can't instantiate Counter", var5);
      }
   }

   private static Object invoke(Method method, Object obj, Object... args) {
      try {
         return method.invoke(obj, args);
      } catch (InvocationTargetException | IllegalAccessException var4) {
         throw new IllegalArgumentException("Can't invoke method " + method.getName(), var4);
      }
   }

   public static Configuration getConfiguration(JobContext context) {
      return (Configuration)invoke(GET_CONFIGURATION_METHOD, context, new Object[0]);
   }

   public static void setStatus(TaskAttemptContext context, String status) {
      invoke(SET_STATUS_METHOD, context, new Object[]{status});
   }

   public static TaskAttemptID getTaskAttemptID(TaskAttemptContext taskContext) {
      return (TaskAttemptID)invoke(GET_TASK_ATTEMPT_ID, taskContext, new Object[0]);
   }

   public static Counter getCounter(TaskInputOutputContext context, String groupName, String counterName) {
      return (Counter)invoke(GET_COUNTER_METHOD, context, new Object[]{groupName, counterName});
   }

   public static void progress(TaskAttemptContext context) {
      invoke(PROGRESS_METHOD, context, new Object[0]);
   }

   public static void incrementCounter(Counter counter, long increment) {
      invoke(INCREMENT_COUNTER_METHOD, counter, new Object[]{Long.valueOf(increment)});
   }

   static {
      boolean v21 = true;
      String var1 = "org.apache.hadoop.mapreduce";

      try {
         Class.forName("org.apache.hadoop.mapreduce.task.JobContextImpl");
      } catch (ClassNotFoundException var15) {
         v21 = false;
      }

      useV21 = v21;

      Class jobContextCls;
      Class taskContextCls;
      Class taskIOContextCls;
      Class mapContextCls;
      Class genericCounterCls;
      try {
         if(v21) {
            jobContextCls = Class.forName("org.apache.hadoop.mapreduce.task.JobContextImpl");
            taskContextCls = Class.forName("org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl");
            taskIOContextCls = Class.forName("org.apache.hadoop.mapreduce.task.TaskInputOutputContextImpl");
            mapContextCls = Class.forName("org.apache.hadoop.mapreduce.task.MapContextImpl");
            genericCounterCls = Class.forName("org.apache.hadoop.mapreduce.counters.GenericCounter");
         } else {
            jobContextCls = Class.forName("org.apache.hadoop.mapreduce.JobContext");
            taskContextCls = Class.forName("org.apache.hadoop.mapreduce.TaskAttemptContext");
            taskIOContextCls = Class.forName("org.apache.hadoop.mapreduce.TaskInputOutputContext");
            mapContextCls = Class.forName("org.apache.hadoop.mapreduce.MapContext");
            genericCounterCls = Class.forName("org.apache.hadoop.mapred.Counters$Counter");
         }
      } catch (ClassNotFoundException var14) {
         throw new IllegalArgumentException("Can't find class", var14);
      }

      try {
         JOB_CONTEXT_CONSTRUCTOR = jobContextCls.getConstructor(new Class[]{Configuration.class, JobID.class});
         JOB_CONTEXT_CONSTRUCTOR.setAccessible(true);
         TASK_CONTEXT_CONSTRUCTOR = taskContextCls.getConstructor(new Class[]{Configuration.class, TaskAttemptID.class});
         TASK_CONTEXT_CONSTRUCTOR.setAccessible(true);
         GENERIC_COUNTER_CONSTRUCTOR = genericCounterCls.getDeclaredConstructor(new Class[]{String.class, String.class, Long.TYPE});
         GENERIC_COUNTER_CONSTRUCTOR.setAccessible(true);
         if(useV21) {
            MAP_CONTEXT_CONSTRUCTOR = mapContextCls.getDeclaredConstructor(new Class[]{Configuration.class, TaskAttemptID.class, RecordReader.class, RecordWriter.class, OutputCommitter.class, StatusReporter.class, InputSplit.class});

            Method get_counter;
            try {
               get_counter = Class.forName("org.apache.hadoop.mapreduce.TaskAttemptContext").getMethod("getCounter", new Class[]{String.class, String.class});
            } catch (Exception var9) {
               get_counter = Class.forName("org.apache.hadoop.mapreduce.TaskInputOutputContext").getMethod("getCounter", new Class[]{String.class, String.class});
            }

            GET_COUNTER_METHOD = get_counter;
         } else {
            MAP_CONTEXT_CONSTRUCTOR = mapContextCls.getConstructor(new Class[]{Configuration.class, TaskAttemptID.class, RecordReader.class, RecordWriter.class, OutputCommitter.class, StatusReporter.class, InputSplit.class});
            GET_COUNTER_METHOD = Class.forName("org.apache.hadoop.mapreduce.TaskInputOutputContext").getMethod("getCounter", new Class[]{String.class, String.class});
         }

         MAP_CONTEXT_CONSTRUCTOR.setAccessible(true);
         READER_FIELD = mapContextCls.getDeclaredField("reader");
         READER_FIELD.setAccessible(true);
         WRITER_FIELD = taskIOContextCls.getDeclaredField("output");
         WRITER_FIELD.setAccessible(true);
         GET_CONFIGURATION_METHOD = Class.forName("org.apache.hadoop.mapreduce.JobContext").getMethod("getConfiguration", new Class[0]);
         SET_STATUS_METHOD = Class.forName("org.apache.hadoop.mapreduce.TaskAttemptContext").getMethod("setStatus", new Class[]{String.class});
         GET_TASK_ATTEMPT_ID = Class.forName("org.apache.hadoop.mapreduce.TaskAttemptContext").getMethod("getTaskAttemptID", new Class[0]);
         INCREMENT_COUNTER_METHOD = Class.forName("org.apache.hadoop.mapreduce.Counter").getMethod("increment", new Class[]{Long.TYPE});
         PROGRESS_METHOD = Class.forName("org.apache.hadoop.mapreduce.TaskAttemptContext").getMethod("progress", new Class[0]);
      } catch (SecurityException var10) {
         throw new IllegalArgumentException("Can't run constructor ", var10);
      } catch (NoSuchMethodException var11) {
         throw new IllegalArgumentException("Can't find constructor ", var11);
      } catch (NoSuchFieldException var12) {
         throw new IllegalArgumentException("Can't find field ", var12);
      } catch (ClassNotFoundException var13) {
         throw new IllegalArgumentException("Can't find class", var13);
      }
   }
}
