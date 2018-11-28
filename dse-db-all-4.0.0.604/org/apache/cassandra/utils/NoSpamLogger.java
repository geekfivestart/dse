package org.apache.cassandra.utils;

import com.google.common.annotations.VisibleForTesting;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;

public class NoSpamLogger {
   @VisibleForTesting
   static NoSpamLogger.Clock CLOCK = new NoSpamLogger.Clock() {
      public long nanoTime() {
         return System.nanoTime();
      }
   };
   private static final NonBlockingHashMap<Logger, NoSpamLogger> wrappedLoggers = new NonBlockingHashMap();
   private final Logger wrapped;
   private final long minIntervalNanos;
   private final NonBlockingHashMap<String, NoSpamLogger.NoSpamLogStatement> lastMessage = new NonBlockingHashMap();

   @VisibleForTesting
   static void clearWrappedLoggersForTest() {
      wrappedLoggers.clear();
   }

   public static NoSpamLogger getLogger(Logger logger, long minInterval, TimeUnit unit) {
      NoSpamLogger wrapped = (NoSpamLogger)wrappedLoggers.get(logger);
      if(wrapped == null) {
         wrapped = new NoSpamLogger(logger, minInterval, unit);
         NoSpamLogger temp = (NoSpamLogger)wrappedLoggers.putIfAbsent(logger, wrapped);
         if(temp != null) {
            wrapped = temp;
         }
      }

      return wrapped;
   }

   public static boolean log(Logger logger, NoSpamLogger.Level level, long minInterval, TimeUnit unit, String message, Object... objects) {
      return log(logger, level, message, minInterval, unit, CLOCK.nanoTime(), message, objects);
   }

   public static boolean log(Logger logger, NoSpamLogger.Level level, String key, long minInterval, TimeUnit unit, String message, Object... objects) {
      return log(logger, level, key, minInterval, unit, CLOCK.nanoTime(), message, objects);
   }

   public static boolean log(Logger logger, NoSpamLogger.Level level, String key, long minInterval, TimeUnit unit, long nowNanos, String message, Object... objects) {
      NoSpamLogger wrapped = getLogger(logger, minInterval, unit);
      NoSpamLogger.NoSpamLogStatement statement = wrapped.getStatement(key, message);
      return statement.log(level, nowNanos, objects);
   }

   public static NoSpamLogger.NoSpamLogStatement getStatement(Logger logger, String message, long minInterval, TimeUnit unit) {
      NoSpamLogger wrapped = getLogger(logger, minInterval, unit);
      return wrapped.getStatement(message);
   }

   private NoSpamLogger(Logger wrapped, long minInterval, TimeUnit timeUnit) {
      this.wrapped = wrapped;
      this.minIntervalNanos = timeUnit.toNanos(minInterval);
   }

   public boolean debug(long nowNanos, String s, Object... objects) {
      return this.log(NoSpamLogger.Level.DEBUG, s, nowNanos, objects);
   }

   public boolean debug(String s, Object... objects) {
      return this.debug(CLOCK.nanoTime(), s, objects);
   }

   public boolean info(long nowNanos, String s, Object... objects) {
      return this.log(NoSpamLogger.Level.INFO, s, nowNanos, objects);
   }

   public boolean info(String s, Object... objects) {
      return this.info(CLOCK.nanoTime(), s, objects);
   }

   public boolean warn(long nowNanos, String s, Object... objects) {
      return this.log(NoSpamLogger.Level.WARN, s, nowNanos, objects);
   }

   public boolean warn(String s, Object... objects) {
      return this.warn(CLOCK.nanoTime(), s, objects);
   }

   public boolean error(long nowNanos, String s, Object... objects) {
      return this.log(NoSpamLogger.Level.ERROR, s, nowNanos, objects);
   }

   public boolean error(String s, Object... objects) {
      return this.error(CLOCK.nanoTime(), s, objects);
   }

   public boolean log(NoSpamLogger.Level l, String s, long nowNanos, Object... objects) {
      return this.getStatement(s, this.minIntervalNanos).log(l, nowNanos, objects);
   }

   public NoSpamLogger.NoSpamLogStatement getStatement(String s) {
      return this.getStatement(s, this.minIntervalNanos);
   }

   public NoSpamLogger.NoSpamLogStatement getStatement(String key, String s) {
      return this.getStatement(key, s, this.minIntervalNanos);
   }

   public NoSpamLogger.NoSpamLogStatement getStatement(String s, long minInterval, TimeUnit unit) {
      return this.getStatement(s, unit.toNanos(minInterval));
   }

   public NoSpamLogger.NoSpamLogStatement getStatement(String s, long minIntervalNanos) {
      return this.getStatement(s, s, minIntervalNanos);
   }

   public NoSpamLogger.NoSpamLogStatement getStatement(String key, String s, long minIntervalNanos) {
      NoSpamLogger.NoSpamLogStatement statement = (NoSpamLogger.NoSpamLogStatement)this.lastMessage.get(key);
      if(statement == null) {
         statement = new NoSpamLogger.NoSpamLogStatement(s, minIntervalNanos);
         NoSpamLogger.NoSpamLogStatement temp = (NoSpamLogger.NoSpamLogStatement)this.lastMessage.putIfAbsent(key, statement);
         if(temp != null) {
            statement = temp;
         }
      }

      return statement;
   }

   public class NoSpamLogStatement extends AtomicLong {
      private static final long serialVersionUID = 1L;
      private final String statement;
      private final long minIntervalNanos;

      public NoSpamLogStatement(String statement, long minIntervalNanos) {
         this.statement = statement;
         this.minIntervalNanos = minIntervalNanos;
      }

      private boolean shouldLog(long nowNanos) {
         long expected = this.get();
         return nowNanos - expected >= this.minIntervalNanos && this.compareAndSet(expected, nowNanos);
      }

      public /* varargs */ boolean log(Level l, long nowNanos, Object ... objects) {
         if (!this.shouldLog(nowNanos)) {
            return false;
         }
         switch (l) {
            case DEBUG: {
               NoSpamLogger.this.wrapped.debug(this.statement, objects);
               break;
            }
            case INFO: {
               NoSpamLogger.this.wrapped.info(this.statement, objects);
               break;
            }
            case WARN: {
               NoSpamLogger.this.wrapped.warn(this.statement, objects);
               break;
            }
            case ERROR: {
               NoSpamLogger.this.wrapped.error(this.statement, objects);
               break;
            }
            default: {
               throw new AssertionError();
            }
         }
         return true;
      }

      public boolean debug(long nowNanos, Object... objects) {
         return this.log(NoSpamLogger.Level.DEBUG, nowNanos, objects);
      }

      public boolean debug(Object... objects) {
         return this.debug(NoSpamLogger.CLOCK.nanoTime(), objects);
      }

      public boolean info(long nowNanos, Object... objects) {
         return this.log(NoSpamLogger.Level.INFO, nowNanos, objects);
      }

      public boolean info(Object... objects) {
         return this.info(NoSpamLogger.CLOCK.nanoTime(), objects);
      }

      public boolean warn(long nowNanos, Object... objects) {
         return this.log(NoSpamLogger.Level.WARN, nowNanos, objects);
      }

      public boolean warn(Object... objects) {
         return this.warn(NoSpamLogger.CLOCK.nanoTime(), objects);
      }

      public boolean error(long nowNanos, Object... objects) {
         return this.log(NoSpamLogger.Level.ERROR, nowNanos, objects);
      }

      public boolean error(Object... objects) {
         return this.error(NoSpamLogger.CLOCK.nanoTime(), objects);
      }
   }

   @VisibleForTesting
   interface Clock {
      long nanoTime();
   }

   public static enum Level {
      DEBUG,
      INFO,
      WARN,
      ERROR;

      private Level() {
      }
   }
}
