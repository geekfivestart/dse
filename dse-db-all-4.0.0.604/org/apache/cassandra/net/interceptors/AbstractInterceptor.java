package org.apache.cassandra.net.interceptors;

import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.collect.ImmutableSet.Builder;
import java.lang.management.ManagementFactory;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.net.Message;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.VerbGroup;
import org.apache.cassandra.net.Verbs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractInterceptor implements Interceptor, AbstractInterceptorMBean {
   private static final Logger logger = LoggerFactory.getLogger(AbstractInterceptor.class);
   private static final String INTERCEPTED_PROPERTY = "intercepted";
   private static final String INTERCEPTED_TYPES = "intercepted_types";
   private static final String INTERCEPTED_DIRECTIONS = "intercepted_directions";
   private static final String INTERCEPTED_LOCALITIES = "intercepted_localities";
   private static final String DISABLE_ON_STARTUP_PROPERTY = "disable_on_startup";
   private static final String RANDOM_INTERCEPTION = "random_interception_ratio";
   private final String name;
   private volatile ImmutableSet<Verb<?, ?>> interceptedVerbs;
   private volatile ImmutableSet<Message.Type> interceptedTypes;
   private volatile ImmutableSet<MessageDirection> interceptedDirections;
   private volatile ImmutableSet<Message.Locality> interceptedLocalities;
   private volatile boolean enabled;
   private volatile float interceptionChance;
   private final AtomicLong seen = new AtomicLong();
   private final AtomicLong intercepted = new AtomicLong();

   protected AbstractInterceptor(String name, ImmutableSet<Verb<?, ?>> interceptedVerbs, ImmutableSet<Message.Type> interceptedTypes, ImmutableSet<MessageDirection> interceptedDirections, ImmutableSet<Message.Locality> interceptedLocalities) {
      this.name = name;
      this.interceptedVerbs = interceptedVerbs;
      this.interceptedDirections = interceptedDirections;
      this.interceptedTypes = interceptedTypes;
      this.interceptedLocalities = interceptedLocalities;
      this.enabled = !this.disabledOnStartup();
      if(this.allowModifyingIntercepted()) {
         this.setFromProperty("intercepted", this::setIntercepted);
         this.setFromProperty("intercepted_types", this::setInterceptedTypes);
         this.setFromProperty("intercepted_directions", this::setInterceptedDirections);
         this.setFromProperty("intercepted_localities", this::setInterceptedLocalities);
      }

      this.setInterceptionChance(this.randomInterceptionRatio());
      this.registerJMX(name);
   }

   protected boolean allowModifyingIntercepted() {
      return true;
   }

   private void setFromProperty(String propertyName, Consumer<String> setter) {
      String configured = getProperty(propertyName);
      if(configured != null) {
         try {
            setter.accept(configured);
         } catch (ConfigurationException var5) {
            throw new ConfigurationException(String.format("Error parsing property -D%s%s: %s", new Object[]{"dse.net.interceptors.", propertyName, var5.getMessage()}), var5.getCause());
         }
      }
   }

   private boolean disabledOnStartup() {
      return getProperty("disable_on_startup", "false").trim().toLowerCase().equals("true");
   }

   private float randomInterceptionRatio() {
      String property = getProperty("random_interception_ratio");
      return property == null?1.0F:Float.valueOf(property).floatValue();
   }

   private void registerJMX(String name) {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();

      try {
         ObjectName jmxName = new ObjectName(String.format("%s:type=%s,name=%s", new Object[]{"com.datastax.net", "Interceptors", name}));
         mbs.registerMBean(this, jmxName);
      } catch (InstanceAlreadyExistsException var4) {
         throw new ConfigurationException(String.format("Multiple instances created with the same name '%s'. Use '%s=<someName>' when declaring interceptors to disambiguate", new Object[]{name, this.getClass().getSimpleName()}));
      } catch (Exception var5) {
         throw new RuntimeException("Unexpected error while setting up JMX for " + this.getClass(), var5);
      }
   }

   protected static String getProperty(String name) {
      return System.getProperty("dse.net.interceptors." + name);
   }

   protected static String getProperty(String name, String defaultValue) {
      return System.getProperty("dse.net.interceptors." + name, defaultValue);
   }

   private boolean shouldIntercept(Message<?> msg, MessageDirection direction) {
      if(this.enabled && this.interceptedVerbs.contains(msg.verb()) && this.interceptedDirections.contains(direction) && this.interceptedLocalities.contains(msg.locality()) && this.interceptedTypes.contains(msg.type())) {
         this.seen.incrementAndGet();
         return this.interceptionChance == 1.0F || ThreadLocalRandom.current().nextFloat() < this.interceptionChance;
      } else {
         return false;
      }
   }

   public <M extends Message<?>> void intercept(M message, InterceptionContext<M> context) {
      if(this.shouldIntercept(message, context.direction())) {
         logger.debug("{} intercepted {}", this.name, message);
         this.intercepted.incrementAndGet();
         this.handleIntercepted(message, context);
      } else {
         context.passDown(message);
      }

   }

   protected abstract <M extends Message<?>> void handleIntercepted(M var1, InterceptionContext<M> var2);

   public void enable() {
      if(!this.enabled) {
         logger.info("Enabling interceptor {}", this.name);
      }

      this.enabled = true;
   }

   public void disable() {
      if(this.enabled) {
         logger.info("Disabling interceptor {}", this.name);
      }

      this.enabled = false;
   }

   public boolean getEnabled() {
      return this.enabled;
   }

   public long getSeenCount() {
      return this.seen.get();
   }

   public long getInterceptedCount() {
      return this.intercepted.get();
   }

   public String getIntercepted() {
      Set<VerbGroup<?>> groups = Sets.newHashSet(Iterables.filter(Verbs.allGroups(), (g) -> {
         ImmutableSet var10001 = this.interceptedVerbs;
         this.interceptedVerbs.getClass();
         return Iterables.all(g, var10001::contains);
      }));
      Iterable<Verb<?, ?>> verbs = Iterables.filter(this.interceptedVerbs, (v) -> {
         return Iterables.all(groups, (g) -> {
            return !Iterables.contains(g, v);
         });
      });
      return Joiner.on(",").join(Iterables.concat(Iterables.transform(groups, VerbGroup::toString), Iterables.transform(verbs, (v) -> {
         return String.format("%s.%s", new Object[]{v.group(), v});
      })));
   }

   public void setIntercepted(String interceptedString) {
      if(!this.allowModifyingIntercepted()) {
         throw new ConfigurationException("Cannot update/configure what this interceptor intercepts");
      } else {
         Builder<Verb<?, ?>> builder = ImmutableSet.builder();
         List<String> names = Splitter.on(',').trimResults().omitEmptyStrings().splitToList(interceptedString);
         Iterator var4 = names.iterator();

         while(var4.hasNext()) {
            String name = (String)var4.next();
            List<String> vals = Splitter.on('.').splitToList(name.toUpperCase());
            if(vals.isEmpty() || vals.size() > 2) {
               throw new ConfigurationException(String.format("Invalid value '%s' for intercepted", new Object[]{name}));
            }

            try {
               Verbs.Group groupName = Verbs.Group.valueOf((String)vals.get(0));
               VerbGroup<?> group = (VerbGroup)Iterables.find(Verbs.allGroups(), (g) -> {
                  return g.id() == groupName;
               });
               if(vals.size() == 1) {
                  builder.addAll(group);
               } else {
                  builder.add(Iterables.find(group, (v) -> {
                     return v.name().equalsIgnoreCase((String)vals.get(1));
                  }));
               }
            } catch (Exception var9) {
               throw new ConfigurationException(String.format("Invalid value '%s' for intercepted", new Object[]{name}));
            }
         }

         this.interceptedVerbs = builder.build();
      }
   }

   private <T extends Enum<?>> String getInterceptedEnum(Set<T> values) {
      return Joiner.on(",").join(values);
   }

   private <T extends Enum<T>> ImmutableSet<T> setInterceptedEnum(String interceptedString, Class<T> klass) {
      if(!this.allowModifyingIntercepted()) {
         throw new ConfigurationException("Cannot update/configure what this interceptor intercepts");
      } else {
         try {
            Iterable<String> splits = Splitter.on(',').trimResults().omitEmptyStrings().split(interceptedString);
            return Sets.immutableEnumSet(Iterables.transform(splits, (str) -> {
               return Enum.valueOf(klass, str.toUpperCase());
            }));
         } catch (Exception var4) {
            throw new ConfigurationException(String.format("Invalid value '%s' for intercepted directions", new Object[]{interceptedString}));
         }
      }
   }

   public String getInterceptedDirections() {
      return this.getInterceptedEnum(this.interceptedDirections);
   }

   public void setInterceptedDirections(String interceptedString) {
      this.interceptedDirections = this.setInterceptedEnum(interceptedString, MessageDirection.class);
   }

   public String getInterceptedTypes() {
      return this.getInterceptedEnum(this.interceptedTypes);
   }

   public void setInterceptedTypes(String interceptedString) {
      this.interceptedTypes = this.setInterceptedEnum(interceptedString, Message.Type.class);
   }

   public String getInterceptedLocalities() {
      return this.getInterceptedEnum(this.interceptedLocalities);
   }

   public void setInterceptedLocalities(String interceptedString) {
      this.interceptedLocalities = this.setInterceptedEnum(interceptedString, Message.Locality.class);
   }

   public float getInterceptionChance() {
      return this.interceptionChance;
   }

   public void setInterceptionChance(float ratio) {
      if(ratio >= 0.0F && ratio <= 1.0F) {
         this.interceptionChance = ratio;
      } else {
         throw new ConfigurationException(String.format("Invalid value for %s: must be in [0, 1], got %f", new Object[]{"random_interception_ratio", Float.valueOf(ratio)}));
      }
   }
}
