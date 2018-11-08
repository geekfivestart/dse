package org.apache.cassandra.dht;

import com.google.common.collect.Multimap;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.gms.IFailureDetector;
import org.apache.cassandra.locator.IEndpointSnitch;
import org.apache.cassandra.locator.TokenMetadata;
import org.apache.cassandra.schema.Schema;

public final class StreamingOptions {
   public static final String BOOTSTRAP_INCLUDE_DCS = "cassandra.bootstrap.includeDCs";
   public static final String BOOTSTRAP_EXCLUDE_DCS = "cassandra.bootstrap.excludeDCs";
   public static final String BOOTSTRAP_INCLUDE_SOURCES = "cassandra.bootstrap.includeSources";
   public static final String BOOTSTRAP_EXCLUDE_SOURCES = "cassandra.bootstrap.excludeSources";
   public static final String BOOTSTRAP_EXCLUDE_KEYSPACES = "cassandra.bootstrap.excludeKeyspaces";
   public static final String ARG_INCLUDE_SOURCES = "specific_sources";
   public static final String ARG_EXCLUDE_SOURCES = "exclude_sources";
   public static final String ARG_INCLUDE_DC_NAMES = "source_dc_names";
   public static final String ARG_EXCLUDE_DC_NAMES = "exclude_dc_names";
   private final Map<String, StreamingOptions.Racks> includedDcs;
   private final Map<String, StreamingOptions.Racks> excludedDcs;
   private final Set<InetAddress> includedSources;
   private final Set<InetAddress> excludedSources;
   private final Set<String> excludedKeyspaces;

   public static StreamingOptions forBootStrap(TokenMetadata tokenMetadata) {
      try {
         Set<InetAddress> includedSources = getHostsFromProperty("cassandra.bootstrap.includeSources");
         Set<InetAddress> excludedSource = getHostsFromProperty("cassandra.bootstrap.excludeSources");
         Map<String, StreamingOptions.Racks> includedDCs = getDatacentersFromProperty("cassandra.bootstrap.includeDCs");
         Map<String, StreamingOptions.Racks> excludedDCs = getDatacentersFromProperty("cassandra.bootstrap.excludeDCs");
         validateSystemProperties(tokenMetadata, includedSources, excludedSource, includedDCs, excludedDCs);
         return new StreamingOptions(includedDCs, excludedDCs, includedSources, excludedSource, getKeyspacesFromProperty("cassandra.bootstrap.excludeKeyspaces"));
      } catch (IllegalArgumentException var5) {
         throw new ConfigurationException(var5.getMessage(), false);
      }
   }

   private static void validateSystemProperties(TokenMetadata tokenMetadata, Set<InetAddress> includedSources, Set<InetAddress> excludedSources, Map<String, StreamingOptions.Racks> includedDCs, Map<String, StreamingOptions.Racks> excludedDCs) {
      if(includedSources == null || excludedSources == null && includedDCs == null && excludedDCs == null) {
         if(excludedSources == null || includedDCs == null && excludedDCs == null) {
            if(includedDCs != null && excludedDCs != null) {
               Iterator var5 = includedDCs.keySet().iterator();

               while(var5.hasNext()) {
                  String dc = (String)var5.next();
                  if(excludedDCs.containsKey(dc) && ((StreamingOptions.Racks)excludedDCs.get(dc)).isEmpty() == ((StreamingOptions.Racks)includedDCs.get(dc)).isEmpty()) {
                     throw new IllegalArgumentException("The cassandra.bootstrap.includeDCs and cassandra.bootstrap.excludeDCs system properties are conflicting for the datacenter: " + dc);
                  }
               }
            }

            validateSourcesDCsRacks(tokenMetadata, includedSources, excludedSources, includedDCs, excludedDCs);
         } else {
            throw new IllegalArgumentException("The cassandra.bootstrap.excludeSources system property cannot be used together with the cassandra.bootstrap.includeSources, cassandra.bootstrap.includeDCs or cassandra.bootstrap.excludeDCs system properties");
         }
      } else {
         throw new IllegalArgumentException("The cassandra.bootstrap.includeSources system property cannot be used together with the cassandra.bootstrap.excludeSources, cassandra.bootstrap.includeDCs or cassandra.bootstrap.excludeDCs system properties");
      }
   }

   public static StreamingOptions forRebuild(TokenMetadata tokenMetadata, List<String> srcDcNames, List<String> excludeDcNames, List<String> specifiedSources, List<String> excludeSources) {
      Set<InetAddress> includedSources = getHostsFromArgument("specific_sources", "argument", specifiedSources);
      Set<InetAddress> excludedSource = getHostsFromArgument("exclude_sources", "argument", excludeSources);
      Map<String, StreamingOptions.Racks> includedDCs = getDatacentersFromArgument("source_dc_names", "argument", srcDcNames);
      Map<String, StreamingOptions.Racks> excludedDCs = getDatacentersFromArgument("exclude_dc_names", "argument", excludeDcNames);
      validateArguments(tokenMetadata, includedSources, excludedSource, includedDCs, excludedDCs);
      return new StreamingOptions(includedDCs, excludedDCs, includedSources, excludedSource, (Set)null);
   }

   public static StreamingOptions forRebuild(TokenMetadata tokenMetadata, String sourceDc, String specificSources) {
      Set<InetAddress> includedSources = getHostsFromArgument("specific-sources", "argument", asList(specificSources));
      Map<String, StreamingOptions.Racks> includedDCs = getDatacentersFromArgument("source-DC", "argument", sourceDc != null?Collections.singletonList(sourceDc):null);
      validateArguments(tokenMetadata, includedSources, (Set)null, includedDCs, (Map)null);
      return new StreamingOptions(includedDCs, (Map)null, includedSources, (Set)null, (Set)null);
   }

   private static void validateArguments(TokenMetadata tokenMetadata, Set<InetAddress> includedSources, Set<InetAddress> excludedSources, Map<String, StreamingOptions.Racks> includedDCs, Map<String, StreamingOptions.Racks> excludedDCs) {
      if(includedSources == null && excludedSources == null && includedDCs == null && excludedDCs == null) {
         throw new IllegalArgumentException("At least one of the specific_sources, exclude_sources, source_dc_names, exclude_dc_names (or src-dc-name) arguments must be specified for rebuild.");
      } else if(includedSources != null && (excludedSources != null || includedDCs != null || excludedDCs != null)) {
         throw new IllegalArgumentException("The specific_sources argument cannot be used together with the exclude_sources, source_dc_names or exclude_dc_names arguments");
      } else if(excludedSources != null && (includedDCs != null || excludedDCs != null)) {
         throw new IllegalArgumentException("The exclude_sources argument cannot be used together with the specific_sources, source_dc_names or exclude_dc_names arguments");
      } else {
         if(includedDCs != null && excludedDCs != null) {
            Iterator var5 = includedDCs.keySet().iterator();

            while(var5.hasNext()) {
               String dc = (String)var5.next();
               if(excludedDCs.containsKey(dc) && ((StreamingOptions.Racks)excludedDCs.get(dc)).isEmpty() == ((StreamingOptions.Racks)includedDCs.get(dc)).isEmpty()) {
                  throw new IllegalArgumentException("The source_dc_names and exclude_dc_names arguments are conflicting for the datacenter: " + dc);
               }
            }
         }

         validateSourcesDCsRacks(tokenMetadata, includedSources, excludedSources, includedDCs, excludedDCs);
      }
   }

   private static void validateSourcesDCsRacks(TokenMetadata tokenMetadata, Set<InetAddress> includedSources, Set<InetAddress> excludedSources, Map<String, StreamingOptions.Racks> includedDCs, Map<String, StreamingOptions.Racks> excludedDCs) {
      List<String> errors = new ArrayList();
      validateSources(includedSources, tokenMetadata, errors);
      validateSources(excludedSources, tokenMetadata, errors);
      validateDCsRacks(includedDCs, tokenMetadata, errors);
      validateDCsRacks(excludedDCs, tokenMetadata, errors);
      if(!errors.isEmpty()) {
         throw new IllegalArgumentException((String)errors.stream().collect(Collectors.joining(", ")));
      }
   }

   private static void validateSources(Set<InetAddress> sources, TokenMetadata tokenMetadata, List<String> errors) {
      if(sources != null) {
         Set<InetAddress> validSources = tokenMetadata.getEndpointToHostIdMapForReading().keySet();
         sources.stream().filter((source) -> {
            return !validSources.contains(source);
         }).forEach((source) -> {
            errors.add("Source '" + source + "' is not a known node in this cluster");
         });
      }
   }

   private static void validateDCsRacks(Map<String, StreamingOptions.Racks> dcRacks, TokenMetadata tokenMetadata, List<String> errors) {
      if(dcRacks != null) {
         Map<String, Multimap<String, InetAddress>> validDcRacks = tokenMetadata.getTopology().getDatacenterRacks();
         Iterator var4 = dcRacks.entrySet().iterator();

         while(true) {
            while(var4.hasNext()) {
               Entry<String, StreamingOptions.Racks> entry = (Entry)var4.next();
               String dc = (String)entry.getKey();
               Multimap<String, InetAddress> validRacks = (Multimap)validDcRacks.get(dc);
               if(validRacks != null && !validRacks.isEmpty()) {
                  ((StreamingOptions.Racks)entry.getValue()).racks.stream().filter((rack) -> {
                     return !validRacks.containsKey(rack);
                  }).forEach((rack) -> {
                     errors.add("Rack '" + rack + "' is not a known rack in DC '" + dc + "' of this cluster");
                  });
               } else {
                  errors.add("DC '" + dc + "' is not a known DC in this cluster");
               }
            }

            return;
         }
      }
   }

   private StreamingOptions(Map<String, StreamingOptions.Racks> includedDcs, Map<String, StreamingOptions.Racks> excludedDcs, Set<InetAddress> includedSources, Set<InetAddress> excludedSources, Set<String> excludedKeyspaces) {
      if((includedDcs == null || !includedDcs.isEmpty()) && (excludedDcs == null || !excludedDcs.isEmpty()) && (includedSources == null || !includedSources.isEmpty()) && (excludedSources == null || !excludedSources.isEmpty())) {
         this.includedDcs = includedDcs;
         this.excludedDcs = excludedDcs;
         this.includedSources = includedSources;
         this.excludedSources = excludedSources;
         this.excludedKeyspaces = excludedKeyspaces;
      } else {
         throw new IllegalArgumentException("Collections in this class must be ither null or not empty");
      }
   }

   public boolean acceptKeyspace(String keyspace) {
      return this.excludedKeyspaces == null || !this.excludedKeyspaces.contains(keyspace);
   }

   public RangeStreamer.ISourceFilter toSourceFilter(IEndpointSnitch snitch, IFailureDetector fd) {
      List<RangeStreamer.ISourceFilter> filters = new ArrayList();
      filters.add(SourceFilters.failureDetectorFilter(fd));
      filters.add(SourceFilters.excludeLocalNode());
      if(this.excludedSources != null) {
         filters.add(SourceFilters.excludeSources(this.excludedSources));
      }

      if(this.includedSources != null) {
         filters.add(SourceFilters.includeSources(this.includedSources));
      }

      if(this.excludedDcs != null) {
         filters.add(SourceFilters.excludeDcs(toSourceFilters(this.excludedDcs, snitch), snitch));
      }

      if(this.includedDcs != null) {
         filters.add(SourceFilters.includeDcs(toSourceFilters(this.includedDcs, snitch), snitch));
      }

      return SourceFilters.composite((List)filters);
   }

   private static Map<String, RangeStreamer.ISourceFilter> toSourceFilters(Map<String, StreamingOptions.Racks> racksPerDcs, IEndpointSnitch snitch) {
      Map<String, RangeStreamer.ISourceFilter> filtersPerDcs = new HashMap(racksPerDcs.size());
      Iterator var3 = racksPerDcs.entrySet().iterator();

      while(var3.hasNext()) {
         Entry<String, StreamingOptions.Racks> entry = (Entry)var3.next();
         filtersPerDcs.put(entry.getKey(), ((StreamingOptions.Racks)entry.getValue()).toSourceFilter(snitch));
      }

      return filtersPerDcs;
   }

   public String toString() {
      StringBuilder builder = new StringBuilder();
      if(this.includedDcs != null) {
         builder.append(" included DCs: ");
         builder.append(toString(this.includedDcs.values()));
      }

      if(this.excludedDcs != null) {
         builder.append(" excluded DCs: ");
         builder.append(toString(this.excludedDcs.values()));
      }

      if(this.includedSources != null) {
         builder.append(" included sources: ").append(toString(this.includedSources));
      }

      if(this.excludedSources != null) {
         builder.append(" excluded sources: ").append(toString(this.excludedSources));
      }

      if(this.excludedKeyspaces != null) {
         builder.append(" excluded keyspaces: ").append(toString(this.excludedKeyspaces));
      }

      return builder.toString();
   }

   private static String toString(Collection<? extends Object> collection) {
      return (String)collection.stream().map(Object::toString).collect(Collectors.joining(", "));
   }

   private static Set<String> getKeyspacesFromProperty(String property) {
      List<String> list = getPropertyAsList(property);
      if(list == null) {
         return null;
      } else {
         Set<String> keyspaces = new LinkedHashSet(list.size());
         Set<String> nonLocalStrategyKeyspaces = new HashSet(Schema.instance.getNonLocalStrategyKeyspaces());
         Iterator var4 = list.iterator();

         String name;
         do {
            if(!var4.hasNext()) {
               if(keyspaces.isEmpty()) {
                  throw new IllegalArgumentException(String.format("The %s system property does not specify any keyspace", new Object[]{property}));
               }

               return keyspaces;
            }

            name = (String)var4.next();
            if(!nonLocalStrategyKeyspaces.contains(name)) {
               throw new IllegalArgumentException(String.format("The %s keyspace specified within the %s system property is not an existing non local strategy keyspace", new Object[]{name, property}));
            }
         } while(keyspaces.add(name));

         throw new IllegalArgumentException(String.format("The %s keyspace specified within the %s system property must be specified only once", new Object[]{name, property}));
      }
   }

   private static Set<InetAddress> getHostsFromProperty(String property) {
      List<String> hostNames = getPropertyAsList(property);
      return getHostsFromArgument(property, "system property", hostNames);
   }

   private static Set<InetAddress> getHostsFromArgument(String argumentName, String argumentType, List<String> argument) {
      if(argument == null) {
         return null;
      } else {
         Set<InetAddress> hosts = new HashSet(argument.size());
         Iterator var4 = argument.iterator();

         while(var4.hasNext()) {
            String hostName = (String)var4.next();

            try {
               if(!hosts.add(InetAddress.getByName(hostName))) {
                  throw new IllegalArgumentException(String.format("The %s source must be specified only once in the %s %s", new Object[]{hostName, argumentName, argumentType}));
               }
            } catch (UnknownHostException var7) {
               throw new IllegalArgumentException(String.format("The %s source specified within the %s %s is unknown", new Object[]{hostName, argumentName, argumentType}));
            }
         }

         if(hosts.isEmpty()) {
            throw new IllegalArgumentException(String.format("The %s %s does not specify any source", new Object[]{argumentName, argumentType}));
         } else {
            return hosts;
         }
      }
   }

   private static Map<String, StreamingOptions.Racks> getDatacentersFromProperty(String property) {
      List<String> names = getPropertyAsList(property);
      return getDatacentersFromArgument(property, "system property", names);
   }

   private static Map<String, StreamingOptions.Racks> getDatacentersFromArgument(String argumentName, String argumentType, List<String> argument) {
      if(argument == null) {
         return null;
      } else {
         Map<String, StreamingOptions.Racks> datacenters = new LinkedHashMap();
         Iterator var4 = argument.iterator();

         while(var4.hasNext()) {
            String name = (String)var4.next();
            int sep = name.indexOf(58);
            if(sep == -1) {
               StreamingOptions.Racks racks = (StreamingOptions.Racks)datacenters.get(name);
               if(racks != null) {
                  if(racks.isEmpty()) {
                     throw new IllegalArgumentException(String.format("The %s datacenter must be specified only once in the %s %s", new Object[]{name, argumentName, argumentType}));
                  }

                  throw new IllegalArgumentException(String.format("The %s %s contains both a rack restriction and a datacenter restriction for the %s datacenter", new Object[]{argumentName, argumentType, name}));
               }

               datacenters.put(name, new StreamingOptions.Racks(name));
            } else {
               String dcName = name.substring(0, sep);
               String rackName = name.substring(sep + 1);
               StreamingOptions.Racks racks = (StreamingOptions.Racks)datacenters.get(dcName);
               if(racks == null) {
                  racks = new StreamingOptions.Racks(dcName);
                  racks.addRack(rackName);
                  datacenters.put(dcName, racks);
               } else {
                  if(racks.isEmpty()) {
                     throw new IllegalArgumentException(String.format("The %s %s contains both a rack restriction and a datacenter restriction for the %s datacenter", new Object[]{argumentName, argumentType, dcName}));
                  }

                  if(!racks.addRack(rackName)) {
                     throw new IllegalArgumentException(String.format("The %s rack must be specified only once in the %s %s", new Object[]{name, argumentName, argumentType}));
                  }
               }
            }
         }

         if(datacenters.isEmpty()) {
            throw new IllegalArgumentException(String.format("The %s %s does not specify any datacenter/rack", new Object[]{argumentName, argumentType}));
         } else {
            return datacenters;
         }
      }
   }

   private static List<String> getPropertyAsList(String name) {
      String property = System.getProperty(name);
      return asList(property);
   }

   private static List<String> asList(String property) {
      return property == null?null:(property.trim().isEmpty()?Collections.emptyList():(List)Arrays.stream(property.split(",")).map(String::trim).collect(Collectors.toList()));
   }

   private static final class Racks {
      private final String datacenter;
      private final Set<String> racks = new LinkedHashSet();

      public Racks(String datacenter) {
         this.datacenter = datacenter;
      }

      public boolean isEmpty() {
         return this.racks.isEmpty();
      }

      public boolean addRack(String name) {
         return this.racks.add(name);
      }

      public String toString() {
         return this.racks.isEmpty()?this.datacenter:(String)this.racks.stream().map((e) -> {
            return this.datacenter + ':' + e;
         }).collect(Collectors.joining(", "));
      }

      public RangeStreamer.ISourceFilter toSourceFilter(IEndpointSnitch snitch) {
         return this.racks.isEmpty()?SourceFilters.noop():SourceFilters.includeRacks(this.racks, snitch);
      }
   }
}
