package com.datastax.bdp.db.tools.nodesync;

import com.datastax.bdp.db.nodesync.NodeSyncService;
import com.datastax.driver.core.AbstractTableMetadata;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TokenRange;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Sets;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

@Command(
   name = "submit",
   description = "Submit a forced user validation"
)
public class SubmitValidation extends NodeSyncCommand {
   private static final Pattern RANGE_PATTERN = Pattern.compile("\\(\\s*(?<l>\\S+)\\s*,\\s*(?<r>\\S+)\\s*]");
   private static final Pattern UNSUPPORTED_RANGE_PATTERN = Pattern.compile("(\\(|\\[)\\s*(?<l>\\S+)\\s*,\\s*(?<r>\\S+)\\s*(]|\\))");
   @VisibleForTesting
   static final String UNSUPPORTED_RANGE_MESSAGE = "Invalid input range: %s: only ranges with an open start and closed end are allowed. Did you meant (%s, %s]?";
   @Arguments(
      usage = "<table> [<range>...]",
      description = "The qualified table name, optionally followed by token ranges of the form (x, y]. If no token ranges are specified, then all the tokens will be validated."
   )
   List<String> args = new ArrayList();
   @Option(
      type = OptionType.COMMAND,
      name = {"-r", "--rate"},
      description = "Rate to be used just for this validation, in KB per second"
   )
   private Integer rateInKB = null;

   public SubmitValidation() {
   }

   public final void execute(Metadata metadata, Session session, NodeProbes probes) {
      if(this.args.size() < 1) {
         throw new NodeSyncException("A qualified table name should be specified");
      } else {
         AbstractTableMetadata tableMetadata = parseTable(metadata, (String)null, (String)this.args.get(0));
         String keyspaceName = tableMetadata.getKeyspace().getName();
         String tableName = tableMetadata.getName();
         List<Range<Token>> requestedRanges = parseRanges(metadata, this.args.subList(1, this.args.size()));
         Map<Range<Token>, Set<InetAddress>> rangeReplicas = liveRangeReplicas(metadata, keyspaceName, requestedRanges);
         Set<InetAddress> allReplicas = (Set)rangeReplicas.values().stream().flatMap(Collection::stream).collect(Collectors.toSet());
         validateRate(this.rateInKB);
         String id = UUIDGen.getTimeUUID().toString();
         Map<String, String> baseOptions = new HashMap();
         baseOptions.put("id", id);
         baseOptions.put("keyspace", keyspaceName);
         baseOptions.put("table", tableName);
         if(this.rateInKB != null) {
            baseOptions.put("rate", this.rateInKB.toString());
         }

         Set<InetAddress> successfulReplicas = new HashSet();
         Iterator var13 = allReplicas.iterator();

         while(true) {
            if(var13.hasNext()) {
               InetAddress address = (InetAddress)var13.next();
               if(!rangeReplicas.isEmpty()) {
                  Set<Range<Token>> ranges = (Set)rangeReplicas.entrySet().stream().filter((e) -> {
                     return ((Set)e.getValue()).remove(address);
                  }).map(Entry::getKey).collect(Collectors.toCollection(TreeSet::<init>));
                  if(ranges.isEmpty()) {
                     return;
                  }

                  try {
                     Map<String, String> options = new HashMap(baseOptions);
                     options.put("ranges", format((Collection)ranges));
                     probes.startUserValidation(address, options);
                     ranges.forEach(rangeReplicas::remove);
                     successfulReplicas.add(address);
                     this.printVerbose("%s: submitted for ranges %s", new Object[]{address, ranges});
                  } catch (Exception var17) {
                     if(rangeReplicas.values().stream().anyMatch(Set::isEmpty)) {
                        System.err.printf("%s: failed for ranges %s, there are no more replicas to try: %s%n", new Object[]{address, ranges, var17.getMessage()});
                        this.cancel(probes, successfulReplicas, id);
                        throw new NodeSyncException("Submission failed");
                     }

                     this.printVerbose("%s: failed for ranges %s, trying next replicas: %s", new Object[]{address, ranges, var17.getMessage()});
                  }
                  continue;
               }
            }

            System.out.println(id);
            return;
         }
      }
   }

   private static Map<Range<Token>, Set<InetAddress>> liveRangeReplicas(Metadata metadata, String keyspace, Collection<Range<Token>> ranges) {
      Token.TokenFactory tkFactory = tokenFactory(metadata);
      Map<Range<Token>, Set<InetAddress>> replicas = new HashMap();
      Iterator var5 = ((Set)metadata.getAllHosts().stream().filter(Host::isUp).collect(Collectors.toSet())).iterator();

      while(var5.hasNext()) {
         Host host = (Host)var5.next();
         List<Range<Token>> localRanges = ranges(tkFactory, metadata.getTokenRanges(keyspace, host));
         localRanges.stream().flatMap((r) -> {
            Stream var10000 = ranges.stream();
            r.getClass();
            var10000 = var10000.filter(r::intersects);
            r.getClass();
            return var10000.map(r::intersectionWith);
         }).flatMap(Collection::stream).distinct().forEach((r) -> {
            ((Set)replicas.computeIfAbsent(r, (k) -> {
               return Sets.newHashSet();
            })).add(host.getBroadcastAddress());
         });
      }

      if(ranges.stream().anyMatch((r) -> {
         return !r.subtractAll(replicas.keySet()).isEmpty();
      })) {
         throw new NodeSyncException("There are not enough live replicas to cover all the requested ranges");
      } else {
         return replicas;
      }
   }

   private void cancel(NodeProbes probes, Set<InetAddress> addresses, String id) {
      if(!addresses.isEmpty()) {
         System.err.println("Cancelling validation in those nodes where it was already submitted: " + addresses);
         Set<InetAddress> failed = new HashSet();
         Iterator var5 = addresses.iterator();

         while(var5.hasNext()) {
            InetAddress address = (InetAddress)var5.next();

            try {
               probes.cancelUserValidation(address, id);
               System.err.printf("%s: cancelled%n", new Object[]{address});
            } catch (NodeSyncService.NotFoundValidationException var8) {
               System.err.printf("%s: already finished%n", new Object[]{address});
            } catch (NodeSyncService.CancelledValidationException var9) {
               System.err.printf("%s: already cancelled%n", new Object[]{address});
            } catch (Exception var10) {
               failed.add(address);
               System.err.printf("%s: cancellation failed: %s%n", new Object[]{address, var10.getMessage()});
            }
         }

         if(failed.isEmpty()) {
            System.err.printf("Validation %s has been successfully cancelled%n", new Object[]{id});
         } else {
            System.err.printf("Validation %s is still running in nodes %s%n", new Object[]{id, failed});
         }

      }
   }

   @VisibleForTesting
   static void validateRate(Integer rateInKB) {
      if(rateInKB != null && rateInKB.intValue() <= 0) {
         throw new NodeSyncException("Rate must be positive");
      }
   }

   @VisibleForTesting
   static Token.TokenFactory tokenFactory(Metadata metadata) {
      return FBUtilities.newPartitioner(metadata.getPartitioner()).getTokenFactory();
   }

   @VisibleForTesting
   static List<Range<Token>> parseRanges(Metadata metadata, List<String> args) {
      Token.TokenFactory tkFactory = tokenFactory(metadata);
      return Range.normalize((Collection)(args.isEmpty()?ranges(tkFactory, metadata.getTokenRanges()):(Collection)args.stream().map((s) -> {
         return parseRange(tkFactory, s);
      }).collect(Collectors.toList())));
   }

   @VisibleForTesting
   static Range<Token> parseRange(Token.TokenFactory tokenFactory, String str) {
      String s = str.trim();
      Matcher matcher = RANGE_PATTERN.matcher(s);
      if(!matcher.matches()) {
         matcher = UNSUPPORTED_RANGE_PATTERN.matcher(s);
         if(matcher.matches()) {
            String msg = String.format("Invalid input range: %s: only ranges with an open start and closed end are allowed. Did you meant (%s, %s]?", new Object[]{s, matcher.group("l"), matcher.group("r")});
            throw new NodeSyncException(msg);
         } else {
            throw new NodeSyncException("Cannot parse range: " + s);
         }
      } else {
         Token start = parseToken(tokenFactory, matcher.group("l"));
         Token end = parseToken(tokenFactory, matcher.group("r"));
         return new Range(start, end);
      }
   }

   private static Token parseToken(Token.TokenFactory tokenFactory, String str) {
      try {
         return tokenFactory.fromString(str);
      } catch (Exception var3) {
         throw new NodeSyncException("Cannot parse token: " + str);
      }
   }

   @VisibleForTesting
   static String format(Collection<Range<Token>> ranges) {
      return (String)Range.normalize(ranges).stream().map(SubmitValidation::format).collect(Collectors.joining(","));
   }

   private static String format(Range<Token> range) {
      return range.left + ":" + range.right;
   }

   private static List<Range<Token>> ranges(Token.TokenFactory tokenFactory, Collection<TokenRange> ranges) {
      return (List)ranges.stream().map((s) -> {
         return range(tokenFactory, s);
      }).collect(Collectors.toList());
   }

   private static Range<Token> range(Token.TokenFactory tokenFactory, TokenRange range) {
      Token start = tokenFactory.fromString(range.getStart().toString());
      Token end = tokenFactory.fromString(range.getEnd().toString());
      return new Range(start, end);
   }
}
