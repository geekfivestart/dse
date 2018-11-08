package org.apache.cassandra.tools.nodetool.nodesync;

import com.datastax.bdp.db.nodesync.RateSimulator;
import com.google.common.base.Splitter;
import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;
import org.apache.cassandra.utils.Streams;
import org.apache.cassandra.utils.units.TimeValue;

@Command(
   name = "ratesimulator",
   description = "Simulate rates necessary to achieve NodeSync deadline based on configurable assumptions "
)
public class RateSimulatorCmd extends NodeTool.NodeToolCmd {
   private static final Splitter SPLIT_ON_COMMA = Splitter.on(',').trimResults().omitEmptyStrings();
   private static final Splitter SPLIT_ON_COLON = Splitter.on(':').trimResults().omitEmptyStrings();
   private static final Pattern TABLE_NAME_PATTERN = Pattern.compile("((?<k>(\\w+|\"\\w+\"))\\.)?(?<t>(\\w+|\"\\w+\"))");
   @Arguments(
      title = "sub-command",
      usage = "<sub-command>",
      description = "Simulator sub-command: use 'help' (the default if unset) for a listing of all available sub-commands."
   )
   private String subCommand = null;
   @Option(
      title = "factor",
      name = {"-sg", "--size-growth-factor"},
      description = "by how much to increase data sizes to account for data grow; only for the 'simulate' sub-command."
   )
   private Float sizeGrowthFactor = null;
   @Option(
      title = "factor",
      name = {"-ds", "--deadline-safety-factor"},
      description = "by how much to decrease table deadlines to account for imperfect conditions; only for the 'simulate' sub-command."
   )
   private Float deadlineSafetyFactor = null;
   @Option(
      title = "factor",
      name = {"-rs", "--rate-safety-factor"},
      description = "By how much to increase the final rate to account for imperfect conditions; only for the 'simulate' sub-command."
   )
   private Float rateSafetyFactor = null;
   @Option(
      title = "overrides",
      name = {"--deadline-overrides"},
      description = "Allow override the configure deadline for some/all of the tables in the simulation."
   )
   private String deadlineOverrides = null;
   @Option(
      title = "ignore replication factor",
      name = {"--ignore-replication-factor"},
      description = "Don't take the replication factor in the simulation."
   )
   private boolean ignoreReplicationFactor = false;
   @Option(
      title = "includes",
      name = {"-i", "--includes"},
      description = "A comma-separated list of tables to include in the simulation even if NodeSync is not enabled server-side; this allow to simulate the impact on the rate of enabling NodeSync on those tables."
   )
   private String includes = null;
   @Option(
      title = "excludes",
      name = {"-e", "--excludes"},
      description = "A comma-separated list of tables to exclude tables from the simulation even if NodeSync is enabled server-side; this allow to simulate the impact on the rate of disabling NodeSync on those tables."
   )
   private String excludes = null;
   @Option(
      title = "verbose output",
      name = {"-v", "--verbose"},
      description = "Turn on verbose output, giving details on how the simulation is carried out."
   )
   private boolean verbose = false;

   public RateSimulatorCmd() {
   }

   public void execute(NodeProbe probe) {
      RateSimulatorCmd.SubCommand cmd = this.parseSubCommand();
      if(cmd == RateSimulatorCmd.SubCommand.HELP) {
         this.printHelp();
      } else {
         RateSimulator.Info info = RateSimulator.Info.fromJMX(probe.getNodeSyncRateSimulatorInfo(this.includes != null));

         try {
            info = this.withIncludes(info);
            info = this.withExcludes(info);
            info = this.withOverrides(info);
         } catch (IllegalArgumentException var5) {
            this.printError(var5.getMessage(), new Object[0]);
            System.exit(1);
            throw new AssertionError();
         }

         if(info.isEmpty()) {
            this.print("No tables have NodeSync enabled; nothing to simulate", new Object[0]);
         } else {
            RateSimulator simulator = new RateSimulator(info, this.getSimulationParameters(cmd));
            if(this.ignoreReplicationFactor) {
               simulator.ignoreReplicationFactor();
            }

            if(this.verbose) {
               simulator.withLogger((x$0) -> {
                  this.print(x$0, new Object[0]);
               });
               simulator.computeRate();
            } else {
               this.print("Computed rate: %s.", new Object[]{simulator.computeRate()});
            }

         }
      }
   }

   private RateSimulatorCmd.SubCommand parseSubCommand() {
      if(this.subCommand == null) {
         return RateSimulatorCmd.SubCommand.HELP;
      } else {
         try {
            return RateSimulatorCmd.SubCommand.valueOf(this.subCommand.trim().toUpperCase());
         } catch (IllegalArgumentException var2) {
            this.printError("Unknown sub-command '%s' for the rate simulator; use 'help' ('nodetool nodesyncservice ratesimulator help') for details on available sub-commands", new Object[]{this.subCommand});
            System.exit(1);
            throw new AssertionError();
         }
      }
   }

   private RateSimulator.Parameters getSimulationParameters(RateSimulatorCmd.SubCommand cmd) {
      if(cmd == RateSimulatorCmd.SubCommand.SIMULATE) {
         this.checkSet(this.sizeGrowthFactor, "-sg/--size-growth-factor");
         this.checkSet(this.deadlineSafetyFactor, "-ds/--deadline-safety-factor");
         this.checkSet(this.rateSafetyFactor, "-rs/--rate-safety-factor");
         return RateSimulator.Parameters.builder().sizeGrowingFactor(this.sizeGrowthFactor.floatValue()).deadlineSafetyFactor(this.deadlineSafetyFactor.floatValue()).rateSafetyFactor(this.rateSafetyFactor.floatValue()).build();
      } else {
         this.checkUnset(this.sizeGrowthFactor, cmd, "-sg/--size-growth-factor");
         this.checkUnset(this.deadlineSafetyFactor, cmd, "-ds/--deadline-safety-factor");
         this.checkUnset(this.rateSafetyFactor, cmd, "-rs/--rate-safety-factor");
         switch(null.$SwitchMap$org$apache$cassandra$tools$nodetool$nodesync$RateSimulatorCmd$SubCommand[cmd.ordinal()]) {
         case 1:
            return RateSimulator.Parameters.THEORETICAL_MINIMUM;
         case 2:
            return RateSimulator.Parameters.MINIMUM_RECOMMENDED;
         case 3:
            return RateSimulator.Parameters.RECOMMENDED;
         default:
            throw new AssertionError();
         }
      }
   }

   private RateSimulator.Info withOverrides(RateSimulator.Info info) {
      if(this.deadlineOverrides != null && !this.deadlineOverrides.isEmpty()) {
         Set<String> knownTables = (Set)Streams.of(info.tables()).map(RateSimulator.TableInfo::tableName).collect(Collectors.toSet());
         long catchAllOverride = -1L;
         Map<String, Long> overrides = new HashMap();
         Iterator var6 = SPLIT_ON_COMMA.split(this.deadlineOverrides).iterator();

         while(var6.hasNext()) {
            String s = (String)var6.next();
            List<String> l = SPLIT_ON_COLON.splitToList(s);
            if(l.size() != 2) {
               throw new IllegalArgumentException(String.format("Invalid deadline override '%s' in '%s'", new Object[]{s, this.deadlineOverrides}));
            }

            String tableName = (String)l.get(0);
            long deadlineValue = this.parseDeadlineValue((String)l.get(1), (String)l.get(0));
            if(tableName.equals("*")) {
               if(catchAllOverride > 0L) {
                  throw new IllegalArgumentException(String.format("Duplicate entry for '*' found in '%s'", new Object[]{this.deadlineOverrides}));
               }

               catchAllOverride = deadlineValue;
            } else {
               if(!TABLE_NAME_PATTERN.matcher(tableName).matches()) {
                  throw new IllegalArgumentException(String.format("Invalid table name '%s' in '%s'", new Object[]{tableName, this.deadlineOverrides}));
               }

               if(!knownTables.contains(tableName)) {
                  throw new IllegalArgumentException(String.format("Table %s doesn't appear to be a NodeSync-enabled table", new Object[]{tableName}));
               }

               overrides.put(tableName, Long.valueOf(deadlineValue));
            }
         }

         return info.transform((t) -> {
            Long v = (Long)overrides.get(t.tableName());
            if(v != null && !t.isNodeSyncEnabled) {
               throw new IllegalArgumentException(String.format("Deadline override for %s, but it is not included in the rate simulation (doesn't have nodesyn enabled server side, and is not part of -i/--includes)", new Object[]{t.tableName()}));
            } else {
               long tableOverride = v == null?catchAllOverride:v.longValue();
               return tableOverride < 0L?t:t.withNewDeadline(TimeValue.of(tableOverride, TimeUnit.SECONDS));
            }
         });
      } else {
         return info;
      }
   }

   private long parseDeadlineValue(String v, String table) {
      TimeUnit unit = TimeUnit.SECONDS;
      switch(v.charAt(v.length() - 1)) {
      case 'd':
         unit = TimeUnit.DAYS;
         v = v.substring(0, v.length() - 1);
         break;
      case 'h':
         unit = TimeUnit.HOURS;
         v = v.substring(0, v.length() - 1);
         break;
      case 'm':
         unit = TimeUnit.MINUTES;
         v = v.substring(0, v.length() - 1);
      }

      try {
         return unit.toSeconds(Long.parseLong(v));
      } catch (NumberFormatException var5) {
         throw new IllegalArgumentException(String.format("Cannot parse deadline from '%s' for table %s", new Object[]{v, table}));
      }
   }

   private RateSimulator.Info withIncludes(RateSimulator.Info info) {
      if(this.includes == null) {
         return info;
      } else {
         Set<String> toInclude = this.parseTableNames(this.includes);
         RateSimulator.Info newInfo = info.transform((t) -> {
            return toInclude.remove(t.tableName())?t.withNodeSyncEnabled():t;
         });
         if(!toInclude.isEmpty()) {
            throw new IllegalArgumentException("Unknown tables listed in -i/--includes: " + toInclude);
         } else {
            return newInfo;
         }
      }
   }

   private RateSimulator.Info withExcludes(RateSimulator.Info info) {
      if(this.excludes == null) {
         return info;
      } else {
         Set<String> toExclude = this.parseTableNames(this.excludes);
         RateSimulator.Info newInfo = info.transform((t) -> {
            return toExclude.remove(t.tableName())?t.withoutNodeSyncEnabled():t;
         });
         if(!toExclude.isEmpty()) {
            throw new IllegalArgumentException("Unknown or not-NodeSync-enabled tables listed in -e/--excludes: " + toExclude);
         } else {
            return newInfo;
         }
      }
   }

   private Set<String> parseTableNames(String str) {
      Set<String> names = new HashSet();
      Iterator var3 = SPLIT_ON_COMMA.split(str).iterator();

      while(var3.hasNext()) {
         String name = (String)var3.next();
         if(!TABLE_NAME_PATTERN.matcher(name).matches()) {
            throw new IllegalArgumentException(String.format("Invalid table name '%s' in '%s'", new Object[]{name, str}));
         }

         names.add(name);
      }

      return names;
   }

   private void checkUnset(Float factor, RateSimulatorCmd.SubCommand cmd, String option) {
      if(factor != null) {
         this.printError("Cannot use %s for the %s sub-command; this can only be used with the %s sub-command", new Object[]{option, cmd, RateSimulatorCmd.SubCommand.SIMULATE});
         System.exit(1);
      }
   }

   private void checkSet(Float factor, String option) {
      if(factor == null) {
         this.printError("Missing mandatory option %s for the %s sub-command", new Object[]{option, RateSimulatorCmd.SubCommand.SIMULATE});
         System.exit(1);
      }
   }

   private void print(String format, Object... args) {
      if(args.length == 0) {
         System.out.println(format);
      } else {
         System.out.println(String.format(format, args));
      }

   }

   private void printError(String format, Object... args) {
      if(args.length == 0) {
         System.err.println(format);
      } else {
         System.err.println(String.format(format, args));
      }

   }

   private void printHelp() {
      this.print("NodeSync Rate Simulator", new Object[0]);
      this.print("=======================", new Object[0]);
      this.print("", new Object[0]);
      this.print("The NodeSync rate simulator helps in the configuration of the validation rate of", new Object[0]);
      this.print("the NodeSync service by computing the rate necessary for NodeSync to validate", new Object[0]);
      this.print("all tables within their allowed deadlines (the NodeSync 'deadline_target_sec'", new Object[0]);
      this.print("table option) taking a number of parameters into account.", new Object[0]);
      this.print("", new Object[0]);
      this.print("There is unfortunately no perfect value for the validation rate because NodeSync", new Object[0]);
      this.print("has to deal with many imponderables. Typically, when a node fails, it won't", new Object[0]);
      this.print("participate in NodeSync validation while it is offline, which impact the overall", new Object[0]);
      this.print("rate, but failures cannot by nature be fully predicted. Similarly, some node", new Object[0]);
      this.print("may not achieve the configured rate at all time in period of overload, or due to", new Object[0]);
      this.print("various unexpected events. Lastly, the rate required to repair all tables within", new Object[0]);
      this.print("a fixed amount of time directly depends on the size of the data to validate,", new Object[0]);
      this.print("which is generally a moving target. One should thus build safety margins within", new Object[0]);
      this.print("the configured rate and this tool helps with this.", new Object[0]);
      this.print("", new Object[0]);
      this.print("", new Object[0]);
      this.print("Sub-commands", new Object[0]);
      this.print("------------", new Object[0]);
      this.print("", new Object[0]);
      this.print("The simulator supports the following sub-commands:", new Object[0]);
      this.print("", new Object[0]);
      this.print("  help: display this help message.", new Object[0]);
      this.print("  simulate: simulates the rate corresponding to the parameters provided as", new Object[0]);
      this.print("    options (see below for details).", new Object[0]);
      this.print("  recommended: simulates a recommended 'default' rate, one that considers data", new Object[0]);
      this.print("    growing up to double the current size, and has healthy margin to account", new Object[0]);
      this.print("    for failures and other events. ", new Object[0]);
      this.print("  recommended_minimum: simulates the minimum rate that is recommended. Note that", new Object[0]);
      this.print("    this is truly a minimum, which assume barely any increase in data size and", new Object[0]);
      this.print("    a fairly healthy cluster (little failures, mostly sustained rate). If you", new Object[0]);
      this.print("    are new to NodeSync and/or unsure, we advise starting with the 'recommended'", new Object[0]);
      this.print("    sub-command instead.", new Object[0]);
      this.print("  theoretical_minimum: simulates the minimum theoretical rate that could", new Object[0]);
      this.print("    possibly allow to validate all NodeSync-enabled tables within their", new Object[0]);
      this.print("    respective deadlines, assuming no data-grow, no failure and a perfectly", new Object[0]);
      this.print("    sustained rate. This is purely an indicative value: those assumption are", new Object[0]);
      this.print("    unrealistic and one should *not* use the rate this return in practice.", new Object[0]);
      this.print("", new Object[0]);
      this.print("The rate computed by the 'recommended' simulation is a good starting point for", new Object[0]);
      this.print("new comers, but please keep in mind that this is largely indicative and cannot", new Object[0]);
      this.print("be a substitute for monitoring NodeSync and adjusting the rate if necessary.", new Object[0]);
      this.print("", new Object[0]);
      this.print("Further, those recommended values are likely to be too low on new and almost", new Object[0]);
      this.print("empty clusters. Indeed, the size of data on such cluster may initially grow", new Object[0]);
      this.print("with a high multiplicative factor (Loading 100GB of data rapidly in a 1MB", new Object[0]);
      this.print("initial cluster, the 'recommended' value at 1MB will be way below what is", new Object[0]);
      this.print("needed at 100GB). In such cases, consider using the 'simulate' sub-command to", new Object[0]);
      this.print("perform a simulation with parameters tailored to your own needs.", new Object[0]);
      this.print("", new Object[0]);
      this.print("", new Object[0]);
      this.print("Simulation", new Object[0]);
      this.print("----------", new Object[0]);
      this.print("", new Object[0]);
      this.print("To perform a simulation, the simulator retrieves from the connected nodes", new Object[0]);
      this.print("information on all NodeSync-enabled tables, including their current data size", new Object[0]);
      this.print("and the value of their 'deadline_target_sec' property. Then, the minimum viable", new Object[0]);
      this.print("rate is computed using the following parameters:", new Object[0]);
      this.print("", new Object[0]);
      this.print("  'size growth factor' (-sg/--size-growth-factor): by how much to increase the", new Object[0]);
      this.print("    current size to account for data size. For instance, a factor of 0.5 will", new Object[0]);
      this.print("    compute a rate that is suitable up to data growing 50%, 1.0 will be suitable", new Object[0]);
      this.print("    for doubling data, etc.", new Object[0]);
      this.print("  'deadline safety factor' (-ds/--deadline-safety-factor): by how much to", new Object[0]);
      this.print("    decrease each table deadline target in the computation. For example, a", new Object[0]);
      this.print("    factor of 0.25 will compute a rate such that in perfect condition, each", new Object[0]);
      this.print("    table is validated within 75% of their full deadline target.", new Object[0]);
      this.print("  'rate safety factor' (-rs/--rate-safety-factor): a final factor by which the", new Object[0]);
      this.print("    computed rate is increased as a safety margin. For instance, a 10% factor", new Object[0]);
      this.print("    will return a rate that 10% bigger than with a 0% factor.", new Object[0]);
      this.print("", new Object[0]);
      this.print("Note that the parameters above should be provided for the 'simulate' sub-command", new Object[0]);
      this.print("but couldn't/shouldn't for other sub-command, as those provide simulations based", new Object[0]);
      this.print("on pre-defined parameters.", new Object[0]);
      this.print("", new Object[0]);
      this.print("Lastly, all simulations (all sub-commands) can also be influenced by the", new Object[0]);
      this.print("following options:", new Object[0]);
      this.print("", new Object[0]);
      this.print("  -v/--verbose: Display all steps taken by the simulation. This is a useful", new Object[0]);
      this.print("    option to understand the simulations, but can be very verbose with many", new Object[0]);
      this.print("    tables.", new Object[0]);
      this.print("  -i/--includes: takes a comma-separated list of table names that doesn't have", new Object[0]);
      this.print("    NodeSync enabled server-side but should be included in the simulation", new Object[0]);
      this.print("    nonetheless. This allows to simulate the impact enabling NodeSync on those", new Object[0]);
      this.print("    tables would have on the rate.", new Object[0]);
      this.print("  -e/--excludes: takes a comma-separated list of table names that have NodeSync", new Object[0]);
      this.print("    enabled server-side but should not be included in the simulation regardless.", new Object[0]);
      this.print("    This allows to simulate the impact disabling NodeSync on those tables would", new Object[0]);
      this.print("    have on the rate.", new Object[0]);
      this.print("  --ignore-replication-factor: ignores the replication factor in the simulation.", new Object[0]);
      this.print("    By default, the simulator assumes NodeSync runs on every node of the cluster", new Object[0]);
      this.print("    (which is highly recommended), and so that validation work is spread amongst", new Object[0]);
      this.print("    replica and as such, each node only has to validate 1/RF of the data it", new Object[0]);
      this.print("    owns. This option removes that assumption, computing a rate that takes the", new Object[0]);
      this.print("    totally of the data the node stores into account.", new Object[0]);
      this.print("  -do/--deadline-overrides=<overrides>: by default, the simulator considers each", new Object[0]);
      this.print("    table must be validated within the table 'deadline_target_sec' option. This", new Object[0]);
      this.print("    option allows to simulate the impact on the rate of changing that option for", new Object[0]);
      this.print("    some (or all) of the tables. When provided, <overrides> must be a comma", new Object[0]);
      this.print("    separated list of <table>:<deadline> pairs, where <table> should be a fully", new Object[0]);
      this.print("    qualified table name and <deadline> the deadline target to use for that", new Object[0]);
      this.print("    table in the simulation (in seconds by default, but can be followed by a", new Object[0]);
      this.print("    single character unit for convenient: 'm' for minutes, 'h' for hours or 'd'", new Object[0]);
      this.print("    for days). Optionally, the special character '*' can be used in lieu of", new Object[0]);
      this.print("    <table> to define a deadline for 'all other' tables (if not present, any", new Object[0]);
      this.print("    table no on the override list will use the deadline configured on the table", new Object[0]);
      this.print("    as usual). So for instance:", new Object[0]);
      this.print("      --deadline-overrides 'ks.foo:20h,*:10d'", new Object[0]);
      this.print("    will simulate using a 20 hours deadline for the 'ks.foo' table, and a 10", new Object[0]);
      this.print("    days deadline for any other tables.", new Object[0]);
   }

   private static enum SubCommand {
      HELP,
      SIMULATE,
      THEORETICAL_MINIMUM,
      RECOMMENDED_MINIMUM,
      RECOMMENDED;

      private SubCommand() {
      }

      public String toString() {
         return super.toString().toLowerCase();
      }
   }
}
