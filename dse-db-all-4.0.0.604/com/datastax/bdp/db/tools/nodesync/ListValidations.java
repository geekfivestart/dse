package com.datastax.bdp.db.tools.nodesync;

import com.datastax.bdp.db.nodesync.UserValidationProposer;
import com.datastax.bdp.db.nodesync.ValidationOutcome;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.UDTValue;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import io.airlift.airline.OptionType;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.BinaryOperator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.ToIntFunction;
import java.util.stream.Collectors;
import org.apache.cassandra.utils.Streams;
import org.apache.cassandra.utils.units.SizeUnit;
import org.apache.cassandra.utils.units.TimeValue;
import org.apache.cassandra.utils.units.Units;

@Command(
   name = "list",
   description = "List user validations. By default, only running validations are displayed."
)
public class ListValidations extends NodeSyncCommand {
   private static final String ID = "Identifier";
   private static final String TABLE = "Table";
   private static final String STATUS = "Status";
   private static final String OUTCOME = "Outcome";
   private static final String DURATION = "Duration";
   private static final String ETA = "ETA";
   private static final String PROGRESS = "Progress";
   private static final String VALIDATED = "Validated";
   private static final String REPAIRED = "Repaired";
   private static final String FORMAT = "%%-%ds  %%-%ds  %%-%ds  %%-%ds  %%%ds  %%%ds  %%%ds  %%%ds  %%%ds%n";
   @Option(
      type = OptionType.COMMAND,
      name = {"-a", "--all"},
      description = "List all either running or finished validations since less then 1 day"
   )
   private boolean all = false;

   public ListValidations() {
   }

   public final void execute(Metadata metadata, Session session, NodeProbes probes) {
      Select select = QueryBuilder.select(new String[]{"id", "keyspace_name", "table_name", "status", "outcomes", "started_at", "ended_at", "segments_to_validate", "segments_validated", "metrics"}).from("system_distributed", "nodesync_user_validations");
      Predicate<Row> statusFilter = (r) -> {
         return this.all || Objects.equals(r.getString("status"), UserValidationProposer.Status.RUNNING.toString());
      };
      List<ListValidations.Validation> validations = (List)((Map)Streams.of(session.execute(select)).filter(statusFilter).map((x$0) -> {
         return ListValidations.Validation.fromRow(x$0);
      }).collect(Collectors.groupingBy((v) -> {
         return v.key;
      }, Collectors.reducing(ListValidations.Validation::combineWith)))).values().stream().filter(Optional::isPresent).map(Optional::get).collect(Collectors.toList());
      List<Map<String, String>> table = new ArrayList(validations.size());
      Iterator var8 = validations.iterator();

      while(var8.hasNext()) {
         ListValidations.Validation validation = (ListValidations.Validation)var8.next();
         Map<String, String> map = new HashMap(6);
         map.put("Identifier", validation.key.id);
         map.put("Table", fullyQualifiedTableName(validation.key.keyspace, validation.key.table));
         map.put("Status", validation.status.toString());
         map.put("Outcome", validation.summarizedOutcome());
         map.put("Duration", validation.duration().toString());
         map.put("ETA", validation.eta());
         map.put("Progress", validation.progress() + "%");
         map.put("Validated", Units.toString(validation.bytesValidated, SizeUnit.BYTES));
         map.put("Repaired", Units.toString(validation.bytesRepaired, SizeUnit.BYTES));
         table.add(map);
      }

      String format = String.format("%%-%ds  %%-%ds  %%-%ds  %%-%ds  %%%ds  %%%ds  %%%ds  %%%ds  %%%ds%n", new Object[]{Integer.valueOf(columnWidth("Identifier", table)), Integer.valueOf(columnWidth("Table", table)), Integer.valueOf(columnWidth("Status", table)), Integer.valueOf(columnWidth("Outcome", table)), Integer.valueOf(columnWidth("Duration", table)), Integer.valueOf(columnWidth("ETA", table)), Integer.valueOf(columnWidth("Progress", table)), Integer.valueOf(columnWidth("Validated", table)), Integer.valueOf(columnWidth("Repaired", table))});
      System.out.println();
      System.out.printf(format, new Object[]{"Identifier", "Table", "Status", "Outcome", "Duration", "ETA", "Progress", "Validated", "Repaired"});
      System.out.println();
      table.forEach((m) -> {
         System.out.printf(format, new Object[]{m.get("Identifier"), m.get("Table"), m.get("Status"), m.get("Outcome"), m.get("Duration"), m.get("ETA"), m.get("Progress"), m.get("Validated"), m.get("Repaired")});
      });
      System.out.println();
   }

   private static int columnWidth(String columnName, List<Map<String, String>> list) {
      int maxValueLength = list.stream().map((m) -> {
         return (String)m.get(columnName);
      }).mapToInt(String::length).max().orElse(0);
      return Math.max(maxValueLength, columnName.length());
   }

   private static class Validation {
      private final ListValidations.Validation.Key key;
      private final UserValidationProposer.Status status;
      private final Map<String, Long> outcomes;
      private final Date startTime;
      private final Date endTime;
      private final long segmentsToValidate;
      private final long segmentsValidated;
      private final long bytesValidated;
      private final long bytesRepaired;

      private Validation(ListValidations.Validation.Key key, UserValidationProposer.Status status, Map<String, Long> outcomes, Date startTime, Date endTime, long segmentsToValidate, long segmentsValidated, long bytesValidated, long bytesRepaired) {
         this.key = key;
         this.status = status;
         this.outcomes = outcomes;
         this.startTime = startTime;
         this.endTime = endTime;
         this.segmentsToValidate = segmentsToValidate;
         this.segmentsValidated = segmentsValidated;
         this.bytesValidated = bytesValidated;
         this.bytesRepaired = bytesRepaired;
      }

      int progress() {
         int p = (int)((double)this.segmentsValidated / (double)this.segmentsToValidate * 100.0D);
         return Math.max(0, Math.min(100, p));
      }

      TimeValue duration() {
         long endMillis = this.status == UserValidationProposer.Status.RUNNING?System.currentTimeMillis():this.endTime.getTime();
         long durationMillis = this.startTime == null?0L:endMillis - this.startTime.getTime();
         return TimeValue.of(durationMillis, TimeUnit.MILLISECONDS);
      }

      String eta() {
         if(this.status != UserValidationProposer.Status.RUNNING) {
            return "-";
         } else {
            int progress = this.progress();
            if(progress == 0) {
               return "?";
            } else {
               long durationMillis = this.duration().in(TimeUnit.MILLISECONDS);
               long etaMillis = durationMillis * 100L / (long)progress - durationMillis;
               return TimeValue.of(etaMillis, TimeUnit.MILLISECONDS).toString();
            }
         }
      }

      private boolean hasOutcome(ValidationOutcome outcome) {
         Long value = (Long)this.outcomes.get(outcome.toString());
         return value != null && value.longValue() > 0L;
      }

      String summarizedOutcome() {
         return this.hasOutcome(ValidationOutcome.UNCOMPLETED)?"uncompleted":(this.hasOutcome(ValidationOutcome.FAILED)?"failed":(!this.hasOutcome(ValidationOutcome.PARTIAL_IN_SYNC) && !this.hasOutcome(ValidationOutcome.PARTIAL_REPAIRED)?"success":"partial"));
      }

      ListValidations.Validation combineWith(ListValidations.Validation other) {
         if(other == null) {
            return this;
         } else {
            assert this.key.equals(other.key);

            Date combinedStartTime;
            if(this.startTime == null) {
               combinedStartTime = other.startTime;
            } else if(other.startTime == null) {
               combinedStartTime = this.startTime;
            } else {
               combinedStartTime = this.startTime.before(other.startTime)?this.startTime:other.startTime;
            }

            Date combinedEndTime;
            if(this.endTime == null) {
               combinedEndTime = other.endTime;
            } else if(other.endTime == null) {
               combinedEndTime = this.endTime;
            } else {
               combinedEndTime = this.endTime.after(other.endTime)?this.endTime:other.endTime;
            }

            Map<String, Long> combinedOutcomes = new HashMap(this.outcomes);
            ValidationOutcome[] var5 = ValidationOutcome.values();
            int var6 = var5.length;

            for(int var7 = 0; var7 < var6; ++var7) {
               ValidationOutcome outcome = var5[var7];
               String key = outcome.toString();
               Long v1 = (Long)this.outcomes.get(key);
               Long v2 = (Long)other.outcomes.get(key);
               if(v1 != null && v2 != null) {
                  combinedOutcomes.put(key, Long.valueOf(v1.longValue() + v2.longValue()));
               } else if(v1 != null) {
                  combinedOutcomes.put(key, v1);
               } else if(v2 != null) {
                  combinedOutcomes.put(key, v2);
               }
            }

            return new ListValidations.Validation(this.key, this.status.combineWith(other.status), combinedOutcomes, combinedStartTime, combinedEndTime, this.segmentsToValidate + other.segmentsToValidate, this.segmentsValidated + other.segmentsValidated, this.bytesValidated + other.bytesValidated, this.bytesRepaired + other.bytesRepaired);
         }
      }

      private static ListValidations.Validation fromRow(Row row) {
         UDTValue metrics = row.getUDTValue("metrics");
         return new ListValidations.Validation(new ListValidations.Validation.Key(row.getString("id"), row.getString("keyspace_name"), row.getString("table_name")), UserValidationProposer.Status.from(row.getString("status")), row.getMap("outcomes", String.class, Long.class), row.getTimestamp("started_at"), row.getTimestamp("ended_at"), row.getLong("segments_to_validate"), row.getLong("segments_validated"), metrics == null?0L:metrics.getLong("data_validated"), metrics == null?0L:metrics.getLong("data_repaired"));
      }

      private static class Key {
         private final String id;
         private final String keyspace;
         private final String table;

         public Key(String id, String keyspace, String table) {
            this.id = id;
            this.keyspace = keyspace;
            this.table = table;
         }

         public boolean equals(Object o) {
            if(this == o) {
               return true;
            } else if(o != null && this.getClass() == o.getClass()) {
               ListValidations.Validation.Key that = (ListValidations.Validation.Key)o;
               return this.id.equals(that.id) && this.keyspace.equals(that.keyspace) && this.table.equals(that.table);
            } else {
               return false;
            }
         }

         public int hashCode() {
            return Objects.hash(new Object[]{this.id, this.keyspace, this.table});
         }
      }
   }
}
