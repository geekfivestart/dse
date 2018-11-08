package org.apache.cassandra.cql3;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.codehaus.jackson.io.JsonStringEncoder;
import org.codehaus.jackson.map.ObjectMapper;

public class Json {
   public static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();
   public static final ColumnIdentifier JSON_COLUMN_ID = new ColumnIdentifier("[json]", true);

   public Json() {
   }

   public static String quoteAsJsonString(String s) {
      return new String(JsonStringEncoder.getInstance().quoteAsString(s));
   }

   public static Object decodeJson(String json) {
      try {
         return JSON_OBJECT_MAPPER.readValue(json, Object.class);
      } catch (IOException var2) {
         throw new MarshalException("Error decoding JSON string: " + var2.getMessage());
      }
   }

   public static Map<ColumnIdentifier, Term> parseJson(String jsonString, Collection<ColumnMetadata> expectedReceivers) {
      try {
         Map<String, Object> valueMap = (Map)JSON_OBJECT_MAPPER.readValue(jsonString, Map.class);
         if(valueMap == null) {
            throw new InvalidRequestException("Got null for INSERT JSON values");
         } else {
            handleCaseSensitivity(valueMap);
            Map<ColumnIdentifier, Term> columnMap = new HashMap(expectedReceivers.size());
            Iterator var4 = expectedReceivers.iterator();

            while(var4.hasNext()) {
               ColumnSpecification spec = (ColumnSpecification)var4.next();
               if(valueMap.containsKey(spec.name.toString())) {
                  Object parsedJsonObject = valueMap.remove(spec.name.toString());
                  if(parsedJsonObject == null) {
                     columnMap.put(spec.name, Constants.NULL_VALUE);
                  } else {
                     try {
                        columnMap.put(spec.name, spec.type.fromJSONObject(parsedJsonObject));
                     } catch (MarshalException var8) {
                        throw new InvalidRequestException(String.format("Error decoding JSON value for %s: %s", new Object[]{spec.name, var8.getMessage()}));
                     }
                  }
               }
            }

            if(!valueMap.isEmpty()) {
               throw new InvalidRequestException(String.format("JSON values map contains unrecognized column: %s", new Object[]{valueMap.keySet().iterator().next()}));
            } else {
               return columnMap;
            }
         }
      } catch (IOException var9) {
         throw new InvalidRequestException(String.format("Could not decode JSON string as a map: %s. (String was: %s)", new Object[]{var9.toString(), jsonString}));
      } catch (MarshalException var10) {
         throw new InvalidRequestException(var10.getMessage());
      }
   }

   public static void handleCaseSensitivity(Map<String, Object> valueMap) {
      Iterator var1 = (new ArrayList(valueMap.keySet())).iterator();

      while(true) {
         while(var1.hasNext()) {
            String mapKey = (String)var1.next();
            if(mapKey.startsWith("\"") && mapKey.endsWith("\"")) {
               valueMap.put(mapKey.substring(1, mapKey.length() - 1), valueMap.remove(mapKey));
            } else {
               String lowered = mapKey.toLowerCase(Locale.US);
               if(!mapKey.equals(lowered)) {
                  valueMap.put(lowered, valueMap.remove(mapKey));
               }
            }
         }

         return;
      }
   }

   private static class DelayedColumnValue extends Term.NonTerminal {
      private final Json.PreparedMarker marker;
      private final ColumnMetadata column;
      private final boolean defaultUnset;

      public DelayedColumnValue(Json.PreparedMarker prepared, ColumnMetadata column, boolean defaultUnset) {
         this.marker = prepared;
         this.column = column;
         this.defaultUnset = defaultUnset;
      }

      public void collectMarkerSpecification(VariableSpecifications boundNames) {
      }

      public boolean containsBindMarker() {
         return true;
      }

      public Term.Terminal bind(QueryOptions options) throws InvalidRequestException {
         Term term = options.getJsonColumnValue(this.marker.bindIndex, this.column.name, this.marker.columns);
         return (Term.Terminal)(term == null?(this.defaultUnset?Constants.UNSET_VALUE:null):term.bind(options));
      }

      public void addFunctionsTo(List<Function> functions) {
      }
   }

   private static class RawDelayedColumnValue extends Term.Raw {
      private final Json.PreparedMarker marker;
      private final ColumnMetadata column;
      private final boolean defaultUnset;

      public RawDelayedColumnValue(Json.PreparedMarker prepared, ColumnMetadata column, boolean defaultUnset) {
         this.marker = prepared;
         this.column = column;
         this.defaultUnset = defaultUnset;
      }

      public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
         return new Json.DelayedColumnValue(this.marker, this.column, this.defaultUnset);
      }

      public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
         return AssignmentTestable.TestResult.WEAKLY_ASSIGNABLE;
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return null;
      }

      public String getText() {
         return this.marker.toString();
      }
   }

   private static class ColumnValue extends Term.Raw {
      private final Term term;

      public ColumnValue(Term term) {
         this.term = term;
      }

      public Term prepare(String keyspace, ColumnSpecification receiver) throws InvalidRequestException {
         return this.term;
      }

      public AssignmentTestable.TestResult testAssignment(String keyspace, ColumnSpecification receiver) {
         return AssignmentTestable.TestResult.NOT_ASSIGNABLE;
      }

      public AbstractType<?> getExactTypeIfKnown(String keyspace) {
         return null;
      }

      public String getText() {
         return this.term.toString();
      }
   }

   private static class PreparedMarker extends Json.Prepared {
      private final int bindIndex;
      private final Collection<ColumnMetadata> columns;

      public PreparedMarker(int bindIndex, Collection<ColumnMetadata> columns) {
         this.bindIndex = bindIndex;
         this.columns = columns;
      }

      public Json.RawDelayedColumnValue getRawTermForColumn(ColumnMetadata def, boolean defaultUnset) {
         return new Json.RawDelayedColumnValue(this, def, defaultUnset);
      }
   }

   private static class PreparedLiteral extends Json.Prepared {
      private final Map<ColumnIdentifier, Term> columnMap;

      public PreparedLiteral(Map<ColumnIdentifier, Term> columnMap) {
         this.columnMap = columnMap;
      }

      public Term.Raw getRawTermForColumn(ColumnMetadata def, boolean defaultUnset) {
         Term value = (Term)this.columnMap.get(def.name);
         return (Term.Raw)(value == null?(defaultUnset?Constants.UNSET_LITERAL:Constants.NULL_LITERAL):new Json.ColumnValue(value));
      }
   }

   public abstract static class Prepared {
      public Prepared() {
      }

      public abstract Term.Raw getRawTermForColumn(ColumnMetadata var1, boolean var2);
   }

   public static class Marker implements Json.Raw {
      protected final int bindIndex;

      public Marker(int bindIndex) {
         this.bindIndex = bindIndex;
      }

      public Json.Prepared prepareAndCollectMarkers(TableMetadata metadata, Collection<ColumnMetadata> receivers, VariableSpecifications boundNames) {
         boundNames.add(this.bindIndex, this.makeReceiver(metadata));
         return new Json.PreparedMarker(this.bindIndex, receivers);
      }

      private ColumnSpecification makeReceiver(TableMetadata metadata) {
         return new ColumnSpecification(metadata.keyspace, metadata.name, Json.JSON_COLUMN_ID, UTF8Type.instance);
      }
   }

   public static class Literal implements Json.Raw {
      private final String text;

      public Literal(String text) {
         this.text = text;
      }

      public Json.Prepared prepareAndCollectMarkers(TableMetadata metadata, Collection<ColumnMetadata> receivers, VariableSpecifications boundNames) {
         return new Json.PreparedLiteral(Json.parseJson(this.text, receivers));
      }
   }

   public interface Raw {
      Json.Prepared prepareAndCollectMarkers(TableMetadata var1, Collection<ColumnMetadata> var2, VariableSpecifications var3);
   }
}
