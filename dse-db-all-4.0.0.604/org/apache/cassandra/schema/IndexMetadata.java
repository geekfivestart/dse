package org.apache.cassandra.schema;

import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.UnknownIndexException;
import org.apache.cassandra.index.Index;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class IndexMetadata {
   private static final Logger logger = LoggerFactory.getLogger(IndexMetadata.class);
   private static final Pattern PATTERN_NON_WORD_CHAR = Pattern.compile("\\W");
   private static final Pattern PATTERN_WORD_CHARS = Pattern.compile("\\w+");
   public static final IndexMetadata.Serializer serializer = new IndexMetadata.Serializer();
   public final UUID id;
   public final String name;
   public final IndexMetadata.Kind kind;
   public final Map<String, String> options;

   private IndexMetadata(String name, Map<String, String> options, IndexMetadata.Kind kind) {
      this.id = UUID.nameUUIDFromBytes(name.getBytes());
      this.name = name;
      this.options = options == null?ImmutableMap.of():ImmutableMap.copyOf(options);
      this.kind = kind;
   }

   public static IndexMetadata fromSchemaMetadata(String name, IndexMetadata.Kind kind, Map<String, String> options) {
      return new IndexMetadata(name, options, kind);
   }

   public static IndexMetadata fromIndexTargets(List<IndexTarget> targets, String name, IndexMetadata.Kind kind, Map<String, String> options) {
      Map<String, String> newOptions = new HashMap(options);
      newOptions.put("target", targets.stream().map((target) -> {
         return target.asCqlString();
      }).collect(Collectors.joining(", ")));
      return new IndexMetadata(name, newOptions, kind);
   }

   public static boolean isNameValid(String name) {
      return name != null && !name.isEmpty() && PATTERN_WORD_CHARS.matcher(name).matches();
   }

   public static String getDefaultIndexName(String cfName, String root) {
      return root == null?PATTERN_NON_WORD_CHAR.matcher(cfName + "_idx").replaceAll(""):PATTERN_NON_WORD_CHAR.matcher(cfName + "_" + root + "_idx").replaceAll("");
   }

   public void validate(TableMetadata table) {
      if(!isNameValid(this.name)) {
         throw new ConfigurationException("Illegal index name " + this.name);
      } else if(this.kind == null) {
         throw new ConfigurationException("Index kind is null for index " + this.name);
      } else {
         if(this.kind == IndexMetadata.Kind.CUSTOM) {
            if(this.options == null || !this.options.containsKey("class_name")) {
               throw new ConfigurationException(String.format("Required option missing for index %s : %s", new Object[]{this.name, "class_name"}));
            }

            String className = (String)this.options.get("class_name");
            Class<Index> indexerClass = FBUtilities.classForName(className, "custom indexer");
            if(!Index.class.isAssignableFrom(indexerClass)) {
               throw new ConfigurationException(String.format("Specified Indexer class (%s) does not implement the Indexer interface", new Object[]{className}));
            }

            this.validateCustomIndexOptions(table, indexerClass, this.options);
         }

      }
   }

   private void validateCustomIndexOptions(TableMetadata table, Class<? extends Index> indexerClass, Map<String, String> options) {
      try {
         Map<String, String> filteredOptions = Maps.filterKeys(options, (key) -> {
            return !key.equals("class_name");
         });
         if(filteredOptions.isEmpty()) {
            return;
         }

         Map unknownOptions;
         try {
            unknownOptions = (Map)indexerClass.getMethod("validateOptions", new Class[]{Map.class, TableMetadata.class}).invoke((Object)null, new Object[]{filteredOptions, table});
         } catch (NoSuchMethodException var7) {
            unknownOptions = (Map)indexerClass.getMethod("validateOptions", new Class[]{Map.class}).invoke((Object)null, new Object[]{filteredOptions});
         }

         if(!unknownOptions.isEmpty()) {
            throw new ConfigurationException(String.format("Properties specified %s are not understood by %s", new Object[]{unknownOptions.keySet(), indexerClass.getSimpleName()}));
         }
      } catch (NoSuchMethodException var8) {
         logger.info("Indexer {} does not have a static validateOptions method. Validation ignored", indexerClass.getName());
      } catch (InvocationTargetException var9) {
         if(var9.getTargetException() instanceof ConfigurationException) {
            throw (ConfigurationException)var9.getTargetException();
         }

         throw new ConfigurationException("Failed to validate custom indexer options: " + options);
      } catch (ConfigurationException var10) {
         throw var10;
      } catch (Exception var11) {
         throw new ConfigurationException("Failed to validate custom indexer options: " + options);
      }

   }

   public boolean isCustom() {
      return this.kind == IndexMetadata.Kind.CUSTOM;
   }

   public boolean isKeys() {
      return this.kind == IndexMetadata.Kind.KEYS;
   }

   public boolean isComposites() {
      return this.kind == IndexMetadata.Kind.COMPOSITES;
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.id, this.name, this.kind, this.options});
   }

   public boolean equalsWithoutName(IndexMetadata other) {
      return Objects.equals(this.kind, other.kind) && Objects.equals(this.options, other.options);
   }

   public boolean equals(Object obj) {
      if(obj == this) {
         return true;
      } else if(!(obj instanceof IndexMetadata)) {
         return false;
      } else {
         IndexMetadata other = (IndexMetadata)obj;
         return Objects.equals(this.id, other.id) && Objects.equals(this.name, other.name) && this.equalsWithoutName(other);
      }
   }

   public String toString() {
      return (new ToStringBuilder(this)).append("id", this.id.toString()).append("name", this.name).append("kind", this.kind).append("options", this.options).build();
   }

   public String toCQLString() {
      return ColumnIdentifier.maybeQuote(this.name);
   }

   public static class Serializer {
      public Serializer() {
      }

      public void serialize(IndexMetadata metadata, DataOutputPlus out) throws IOException {
         UUIDSerializer.serializer.serialize(metadata.id, out);
      }

      public IndexMetadata deserialize(DataInputPlus in, TableMetadata table) throws IOException {
         UUID id = UUIDSerializer.serializer.deserialize(in);
         return (IndexMetadata)table.indexes.get(id).orElseThrow(() -> {
            return new UnknownIndexException(table, id);
         });
      }

      public long serializedSize(IndexMetadata metadata) {
         return UUIDSerializer.serializer.serializedSize(metadata.id);
      }
   }

   public static enum Kind {
      KEYS,
      CUSTOM,
      COMPOSITES;

      private Kind() {
      }
   }
}
