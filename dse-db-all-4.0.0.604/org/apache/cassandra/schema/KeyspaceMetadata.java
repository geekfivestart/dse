package org.apache.cassandra.schema;

import com.google.common.base.MoreObjects;
import com.google.common.collect.Iterables;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Consumer;
import javax.annotation.Nullable;
import org.apache.cassandra.exceptions.ConfigurationException;

public final class KeyspaceMetadata {
   public final String name;
   public final KeyspaceParams params;
   public final Tables tables;
   public final Views views;
   public final Types types;
   public final Functions functions;

   private KeyspaceMetadata(String name, KeyspaceParams params, Tables tables, Views views, Types types, Functions functions) {
      this.name = name;
      this.params = params;
      this.tables = tables;
      this.views = views;
      this.types = types;
      this.functions = functions;
   }

   public static KeyspaceMetadata create(String name, KeyspaceParams params) {
      return new KeyspaceMetadata(name, params, Tables.none(), Views.none(), Types.none(), Functions.none());
   }

   public static KeyspaceMetadata create(String name, KeyspaceParams params, Tables tables) {
      return new KeyspaceMetadata(name, params, tables, Views.none(), Types.none(), Functions.none());
   }

   public static KeyspaceMetadata create(String name, KeyspaceParams params, Tables tables, Views views, Types types, Functions functions) {
      return new KeyspaceMetadata(name, params, tables, views, types, functions);
   }

   public KeyspaceMetadata withSwapped(KeyspaceParams params) {
      return new KeyspaceMetadata(this.name, params, this.tables, this.views, this.types, this.functions);
   }

   public KeyspaceMetadata withSwapped(Tables regular) {
      return new KeyspaceMetadata(this.name, this.params, regular, this.views, this.types, this.functions);
   }

   public KeyspaceMetadata withSwapped(Views views) {
      return new KeyspaceMetadata(this.name, this.params, this.tables, views, this.types, this.functions);
   }

   public KeyspaceMetadata withSwapped(Types types) {
      return new KeyspaceMetadata(this.name, this.params, this.tables, this.views, types, this.functions);
   }

   public KeyspaceMetadata withSwapped(Functions functions) {
      return new KeyspaceMetadata(this.name, this.params, this.tables, this.views, this.types, functions);
   }

   public Iterable<TableMetadata> tablesAndViews() {
      return Iterables.concat(this.tables, this.views.metadatas());
   }

   @Nullable
   public TableMetadata getTableOrViewNullable(String tableOrViewName) {
      ViewMetadata view = this.views.getNullable(tableOrViewName);
      return view == null?this.tables.getNullable(tableOrViewName):view.metadata;
   }

   public Set<String> existingIndexNames(String cfToExclude) {
      Set<String> indexNames = new HashSet();
      Iterator var3 = this.tables.iterator();

      while(true) {
         TableMetadata table;
         do {
            if(!var3.hasNext()) {
               return indexNames;
            }

            table = (TableMetadata)var3.next();
         } while(cfToExclude != null && table.name.equals(cfToExclude));

         Iterator var5 = table.indexes.iterator();

         while(var5.hasNext()) {
            IndexMetadata index = (IndexMetadata)var5.next();
            indexNames.add(index.name);
         }
      }
   }

   public Optional<TableMetadata> findIndexedTable(String indexName) {
      Iterator var2 = this.tablesAndViews().iterator();

      TableMetadata table;
      do {
         if(!var2.hasNext()) {
            return Optional.empty();
         }

         table = (TableMetadata)var2.next();
      } while(!table.indexes.has(indexName));

      return Optional.of(table);
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.name, this.params, this.tables, this.views, this.functions, this.types});
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(!(o instanceof KeyspaceMetadata)) {
         return false;
      } else {
         KeyspaceMetadata other = (KeyspaceMetadata)o;
         return this.name.equals(other.name) && this.params.equals(other.params) && this.tables.equals(other.tables) && this.views.equals(other.views) && this.functions.equals(other.functions) && this.types.equals(other.types);
      }
   }

   public String toString() {
      return MoreObjects.toStringHelper(this).add("name", this.name).add("params", this.params).add("tables", this.tables).add("views", this.views).add("functions", this.functions).add("types", this.types).toString();
   }

   public void validate() {
      if(!SchemaConstants.isValidName(this.name)) {
         throw new ConfigurationException(String.format("Keyspace name must not be empty, more than %s characters long, or contain non-alphanumeric-underscore characters (got \"%s\")", new Object[]{Integer.valueOf(48), this.name}));
      } else {
         this.params.validate(this.name);
         this.tablesAndViews().forEach(TableMetadata::validate);
         Set<String> indexNames = new HashSet();
         Iterator var2 = this.tables.iterator();

         while(var2.hasNext()) {
            TableMetadata table = (TableMetadata)var2.next();
            Iterator var4 = table.indexes.iterator();

            while(var4.hasNext()) {
               IndexMetadata index = (IndexMetadata)var4.next();
               if(indexNames.contains(index.name)) {
                  throw new ConfigurationException(String.format("Duplicate index name %s in keyspace %s", new Object[]{index.name, this.name}));
               }

               indexNames.add(index.name);
            }
         }

      }
   }
}
