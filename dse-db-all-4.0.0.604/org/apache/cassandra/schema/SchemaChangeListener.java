package org.apache.cassandra.schema;

import java.util.List;
import org.apache.cassandra.db.marshal.AbstractType;

public abstract class SchemaChangeListener {
   public SchemaChangeListener() {
   }

   public void onCreateKeyspace(String keyspace) {
   }

   public void onCreateTable(String keyspace, String table) {
   }

   public void onCreateView(String keyspace, String view) {
      this.onCreateTable(keyspace, view);
   }

   public void onCreateType(String keyspace, String type) {
   }

   public void onCreateFunction(String keyspace, String function, List<AbstractType<?>> argumentTypes) {
   }

   public void onCreateAggregate(String keyspace, String aggregate, List<AbstractType<?>> argumentTypes) {
   }

   public void onAlterKeyspace(String keyspace) {
   }

   public void onAlterTable(String keyspace, String table, boolean affectsStatements) {
   }

   public void onAlterView(String keyspace, String view, boolean affectsStataments) {
      this.onAlterTable(keyspace, view, affectsStataments);
   }

   public void onAlterType(String keyspace, String type) {
   }

   public void onAlterFunction(String keyspace, String function, List<AbstractType<?>> argumentTypes) {
   }

   public void onAlterAggregate(String keyspace, String aggregate, List<AbstractType<?>> argumentTypes) {
   }

   public void onDropKeyspace(String keyspace) {
   }

   public void onDropTable(String keyspace, String table) {
   }

   public void onDropView(String keyspace, String view) {
      this.onDropTable(keyspace, view);
   }

   public void onDropType(String keyspace, String type) {
   }

   public void onDropFunction(String keyspace, String function, List<AbstractType<?>> argumentTypes) {
   }

   public void onDropAggregate(String keyspace, String aggregate, List<AbstractType<?>> argumentTypes) {
   }
}
