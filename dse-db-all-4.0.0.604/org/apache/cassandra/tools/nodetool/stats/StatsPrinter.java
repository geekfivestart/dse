package org.apache.cassandra.tools.nodetool.stats;

import java.io.PrintStream;
import org.json.simple.JSONObject;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.DumperOptions.FlowStyle;

public interface StatsPrinter<T extends StatsHolder> {
   void print(T var1, PrintStream var2);

   public static class YamlPrinter<T extends StatsHolder> implements StatsPrinter<T> {
      public YamlPrinter() {
      }

      public void print(T data, PrintStream out) {
         DumperOptions options = new DumperOptions();
         options.setDefaultFlowStyle(FlowStyle.BLOCK);
         Yaml yaml = new Yaml(options);
         out.println(yaml.dump(data.convert2Map()));
      }
   }

   public static class JsonPrinter<T extends StatsHolder> implements StatsPrinter<T> {
      public JsonPrinter() {
      }

      public void print(T data, PrintStream out) {
         JSONObject json = new JSONObject();
         json.putAll(data.convert2Map());
         out.println(json.toString());
      }
   }
}
