package org.apache.cassandra.index;

import com.google.common.collect.UnmodifiableIterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.cassandra.cql3.ColumnIdentifier;
import org.apache.cassandra.cql3.statements.IndexTarget;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang3.StringUtils;

public class TargetParser {
   private static final Pattern TARGET_REGEX = Pattern.compile("^(keys|entries|values|full)\\((.+)\\)$");
   private static final Pattern TWO_QUOTES = Pattern.compile("\"\"");
   private static final String QUOTE = "\"";

   public TargetParser() {
   }

   public static Pair<ColumnMetadata, IndexTarget.Type> parse(TableMetadata metadata, IndexMetadata indexDef) {
      String target = (String)indexDef.options.get("target");

      assert target != null : String.format("No target definition found for index %s", new Object[]{indexDef.name});

      Pair<ColumnMetadata, IndexTarget.Type> result = parse(metadata, target);
      if(result == null) {
         throw new ConfigurationException(String.format("Unable to parse targets for index %s (%s)", new Object[]{indexDef.name, target}));
      } else {
         return result;
      }
   }

   public static Pair<ColumnMetadata, IndexTarget.Type> parse(TableMetadata metadata, String target) {
      Matcher matcher = TARGET_REGEX.matcher(target);
      String columnName;
      IndexTarget.Type targetType;
      if(matcher.matches()) {
         targetType = IndexTarget.Type.fromString(matcher.group(1));
         columnName = matcher.group(2);
      } else {
         columnName = target;
         targetType = IndexTarget.Type.VALUES;
      }

      if(columnName.startsWith("\"")) {
         columnName = StringUtils.substring(StringUtils.substring(columnName, 1), 0, -1);
         columnName = TWO_QUOTES.matcher(columnName).replaceAll("\"");
      }

      ColumnMetadata cd = metadata.getColumn(new ColumnIdentifier(columnName, true));
      if(cd != null) {
         return Pair.create(cd, targetType);
      } else {
         UnmodifiableIterator var6 = metadata.columns().iterator();

         ColumnMetadata column;
         do {
            if(!var6.hasNext()) {
               return null;
            }

            column = (ColumnMetadata)var6.next();
         } while(!column.name.toString().equals(columnName));

         return Pair.create(column, targetType);
      }
   }
}
