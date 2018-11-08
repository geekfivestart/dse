package org.apache.cassandra.tools.nodetool.formatter;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import javax.annotation.Nonnull;

public class TableBuilder {
   private final char columnDelimiter;
   private int[] maximumColumnWidth;
   private final List<String[]> rows;

   public TableBuilder() {
      this(' ');
   }

   public TableBuilder(char columnDelimiter) {
      this.rows = new ArrayList();
      this.columnDelimiter = columnDelimiter;
   }

   public void add(@Nonnull String... row) {
      Objects.requireNonNull(row);
      if(this.rows.isEmpty()) {
         this.maximumColumnWidth = new int[row.length];
      }

      if(row.length > this.maximumColumnWidth.length) {
         this.maximumColumnWidth = Arrays.copyOf(this.maximumColumnWidth, row.length);
      }

      int i = 0;
      String[] var3 = row;
      int var4 = row.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         String col = var3[var5];
         this.maximumColumnWidth[i] = Math.max(this.maximumColumnWidth[i], col != null?col.length():1);
         ++i;
      }

      this.rows.add(row);
   }

   public void printTo(PrintStream out) {
      if(!this.rows.isEmpty()) {
         Iterator var2 = this.rows.iterator();

         while(var2.hasNext()) {
            String[] row = (String[])var2.next();

            for(int i = 0; i < this.maximumColumnWidth.length; ++i) {
               String col = i < row.length?row[i]:"";
               out.print(String.format("%-" + this.maximumColumnWidth[i] + 's', new Object[]{col != null?col:""}));
               if(i < this.maximumColumnWidth.length - 1) {
                  out.print(this.columnDelimiter);
               }
            }

            out.println();
         }

      }
   }
}
