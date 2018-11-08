package org.apache.cassandra.hadoop;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.EOFException;
import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.InputSplit;

public class ColumnFamilySplit extends InputSplit implements Writable, org.apache.hadoop.mapred.InputSplit {
   private String startToken;
   private String endToken;
   private long length;
   private String[] dataNodes;

   /** @deprecated */
   @Deprecated
   public ColumnFamilySplit(String startToken, String endToken, String[] dataNodes) {
      this(startToken, endToken, 9223372036854775807L, dataNodes);
   }

   public ColumnFamilySplit(String startToken, String endToken, long length, String[] dataNodes) {
      assert startToken != null;

      assert endToken != null;

      this.startToken = startToken;
      this.endToken = endToken;
      this.length = length;
      this.dataNodes = dataNodes;
   }

   public String getStartToken() {
      return this.startToken;
   }

   public String getEndToken() {
      return this.endToken;
   }

   public long getLength() {
      return this.length;
   }

   public String[] getLocations() {
      return this.dataNodes;
   }

   protected ColumnFamilySplit() {
   }

   public void write(DataOutput out) throws IOException {
      out.writeUTF(this.startToken);
      out.writeUTF(this.endToken);
      out.writeInt(this.dataNodes.length);
      String[] var2 = this.dataNodes;
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         String endpoint = var2[var4];
         out.writeUTF(endpoint);
      }

      out.writeLong(this.length);
   }

   public void readFields(DataInput in) throws IOException {
      this.startToken = in.readUTF();
      this.endToken = in.readUTF();
      int numOfEndpoints = in.readInt();
      this.dataNodes = new String[numOfEndpoints];

      for(int i = 0; i < numOfEndpoints; ++i) {
         this.dataNodes[i] = in.readUTF();
      }

      try {
         this.length = in.readLong();
      } catch (EOFException var4) {
         ;
      }

   }

   public String toString() {
      return "ColumnFamilySplit((" + this.startToken + ", '" + this.endToken + ']' + " @" + (this.dataNodes == null?null:Arrays.asList(this.dataNodes)) + ')';
   }

   public static ColumnFamilySplit read(DataInput in) throws IOException {
      ColumnFamilySplit w = new ColumnFamilySplit();
      w.readFields(in);
      return w;
   }
}
