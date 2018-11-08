package org.apache.cassandra.streaming;

import java.io.Serializable;
import java.net.InetAddress;
import java.util.Objects;

public class ProgressInfo implements Serializable {
   public final InetAddress peer;
   public final int sessionIndex;
   public final String fileName;
   public final ProgressInfo.Direction direction;
   public final long currentBytes;
   public final long totalBytes;

   public ProgressInfo(InetAddress peer, int sessionIndex, String fileName, ProgressInfo.Direction direction, long currentBytes, long totalBytes) {
      assert totalBytes > 0L;

      this.peer = peer;
      this.sessionIndex = sessionIndex;
      this.fileName = fileName;
      this.direction = direction;
      this.currentBytes = currentBytes;
      this.totalBytes = totalBytes;
   }

   public boolean isCompleted() {
      return this.currentBytes >= this.totalBytes;
   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         ProgressInfo that = (ProgressInfo)o;
         return this.totalBytes != that.totalBytes?false:(this.direction != that.direction?false:(!this.fileName.equals(that.fileName)?false:(this.sessionIndex != that.sessionIndex?false:this.peer.equals(that.peer))));
      } else {
         return false;
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{this.peer, Integer.valueOf(this.sessionIndex), this.fileName, this.direction, Long.valueOf(this.totalBytes)});
   }

   public String toString() {
      StringBuilder sb = new StringBuilder(this.fileName);
      sb.append(" ").append(this.currentBytes);
      sb.append("/").append(this.totalBytes).append(" bytes");
      sb.append("(").append(this.currentBytes * 100L / this.totalBytes).append("%) ");
      sb.append(this.direction == ProgressInfo.Direction.OUT?"sent to ":"received from ");
      sb.append("idx:").append(this.sessionIndex);
      sb.append(this.peer);
      return sb.toString();
   }

   public static enum Direction {
      OUT(0),
      IN(1);

      public final byte code;

      private Direction(int code) {
         this.code = (byte)code;
      }

      public static ProgressInfo.Direction fromByte(byte direction) {
         return direction == 0?OUT:IN;
      }
   }
}
