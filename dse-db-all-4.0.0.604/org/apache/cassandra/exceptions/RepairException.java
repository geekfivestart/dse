package org.apache.cassandra.exceptions;

import org.apache.cassandra.repair.RepairJobDesc;
import org.apache.cassandra.streaming.PreviewKind;

public class RepairException extends Exception {
   public final RepairJobDesc desc;
   public final PreviewKind previewKind;

   public RepairException(RepairJobDesc desc, String message) {
      this(desc, (PreviewKind)null, message);
   }

   public RepairException(RepairJobDesc desc, PreviewKind previewKind, String message) {
      super(message);
      this.desc = desc;
      this.previewKind = previewKind != null?previewKind:PreviewKind.NONE;
   }

   public String getMessage() {
      return this.desc.toString(this.previewKind) + ' ' + super.getMessage();
   }
}
