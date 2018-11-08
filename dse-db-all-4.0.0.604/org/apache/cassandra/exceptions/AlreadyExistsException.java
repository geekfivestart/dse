package org.apache.cassandra.exceptions;

public class AlreadyExistsException extends ConfigurationException {
   public final String ksName;
   public final String cfName;

   public AlreadyExistsException(String ksName, String cfName, String msg) {
      super(ExceptionCode.ALREADY_EXISTS, msg);
      this.ksName = ksName;
      this.cfName = cfName;
   }

   public AlreadyExistsException(String ksName, String cfName) {
      this(ksName, cfName, String.format("Cannot add already existing table \"%s\" to keyspace \"%s\"", new Object[]{cfName, ksName}));
   }

   public AlreadyExistsException(String ksName) {
      this(ksName, "", String.format("Cannot add existing keyspace \"%s\"", new Object[]{ksName}));
   }
}
