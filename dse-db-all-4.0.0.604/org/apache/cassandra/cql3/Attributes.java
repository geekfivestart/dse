package org.apache.cassandra.cql3;

import java.nio.ByteBuffer;
import java.util.List;
import org.apache.cassandra.cql3.functions.Function;
import org.apache.cassandra.db.ExpirationDateOverflowHandling;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.serializers.MarshalException;
import org.apache.cassandra.utils.ByteBufferUtil;

public class Attributes {
   public static final int MAX_TTL = 630720000;
   private final Term timestamp;
   private final Term timeToLive;

   public static Attributes none() {
      return new Attributes((Term)null, (Term)null);
   }

   private Attributes(Term timestamp, Term timeToLive) {
      this.timestamp = timestamp;
      this.timeToLive = timeToLive;
   }

   public void addFunctionsTo(List<Function> functions) {
      if(this.timestamp != null) {
         this.timestamp.addFunctionsTo(functions);
      }

      if(this.timeToLive != null) {
         this.timeToLive.addFunctionsTo(functions);
      }

   }

   public boolean isTimestampSet() {
      return this.timestamp != null;
   }

   public boolean isTimeToLiveSet() {
      return this.timeToLive != null;
   }

   public long getTimestamp(long now, QueryOptions options) throws InvalidRequestException {
      if(this.timestamp == null) {
         return now;
      } else {
         ByteBuffer tval = this.timestamp.bindAndGet(options);
         if(tval == null) {
            throw new InvalidRequestException("Invalid null value of timestamp");
         } else if(tval == ByteBufferUtil.UNSET_BYTE_BUFFER) {
            return now;
         } else {
            try {
               LongType.instance.validate(tval);
            } catch (MarshalException var6) {
               throw new InvalidRequestException("Invalid timestamp value: " + tval);
            }

            return ((Long)LongType.instance.compose(tval)).longValue();
         }
      }
   }

   public int getTimeToLive(QueryOptions options, TableMetadata metadata) throws InvalidRequestException {
      if(this.timeToLive == null) {
         ExpirationDateOverflowHandling.maybeApplyExpirationDateOverflowPolicy(metadata, metadata.params.defaultTimeToLive, true);
         return metadata.params.defaultTimeToLive;
      } else {
         ByteBuffer tval = this.timeToLive.bindAndGet(options);
         if(tval == null) {
            return 0;
         } else if(tval == ByteBufferUtil.UNSET_BYTE_BUFFER) {
            return metadata.params.defaultTimeToLive;
         } else {
            try {
               Int32Type.instance.validate(tval);
            } catch (MarshalException var5) {
               throw new InvalidRequestException("Invalid timestamp value: " + tval);
            }

            int ttl = ((Integer)Int32Type.instance.compose(tval)).intValue();
            if(ttl < 0) {
               throw new InvalidRequestException("A TTL must be greater or equal to 0, but was " + ttl);
            } else if(ttl > 630720000) {
               throw new InvalidRequestException(String.format("ttl is too large. requested (%d) maximum (%d)", new Object[]{Integer.valueOf(ttl), Integer.valueOf(630720000)}));
            } else if(metadata.params.defaultTimeToLive != 0 && ttl == 0) {
               return 0;
            } else {
               ExpirationDateOverflowHandling.maybeApplyExpirationDateOverflowPolicy(metadata, ttl, false);
               return ttl;
            }
         }
      }
   }

   public void collectMarkerSpecification(VariableSpecifications boundNames) {
      if(this.timestamp != null) {
         this.timestamp.collectMarkerSpecification(boundNames);
      }

      if(this.timeToLive != null) {
         this.timeToLive.collectMarkerSpecification(boundNames);
      }

   }

   public static class Raw {
      public Term.Raw timestamp;
      public Term.Raw timeToLive;

      public Raw() {
      }

      public Attributes prepare(String ksName, String cfName) throws InvalidRequestException {
         Term ts = this.timestamp == null?null:this.timestamp.prepare(ksName, this.timestampReceiver(ksName, cfName));
         Term ttl = this.timeToLive == null?null:this.timeToLive.prepare(ksName, this.timeToLiveReceiver(ksName, cfName));
         return new Attributes(ts, ttl);
      }

      private ColumnSpecification timestampReceiver(String ksName, String cfName) {
         return new ColumnSpecification(ksName, cfName, new ColumnIdentifier("[timestamp]", true), LongType.instance);
      }

      private ColumnSpecification timeToLiveReceiver(String ksName, String cfName) {
         return new ColumnSpecification(ksName, cfName, new ColumnIdentifier("[ttl]", true), Int32Type.instance);
      }
   }
}
