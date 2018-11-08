package com.datastax.bdp.db.nodesync;

import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.UnmodifiableIterator;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import javax.annotation.Nullable;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.io.util.DataInputPlus;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.schema.KeyspaceMetadata;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.Serializer;
import org.apache.cassandra.utils.Streams;
import org.apache.cassandra.utils.versioning.VersionDependent;
import org.apache.cassandra.utils.versioning.Versioned;

public class UserValidationOptions {
   private static final Splitter ON_COMMA = Splitter.on(',').omitEmptyStrings().trimResults();
   private static final Splitter ON_COLON = Splitter.on(':').omitEmptyStrings().trimResults();
   public static final Versioned<NodeSyncVerbs.NodeSyncVersion, Serializer<UserValidationOptions>> serializers = NodeSyncVerbs.NodeSyncVersion.versioned(UserValidationOptions.OptionSerializer::<init>);
   static final String ID = "id";
   static final String KEYSPACE_NAME = "keyspace";
   static final String TABLE_NAME = "table";
   static final String REQUESTED_RANGES = "ranges";
   static final String RATE_IN_KB = "rate";
   final UserValidationID id;
   final TableMetadata table;
   @Nullable
   final ImmutableList<Range<Token>> validatedRanges;
   @Nullable
   final Integer rateInKB;

   UserValidationOptions(UserValidationID id, TableMetadata table, Collection<Range<Token>> validatedRanges, Integer rateInKB) {
      assert validatedRanges == null || !validatedRanges.isEmpty();

      this.id = id;
      this.table = table;
      this.validatedRanges = validatedRanges == null?null:ImmutableList.copyOf(Range.normalize(validatedRanges));
      this.rateInKB = rateInKB;
   }

   public static UserValidationOptions fromMap(Map<String, String> optionMap) {
      String id = (String)optionMap.get("id");
      if(id == null) {
         throw new IllegalArgumentException("Missing mandatory option id");
      } else {
         String ksName = (String)optionMap.get("keyspace");
         if(ksName == null) {
            throw new IllegalArgumentException("Missing mandatory option keyspace");
         } else {
            KeyspaceMetadata keyspace = Schema.instance.getKeyspaceMetadata(ksName);
            if(keyspace == null) {
               throw new IllegalArgumentException("Unknown keyspace " + ksName);
            } else {
               String tableName = (String)optionMap.get("table");
               if(tableName == null) {
                  throw new IllegalArgumentException("Missing mandatory option table");
               } else {
                  TableMetadata table = keyspace.getTableOrViewNullable(tableName);
                  if(table == null) {
                     throw new IllegalArgumentException("Unknown table " + tableName);
                  } else {
                     String rangesStr = (String)optionMap.get("ranges");
                     Collection<Range<Token>> ranges = null;
                     if(rangesStr != null) {
                        ranges = parseTokenRanges(rangesStr, table.partitioner);
                        if(ranges.isEmpty()) {
                           throw new IllegalArgumentException("Invalid empty list of ranges to validate (if you want to validate all local ranges, do not specify the ranges option)");
                        }
                     }

                     String rateStr = (String)optionMap.get("rate");
                     Integer rateInKB = null;
                     if(rateStr != null) {
                        try {
                           rateInKB = Integer.valueOf(Integer.parseInt(rateStr));
                        } catch (NumberFormatException var11) {
                           throw new IllegalArgumentException(String.format("Cannot parse %s option: got %s but expected a positive integer", new Object[]{"rate", rangesStr}));
                        }

                        if(rateInKB.intValue() <= 0) {
                           throw new IllegalArgumentException("rate option must be positive");
                        }
                     }

                     return new UserValidationOptions(UserValidationID.from(id), table, ranges, rateInKB);
                  }
               }
            }
         }
      }
   }

   private static Collection<Range<Token>> parseTokenRanges(String str, IPartitioner partitioner) {
      Token.TokenFactory tkFactory = partitioner.getTokenFactory();
      return (Collection)Streams.of(ON_COMMA.split(str)).map((s) -> {
         return parseTokenRange(s, tkFactory);
      }).collect(Collectors.toList());
   }

   private static Range<Token> parseTokenRange(String str, Token.TokenFactory tkFactory) {
      List<String> l = ON_COLON.splitToList(str);
      if(l.size() != 2) {
         throw new IllegalArgumentException("Invalid range definition provided: got " + str + " but expected a range of the form <start>:<end>");
      } else {
         return new Range(parseToken((String)l.get(0), tkFactory), parseToken((String)l.get(1), tkFactory));
      }
   }

   private static Token parseToken(String tk, Token.TokenFactory tkFactory) {
      try {
         return tkFactory.fromString(tk);
      } catch (ConfigurationException var3) {
         throw new IllegalArgumentException("Invalid token " + tk);
      }
   }

   public static class OptionSerializer extends VersionDependent<NodeSyncVerbs.NodeSyncVersion> implements Serializer<UserValidationOptions> {
      public OptionSerializer(NodeSyncVerbs.NodeSyncVersion version) {
         super(version);
      }

      public void serialize(UserValidationOptions options, DataOutputPlus out) throws IOException {
         UserValidationID.serializer.serialize(options.id, out);
         options.table.id.serialize(out);
         List<Range<Token>> ranges = options.validatedRanges;
         out.writeInt(ranges == null?-1:ranges.size());
         if(ranges != null) {
            Iterator var4 = ranges.iterator();

            while(var4.hasNext()) {
               Range<Token> range = (Range)var4.next();
               Range.tokenSerializer.serialize(range, out, ((NodeSyncVerbs.NodeSyncVersion)this.version).boundsVersion);
            }
         }

         out.writeInt(options.rateInKB == null?-1:options.rateInKB.intValue());
      }

      public UserValidationOptions deserialize(DataInputPlus in) throws IOException {
         UserValidationID id = (UserValidationID)UserValidationID.serializer.deserialize(in);
         TableMetadata table = Schema.instance.getTableMetadata(TableId.deserialize(in));
         List<Range<Token>> ranges = null;
         int rangesSize = in.readInt();
         if(rangesSize >= 0) {
            ranges = new ArrayList(rangesSize);
            IPartitioner partitioner = DatabaseDescriptor.getPartitioner();

            for(int i = 0; i < rangesSize; ++i) {
               ranges.add((Range)Range.tokenSerializer.deserialize(in, partitioner, ((NodeSyncVerbs.NodeSyncVersion)this.version).boundsVersion));
            }
         }

         Integer rateInKB = Integer.valueOf(in.readInt());
         if(rateInKB.intValue() < 0) {
            rateInKB = null;
         }

         return new UserValidationOptions(id, table, ranges, rateInKB);
      }

      public long serializedSize(UserValidationOptions options) {
         long size = 8L;
         size += UserValidationID.serializer.serializedSize(options.id);
         size += (long)options.table.id.serializedSize();
         Range range;
         if(options.validatedRanges != null) {
            for(UnmodifiableIterator var4 = options.validatedRanges.iterator(); var4.hasNext(); size += (long)Range.tokenSerializer.serializedSize(range, ((NodeSyncVerbs.NodeSyncVersion)this.version).boundsVersion)) {
               range = (Range)var4.next();
            }
         }

         return size;
      }
   }
}
