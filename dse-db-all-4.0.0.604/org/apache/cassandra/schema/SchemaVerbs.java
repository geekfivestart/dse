package org.apache.cassandra.schema;

import java.util.UUID;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.EncodingVersion;
import org.apache.cassandra.net.DroppedMessages;
import org.apache.cassandra.net.DroppingResponseException;
import org.apache.cassandra.net.EmptyPayload;
import org.apache.cassandra.net.Verb;
import org.apache.cassandra.net.VerbGroup;
import org.apache.cassandra.net.VerbHandlers;
import org.apache.cassandra.net.Verbs;
import org.apache.cassandra.utils.UUIDSerializer;
import org.apache.cassandra.utils.versioning.Version;
import org.apache.cassandra.utils.versioning.Versioned;

public class SchemaVerbs extends VerbGroup<SchemaVerbs.SchemaVersion> {
   public final Verb.RequestResponse<EmptyPayload, UUID> VERSION;
   public final Verb.RequestResponse<PullRequest, SchemaMigration> PULL;
   public final Verb.OneWay<SchemaMigration> PUSH;

   public SchemaVerbs(Verbs.Group id) {
      super(id, true, SchemaVerbs.SchemaVersion.class);
      VerbGroup<SchemaVerbs.SchemaVersion>.RegistrationHelper helper = this.helper().stage(Stage.MIGRATION).droppedGroup(DroppedMessages.Group.SCHEMA);
      this.VERSION = ((VerbGroup.RegistrationHelper.RequestResponseBuilder)((VerbGroup.RegistrationHelper.RequestResponseBuilder)helper.requestResponse("VERSION", EmptyPayload.class, UUID.class).withResponseSerializer(UUIDSerializer.serializer)).timeout(DatabaseDescriptor::getRpcTimeout)).syncHandler((from, x) -> {
         return Schema.instance.getVersion();
      });
      this.PULL = (helper.requestResponse("PULL", PullRequest.class, SchemaMigration.class).timeout(DatabaseDescriptor::getRpcTimeout)).syncHandler((from, pr) -> {
         int version = pr.schemaCompatibilityVersion();
         if(version < 0) {
            if(!Schema.instance.isSchemaCompatibleWith(from)) {
               throw new DroppingResponseException();
            }
         } else if(!Schema.instance.isSchemaCompatibleWith(version)) {
            return SchemaMigration.incompatible();
         }

         return SchemaKeyspace.convertSchemaToMutations();
      });
      this.PUSH = helper.oneWay("PUSH", SchemaMigration.class).handler((from, migration) -> {
         assert migration.isCompatible;

         Schema.instance.mergeAndAnnounceVersion(migration);
      });
   }

   public static enum SchemaVersion implements Version<SchemaVerbs.SchemaVersion> {
      OSS_30(EncodingVersion.OSS_30),
      DSE_603(EncodingVersion.OSS_30);

      public final EncodingVersion encodingVersion;

      private SchemaVersion(EncodingVersion encodingVersion) {
         this.encodingVersion = encodingVersion;
      }

      public static <T> Versioned<SchemaVerbs.SchemaVersion, T> versioned(Function<SchemaVerbs.SchemaVersion, ? extends T> function) {
         return new Versioned(SchemaVerbs.SchemaVersion.class, function);
      }
   }
}
