package org.apache.cassandra.transport;

import java.util.Arrays;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.commons.lang3.ArrayUtils;

public enum ProtocolVersion implements Comparable<ProtocolVersion> {
   V1(1, "v1", false),
   V2(2, "v2", false),
   V3(3, "v3", false),
   V4(4, "v4", false),
   V5(5, "v5-beta", true),
   DSE_V1(65, "dse-v1", false),
   DSE_V2(66, "dse-v2", false);

   private final int num;
   private final String descr;
   private final boolean beta;
   private static final byte DSE_VERSION_BIT = 64;
   static final ProtocolVersion[] OS_VERSIONS = new ProtocolVersion[]{V3, V4, V5};
   static final ProtocolVersion MIN_OS_VERSION = OS_VERSIONS[0];
   static final ProtocolVersion MAX_OS_VERSION = OS_VERSIONS[OS_VERSIONS.length - 1];
   static final ProtocolVersion[] DSE_VERSIONS = new ProtocolVersion[]{DSE_V1, DSE_V2};
   static final ProtocolVersion MIN_DSE_VERSION = DSE_VERSIONS[0];
   static final ProtocolVersion MAX_DSE_VERSION = DSE_VERSIONS[DSE_VERSIONS.length - 1];
   public static final EnumSet<ProtocolVersion> SUPPORTED = EnumSet.copyOf(Arrays.asList((ProtocolVersion[])((ProtocolVersion[])ArrayUtils.addAll(OS_VERSIONS, DSE_VERSIONS))));
   public static final List<String> SUPPORTED_VERSION_NAMES = (List)SUPPORTED.stream().map(ProtocolVersion::toString).collect(Collectors.toList());
   public static final EnumSet<ProtocolVersion> UNSUPPORTED = EnumSet.complementOf(SUPPORTED);
   public static final ProtocolVersion CURRENT = DSE_V2;
   public static final Optional<ProtocolVersion> BETA = Optional.empty();

   private ProtocolVersion(int num, String descr, boolean beta) {
      this.num = num;
      this.descr = descr;
      this.beta = beta;
   }

   public static List<String> supportedVersions() {
      return SUPPORTED_VERSION_NAMES;
   }

   public static ProtocolVersion decode(int versionNum) {
      ProtocolVersion ret = null;
      boolean isDse = isDse(versionNum);
      if(isDse) {
         if(versionNum >= MIN_DSE_VERSION.num && versionNum <= MAX_DSE_VERSION.num) {
            ret = DSE_VERSIONS[versionNum - MIN_DSE_VERSION.num];
         }
      } else if(versionNum >= MIN_OS_VERSION.num && versionNum <= MAX_OS_VERSION.num) {
         ret = OS_VERSIONS[versionNum - MIN_OS_VERSION.num];
      }

      if(ret == null) {
         Iterator var3 = UNSUPPORTED.iterator();

         ProtocolVersion version;
         do {
            if(!var3.hasNext()) {
               throw new ProtocolException(invalidVersionMessage(versionNum), isDse?MAX_DSE_VERSION:MAX_OS_VERSION);
            }

            version = (ProtocolVersion)var3.next();
         } while(version.num != versionNum);

         throw new ProtocolException(invalidVersionMessage(versionNum), version);
      } else {
         return ret;
      }
   }

   public boolean isDse() {
      return isDse(this.num);
   }

   private static boolean isDse(int num) {
      return (num & 64) == 64;
   }

   public boolean isBeta() {
      return this.beta;
   }

   public static String invalidVersionMessage(int version) {
      return String.format("Invalid or unsupported protocol version (%d); supported versions are (%s)", new Object[]{Integer.valueOf(version), String.join(", ", supportedVersions())});
   }

   public int asInt() {
      return this.num;
   }

   public String toString() {
      return String.format("%d/%s", new Object[]{Integer.valueOf(this.num), this.descr});
   }

   public final boolean isGreaterThan(ProtocolVersion other) {
      return this.ordinal() > other.ordinal();
   }

   public final boolean isGreaterOrEqualTo(ProtocolVersion other) {
      return this.ordinal() >= other.ordinal();
   }

   public final boolean isGreaterOrEqualTo(ProtocolVersion ossVersion, ProtocolVersion dseVersion) {
      return this.ordinal() >= (this.isDse()?dseVersion.ordinal():ossVersion.ordinal());
   }

   public final boolean isSmallerThan(ProtocolVersion other) {
      return this.ordinal() < other.ordinal();
   }

   public final boolean isSmallerOrEqualTo(ProtocolVersion other) {
      return this.ordinal() <= other.ordinal();
   }
}
