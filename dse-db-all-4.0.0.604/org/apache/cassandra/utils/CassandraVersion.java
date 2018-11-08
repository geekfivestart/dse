package org.apache.cassandra.utils;

import java.util.Arrays;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.commons.lang3.StringUtils;

public class CassandraVersion implements Comparable<CassandraVersion> {
   private static final String VERSION_REGEXP = "(\\d+)\\.(\\d+)(?:\\.(\\w+))?(\\-[.\\w]+)?([.+][.\\w]+)?";
   private static final Pattern PATTERN_WHITESPACE = Pattern.compile("\\w+");
   private static final Pattern pattern = Pattern.compile("(\\d+)\\.(\\d+)(?:\\.(\\w+))?(\\-[.\\w]+)?([.+][.\\w]+)?");
   private static final Pattern SNAPSHOT = Pattern.compile("-SNAPSHOT");
   public final int major;
   public final int minor;
   public final int patch;
   private final String[] preRelease;
   private final String[] build;

   public CassandraVersion(String version) {
      String stripped = SNAPSHOT.matcher(version).replaceFirst("");
      Matcher matcher = pattern.matcher(stripped);
      if(!matcher.matches()) {
         throw new IllegalArgumentException("Invalid version value: " + version);
      } else {
         try {
            this.major = Integer.parseInt(matcher.group(1));
            this.minor = Integer.parseInt(matcher.group(2));
            this.patch = matcher.group(3) != null?Integer.parseInt(matcher.group(3)):0;
            String pr = matcher.group(4);
            String bld = matcher.group(5);
            this.preRelease = pr != null && !pr.isEmpty()?parseIdentifiers(stripped, pr):null;
            this.build = bld != null && !bld.isEmpty()?parseIdentifiers(stripped, bld):null;
         } catch (NumberFormatException var6) {
            throw new IllegalArgumentException("Invalid version value: " + version, var6);
         }
      }
   }

   private static String[] parseIdentifiers(String version, String str) {
      str = str.substring(1);
      String[] parts = StringUtils.split(str, '.');
      String[] var3 = parts;
      int var4 = parts.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         String part = var3[var5];
         if(!PATTERN_WHITESPACE.matcher(part).matches()) {
            throw new IllegalArgumentException("Invalid version value: " + version);
         }
      }

      return parts;
   }

   public int compareTo(CassandraVersion other) {
      if(this.major < other.major) {
         return -1;
      } else if(this.major > other.major) {
         return 1;
      } else if(this.minor < other.minor) {
         return -1;
      } else if(this.minor > other.minor) {
         return 1;
      } else if(this.patch < other.patch) {
         return -1;
      } else if(this.patch > other.patch) {
         return 1;
      } else {
         int c = compareIdentifiers(this.preRelease, other.preRelease, 1);
         return c != 0?c:compareIdentifiers(this.build, other.build, -1);
      }
   }

   public CassandraVersion findSupportingVersion(CassandraVersion... versions) {
      CassandraVersion[] var2 = versions;
      int var3 = versions.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         CassandraVersion version = var2[var4];
         if(this.isSupportedBy(version)) {
            return version;
         }
      }

      return null;
   }

   public boolean isSupportedBy(CassandraVersion version) {
      return version != null && this.major == version.major && this.compareTo(version) <= 0;
   }

   private static int compareIdentifiers(String[] ids1, String[] ids2, int defaultPred) {
      if(ids1 == null) {
         return ids2 == null?0:defaultPred;
      } else if(ids2 == null) {
         return -defaultPred;
      } else {
         int min = Math.min(ids1.length, ids2.length);

         for(int i = 0; i < min; ++i) {
            Integer i1 = tryParseInt(ids1[i]);
            Integer i2 = tryParseInt(ids2[i]);
            if(i1 != null) {
               if(i2 == null || i1.intValue() < i2.intValue()) {
                  return -1;
               }

               if(i1.intValue() > i2.intValue()) {
                  return 1;
               }
            } else {
               if(i2 != null) {
                  return 1;
               }

               int c = ids1[i].compareTo(ids2[i]);
               if(c != 0) {
                  return c;
               }
            }
         }

         if(ids1.length < ids2.length) {
            return -1;
         } else if(ids1.length > ids2.length) {
            return 1;
         } else {
            return 0;
         }
      }
   }

   private static Integer tryParseInt(String str) {
      try {
         return Integer.valueOf(str);
      } catch (NumberFormatException var2) {
         return null;
      }
   }

   public boolean equals(Object o) {
      if(!(o instanceof CassandraVersion)) {
         return false;
      } else {
         CassandraVersion that = (CassandraVersion)o;
         return this.major == that.major && this.minor == that.minor && this.patch == that.patch && Arrays.equals(this.preRelease, that.preRelease) && Arrays.equals(this.build, that.build);
      }
   }

   public int hashCode() {
      return Objects.hash(new Object[]{Integer.valueOf(this.major), Integer.valueOf(this.minor), Integer.valueOf(this.patch), this.preRelease, this.build});
   }

   public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append(this.major).append('.').append(this.minor).append('.').append(this.patch);
      if(this.preRelease != null) {
         sb.append('-').append(StringUtils.join(this.preRelease, "."));
      }

      if(this.build != null) {
         sb.append('+').append(StringUtils.join(this.build, "."));
      }

      return sb.toString();
   }
}
