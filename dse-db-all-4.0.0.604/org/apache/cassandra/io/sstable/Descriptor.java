package org.apache.cassandra.io.sstable;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import java.io.File;
import java.io.FilenameFilter;
import java.io.IOError;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.regex.Pattern;
import org.apache.cassandra.io.sstable.format.SSTableFormat;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.sstable.metadata.IMetadataSerializer;
import org.apache.cassandra.io.sstable.metadata.MetadataSerializer;
import org.apache.cassandra.utils.Pair;

public class Descriptor {
   private static final String LEGACY_TMP_REGEX_STR = "^((.*)\\-(.*)\\-)?tmp(link)?\\-((?:l|k).)\\-(\\d)*\\-(.*)$";
   private static final Pattern LEGACY_TMP_REGEX = Pattern.compile("^((.*)\\-(.*)\\-)?tmp(link)?\\-((?:l|k).)\\-(\\d)*\\-(.*)$");
   public static String TMP_EXT = ".tmp";
   private static final Splitter filenameSplitter = Splitter.on('-');
   public final File directory;
   public final Version version;
   public final String ksname;
   public final String cfname;
   public final int generation;
   public final SSTableFormat.Type formatType;
   private final int hashCode;

   @VisibleForTesting
   public Descriptor(File directory, String ksname, String cfname, int generation) {
      this(SSTableFormat.Type.current().info.getLatestVersion(), directory, ksname, cfname, generation, SSTableFormat.Type.current());
   }

   public Descriptor(File directory, String ksname, String cfname, int generation, SSTableFormat.Type formatType) {
      this(formatType.info.getLatestVersion(), directory, ksname, cfname, generation, formatType);
   }

   public Descriptor(Version version, File directory, String ksname, String cfname, int generation, SSTableFormat.Type formatType) {
      assert version != null && directory != null && ksname != null && cfname != null && formatType.info.getLatestVersion().getClass().equals(version.getClass());

      this.version = version;

      try {
         this.directory = directory.getCanonicalFile();
      } catch (IOException var8) {
         throw new IOError(var8);
      }

      this.ksname = ksname;
      this.cfname = cfname;
      this.generation = generation;
      this.formatType = formatType;
      this.hashCode = Objects.hash(new Object[]{version, this.directory, Integer.valueOf(generation), ksname, cfname, formatType});
   }

   public Descriptor withGeneration(int newGeneration) {
      return new Descriptor(this.version, this.directory, this.ksname, this.cfname, newGeneration, this.formatType);
   }

   public Descriptor withFormatType(SSTableFormat.Type newType) {
      return new Descriptor(newType.info.getLatestVersion(), this.directory, this.ksname, this.cfname, this.generation, newType);
   }

   public String tmpFilenameFor(Component component) {
      return this.filenameFor(component) + TMP_EXT;
   }

   public String filenameFor(Component component) {
      return this.baseFilenameBuilder().append('-').append(component.name()).toString();
   }

   public String baseFilename() {
      return this.baseFilenameBuilder().toString();
   }

   private StringBuilder baseFilenameBuilder() {
      StringBuilder buff = new StringBuilder();
      buff.append(this.directory).append(File.separatorChar);
      this.appendFileName(buff);
      return buff;
   }

   private void appendFileName(StringBuilder buff) {
      buff.append(this.version).append('-');
      buff.append(this.generation);
      buff.append('-').append(this.formatType.name);
   }

   public String relativeFilenameFor(Component component) {
      StringBuilder buff = new StringBuilder();
      this.appendFileName(buff);
      buff.append('-').append(component.name());
      return buff.toString();
   }

   public SSTableFormat getFormat() {
      return this.formatType.info;
   }

   public List<File> getTemporaryFiles() {
      File[] tmpFiles = this.directory.listFiles((dir, name) -> {
         return name.endsWith(TMP_EXT);
      });
      List<File> ret = new ArrayList(tmpFiles.length);
      File[] var3 = tmpFiles;
      int var4 = tmpFiles.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         File tmpFile = var3[var5];
         ret.add(tmpFile);
      }

      return ret;
   }

   public static boolean isValidFile(File file) {
      String filename = file.getName();
      return filename.endsWith(".db") && !LEGACY_TMP_REGEX.matcher(filename).matches();
   }

   public static Descriptor fromFilename(String filename) {
      return fromFilename(new File(filename));
   }

   public static Descriptor fromFilename(File file) {
      return (Descriptor)fromFilenameWithComponent(file).left;
   }

   public static Component componentFromFilename(File file) {
      String name = file.getName();
      List<String> tokens = filenameTokens(name);
      return Component.parse((String)tokens.get(3));
   }

   public static Pair<Descriptor, Component> fromFilenameWithComponent(File file) {
      if(!file.isAbsolute()) {
         file = file.getAbsoluteFile();
      }

      String name = file.getName();
      List<String> tokens = filenameTokens(name);
      String versionString = (String)tokens.get(0);

      int generation;
      try {
         generation = Integer.parseInt((String)tokens.get(1));
      } catch (NumberFormatException var15) {
         throw invalidSSTable(name, "the 'generation' part of the name doesn't parse as a number", new Object[0]);
      }

      String formatString = (String)tokens.get(2);

      SSTableFormat.Type format;
      try {
         format = SSTableFormat.Type.validate(formatString);
      } catch (IllegalArgumentException var14) {
         throw invalidSSTable(name, "unknown 'format' part (%s)", new Object[]{formatString});
      }

      if(!format.validateVersion(versionString)) {
         throw invalidSSTable(name, "invalid version %s", new Object[]{versionString});
      } else {
         Component component = Component.parse((String)tokens.get(3));
         Version version = format.info.getVersion(versionString);
         if(!version.isCompatible()) {
            throw invalidSSTable(name, "incompatible sstable version (%s); you should have run upgradesstables before upgrading", new Object[]{versionString});
         } else {
            File directory = parentOf(name, file);
            File tableDir = directory;
            String indexName = "";
            if(directory.getName().startsWith(".")) {
               indexName = directory.getName();
               tableDir = parentOf(name, directory);
            }

            if(tableDir.getName().equals("backups")) {
               tableDir = tableDir.getParentFile();
            } else if(parentOf(name, tableDir).getName().equals("snapshots")) {
               tableDir = parentOf(name, parentOf(name, tableDir));
            }

            String table = tableDir.getName().split("-")[0] + indexName;
            String keyspace = parentOf(name, tableDir).getName();
            return Pair.create(new Descriptor(version, directory, keyspace, table, generation, format), component);
         }
      }
   }

   private static List<String> filenameTokens(String name) {
      List<String> tokens = filenameSplitter.splitToList(name);
      int size = tokens.size();
      if(size != 4) {
         if(LEGACY_TMP_REGEX.matcher(name).matches()) {
            throw new IllegalArgumentException(String.format("%s is of version %s which is now unsupported and cannot be read.", new Object[]{name, tokens.get(size - 3)}));
         } else {
            throw new IllegalArgumentException(String.format("Invalid sstable file %s: the name doesn't look like a supported sstable file name", new Object[]{name}));
         }
      } else {
         return tokens;
      }
   }

   private static File parentOf(String name, File file) {
      File parent = file.getParentFile();
      if(parent == null) {
         throw invalidSSTable(name, "cannot extract keyspace and table name; make sure the sstable is in the proper sub-directories", new Object[0]);
      } else {
         return parent;
      }
   }

   private static IllegalArgumentException invalidSSTable(String name, String msgFormat, Object... parameters) {
      throw new IllegalArgumentException(String.format("Invalid sstable file " + name + ": " + msgFormat, parameters));
   }

   public IMetadataSerializer getMetadataSerializer() {
      return new MetadataSerializer();
   }

   public boolean isCompatible() {
      return this.version.isCompatible();
   }

   public String toString() {
      return this.baseFilename();
   }

   public boolean equals(Object o) {
      if(o == this) {
         return true;
      } else if(!(o instanceof Descriptor)) {
         return false;
      } else {
         Descriptor that = (Descriptor)o;
         return that.directory.equals(this.directory) && that.generation == this.generation && that.ksname.equals(this.ksname) && that.cfname.equals(this.cfname) && that.formatType == this.formatType;
      }
   }

   public int hashCode() {
      return this.hashCode;
   }
}
