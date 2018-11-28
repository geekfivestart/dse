package org.apache.cassandra.db;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiPredicate;
import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.db.lifecycle.LifecycleTransaction;
import org.apache.cassandra.io.FSDiskFullWriteError;
import org.apache.cassandra.io.FSError;
import org.apache.cassandra.io.FSWriteError;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.SSTable;
import org.apache.cassandra.io.sstable.SnapshotDeletingTask;
import org.apache.cassandra.io.sstable.format.SSTableReader;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.schema.TableMetadata;
import org.apache.cassandra.utils.DirectorySizeCalculator;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.Pair;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Directories {
   private static final Logger logger = LoggerFactory.getLogger(Directories.class);
   public static final String BACKUPS_SUBDIR = "backups";
   public static final String SNAPSHOT_SUBDIR = "snapshots";
   public static final String TMP_SUBDIR = "tmp";
   public static final String SECONDARY_INDEX_NAME_SEPARATOR = ".";
   public static final Directories.DataDirectory[] dataDirectories;
   private final TableMetadata metadata;
   private final Directories.DataDirectory[] paths;
   private final File[] dataPaths;

   public static boolean verifyFullPermissions(File dir, String dataDir) {
      if(!dir.isDirectory()) {
         logger.error("Not a directory {}", dataDir);
         return false;
      } else if(!Directories.FileAction.hasPrivilege(dir, Directories.FileAction.X)) {
         logger.error("Doesn't have execute permissions for {} directory", dataDir);
         return false;
      } else if(!Directories.FileAction.hasPrivilege(dir, Directories.FileAction.R)) {
         logger.error("Doesn't have read permissions for {} directory", dataDir);
         return false;
      } else if(dir.exists() && !Directories.FileAction.hasPrivilege(dir, Directories.FileAction.W)) {
         logger.error("Doesn't have write permissions for {} directory", dataDir);
         return false;
      } else {
         return true;
      }
   }

   public Directories(TableMetadata metadata) {
      this(metadata, dataDirectories);
   }

   public Directories(TableMetadata metadata, Collection<Directories.DataDirectory> paths) {
      this(metadata, (Directories.DataDirectory[])paths.toArray(new Directories.DataDirectory[0]));
   }


   public Directories(final TableMetadata metadata, DataDirectory[] paths) {
      this.metadata = metadata;
      this.paths = paths;
      String tableId = metadata.id.toHexString();
      int idx = metadata.name.indexOf(SECONDARY_INDEX_NAME_SEPARATOR);
      String cfName = idx >= 0 ? metadata.name.substring(0, idx) : metadata.name;
      String indexNameWithDot = idx >= 0 ? metadata.name.substring(idx) : null;
      this.dataPaths = new File[paths.length];
      String oldSSTableRelativePath = Directories.join(metadata.keyspace, cfName);
      for (int i = 0; i < paths.length; ++i) {
         this.dataPaths[i] = new File(paths[i].location, oldSSTableRelativePath);
      }
      boolean olderDirectoryExists = Iterables.any(Arrays.asList(this.dataPaths), File::exists);
      if (!olderDirectoryExists) {
         String newSSTableRelativePath = Directories.join(metadata.keyspace, cfName + '-' + tableId);
         for (int i = 0; i < paths.length; ++i) {
            this.dataPaths[i] = new File(paths[i].location, newSSTableRelativePath);
         }
      }
      if (indexNameWithDot != null) {
         for (int i = 0; i < paths.length; ++i) {
            this.dataPaths[i] = new File(this.dataPaths[i], indexNameWithDot);
         }
      }
      for (File dir : this.dataPaths) {
         try {
            FileUtils.createDirectory(dir);
         }
         catch (FSError e) {
            logger.error("Failed to create {} directory", (Object)dir);
            FileUtils.handleFSError(e);
         }
      }
      if (indexNameWithDot != null) {
         for (File dataPath : this.dataPaths) {
            File[] indexFiles;
            for (File indexFile : indexFiles = dataPath.getParentFile().listFiles(new FileFilter(){

               @Override
               public boolean accept(File file) {
                  if (file.isDirectory()) {
                     return false;
                  }
                  Descriptor desc = SSTable.tryDescriptorFromFilename(file);
                  return desc != null && desc.ksname.equals(metadata.keyspace) && desc.cfname.equals(metadata.name);
               }
            })) {
               File destFile = new File(dataPath, indexFile.getName());
               logger.trace("Moving index file {} to {}", (Object)indexFile, (Object)destFile);
               FileUtils.renameWithConfirm(indexFile, destFile);
            }
         }
      }
   }

   public File getLocationForDisk(Directories.DataDirectory dataDirectory) {
      if(dataDirectory != null) {
         File[] var2 = this.dataPaths;
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            File dir = var2[var4];
            if(dir.getAbsolutePath().startsWith(dataDirectory.location.getAbsolutePath())) {
               return dir;
            }
         }
      }

      return null;
   }

   public Directories.DataDirectory getDataDirectoryForFile(File directory) {
      if(directory != null) {
         Directories.DataDirectory[] var2 = this.paths;
         int var3 = var2.length;

         for(int var4 = 0; var4 < var3; ++var4) {
            Directories.DataDirectory dataDirectory = var2[var4];
            if(directory.getAbsolutePath().startsWith(dataDirectory.location.getAbsolutePath())) {
               return dataDirectory;
            }
         }
      }

      return null;
   }

   public Descriptor find(String filename) {
      File[] var2 = this.dataPaths;
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         File dir = var2[var4];
         File file = new File(dir, filename);
         if(file.exists()) {
            return Descriptor.fromFilename(file);
         }
      }

      return null;
   }

   public File getDirectoryForNewSSTables() {
      return this.getWriteableLocationAsFile(-1L);
   }

   public File getWriteableLocationAsFile(long writeSize) {
      File location = this.getLocationForDisk(this.getWriteableLocation(writeSize));
      if(location == null) {
         throw new FSWriteError(new IOException("No configured data directory contains enough space to write " + writeSize + " bytes"));
      } else {
         return location;
      }
   }

   public File getWriteableLocationAsFile(ColumnFamilyStore cfs, SSTableReader original, long writeSize) {
      Directories.DataDirectory dataDirectory = cfs.getDiskBoundaries(this).getCorrectDiskForSSTable(original);
      File location = this.getLocationForDisk(dataDirectory);
      return location != null && dataDirectory != null && dataDirectory.getAvailableSpace() >= writeSize?location:this.getWriteableLocationAsFile(writeSize);
   }

   public File getTemporaryWriteableDirectoryAsFile(long writeSize) {
      File location = this.getLocationForDisk(this.getWriteableLocation(writeSize));
      return location == null?null:new File(location, "tmp");
   }

   public void removeTemporaryDirectories() {
      File[] var1 = this.dataPaths;
      int var2 = var1.length;

      for(int var3 = 0; var3 < var2; ++var3) {
         File dataDir = var1[var3];
         File tmpDir = new File(dataDir, "tmp");
         if(tmpDir.exists()) {
            logger.debug("Removing temporary directory {}", tmpDir);
            FileUtils.deleteRecursive(tmpDir);
         }
      }

   }

   public Directories.DataDirectory getWriteableLocation(long writeSize) {
      List<Directories.DataDirectoryCandidate> candidates = new ArrayList();
      long totalAvailable = 0L;
      boolean tooBig = false;
      Directories.DataDirectory[] var7 = this.paths;
      int var8 = var7.length;

      for(int var9 = 0; var9 < var8; ++var9) {
         Directories.DataDirectory dataDir = var7[var9];
         if(BlacklistedDirectories.isUnwritable(this.getLocationForDisk(dataDir))) {
            logger.trace("removing blacklisted candidate {}", dataDir.location);
         } else {
            Directories.DataDirectoryCandidate candidate = new Directories.DataDirectoryCandidate(dataDir);
            if(candidate.availableSpace < writeSize) {
               logger.trace("removing candidate {}, usable={}, requested={}", new Object[]{candidate.dataDirectory.location, Long.valueOf(candidate.availableSpace), Long.valueOf(writeSize)});
               tooBig = true;
            } else {
               candidates.add(candidate);
               totalAvailable += candidate.availableSpace;
            }
         }
      }

      if(candidates.isEmpty()) {
         if(tooBig) {
            throw new FSDiskFullWriteError(new IOException("Insufficient disk space to write " + writeSize + " bytes"));
         } else {
            throw new FSWriteError(new IOException("All configured data directories have been blacklisted as unwritable for erroring out"));
         }
      } else if(candidates.size() == 1) {
         return ((Directories.DataDirectoryCandidate)candidates.get(0)).dataDirectory;
      } else {
         sortWriteableCandidates(candidates, totalAvailable);
         return pickWriteableDirectory(candidates);
      }
   }

   static Directories.DataDirectory pickWriteableDirectory(List<Directories.DataDirectoryCandidate> candidates) {
      double rnd = ThreadLocalRandom.current().nextDouble();
      Iterator var3 = candidates.iterator();

      Directories.DataDirectoryCandidate candidate;
      do {
         if(!var3.hasNext()) {
            return ((Directories.DataDirectoryCandidate)candidates.get(0)).dataDirectory;
         }

         candidate = (Directories.DataDirectoryCandidate)var3.next();
         rnd -= candidate.perc;
      } while(rnd > 0.0D);

      return candidate.dataDirectory;
   }

   static void sortWriteableCandidates(List<Directories.DataDirectoryCandidate> candidates, long totalAvailable) {
      Iterator var3 = candidates.iterator();

      while(var3.hasNext()) {
         Directories.DataDirectoryCandidate candidate = (Directories.DataDirectoryCandidate)var3.next();
         candidate.calcFreePerc(totalAvailable);
      }

      Collections.sort(candidates);
   }

   public boolean hasAvailableDiskSpace(long estimatedSSTables, long expectedTotalWriteSize) {
      long writeSize = expectedTotalWriteSize / estimatedSSTables;
      long totalAvailable = 0L;
      Directories.DataDirectory[] var9 = this.paths;
      int var10 = var9.length;

      for(int var11 = 0; var11 < var10; ++var11) {
         Directories.DataDirectory dataDir = var9[var11];
         if(!BlacklistedDirectories.isUnwritable(this.getLocationForDisk(dataDir))) {
            Directories.DataDirectoryCandidate candidate = new Directories.DataDirectoryCandidate(dataDir);
            if(candidate.availableSpace >= writeSize) {
               totalAvailable += candidate.availableSpace;
            }
         }
      }

      return totalAvailable > expectedTotalWriteSize;
   }

   public Directories.DataDirectory[] getWriteableLocations() {
      List<Directories.DataDirectory> nonBlacklistedDirs = new ArrayList();
      Directories.DataDirectory[] var2 = this.paths;
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         Directories.DataDirectory dir = var2[var4];
         if(!BlacklistedDirectories.isUnwritable(dir.location)) {
            nonBlacklistedDirs.add(dir);
         }
      }

      Collections.sort(nonBlacklistedDirs, new Comparator<Directories.DataDirectory>() {
         public int compare(Directories.DataDirectory o1, Directories.DataDirectory o2) {
            return o1.location.compareTo(o2.location);
         }
      });
      return (Directories.DataDirectory[])nonBlacklistedDirs.toArray(new Directories.DataDirectory[0]);
   }

   public static File getSnapshotDirectory(Descriptor desc, String snapshotName) {
      return getSnapshotDirectory(desc.directory, snapshotName);
   }

   public static File getSnapshotDirectory(File location, String snapshotName) {
      return location.getName().startsWith(".")?getOrCreate(location.getParentFile(), new String[]{"snapshots", snapshotName, location.getName()}):getOrCreate(location, new String[]{"snapshots", snapshotName});
   }

   public File getSnapshotManifestFile(String snapshotName) {
      File snapshotDir = getSnapshotDirectory(this.getDirectoryForNewSSTables(), snapshotName);
      return new File(snapshotDir, "manifest.json");
   }

   public File getSnapshotSchemaFile(String snapshotName) {
      File snapshotDir = getSnapshotDirectory(this.getDirectoryForNewSSTables(), snapshotName);
      return new File(snapshotDir, "schema.cql");
   }

   public File getNewEphemeralSnapshotMarkerFile(String snapshotName) {
      File snapshotDir = new File(this.getWriteableLocationAsFile(1L), join(new String[]{"snapshots", snapshotName}));
      return getEphemeralSnapshotMarkerFile(snapshotDir);
   }

   private static File getEphemeralSnapshotMarkerFile(File snapshotDirectory) {
      return new File(snapshotDirectory, "ephemeral.snapshot");
   }

   public static File getBackupsDirectory(Descriptor desc) {
      return getBackupsDirectory(desc.directory);
   }

   public static File getBackupsDirectory(File location) {
      return location.getName().startsWith(".")?getOrCreate(location.getParentFile(), new String[]{"backups", location.getName()}):getOrCreate(location, new String[]{"backups"});
   }

   public Directories.SSTableLister sstableLister(Directories.OnTxnErr onTxnErr) {
      return new Directories.SSTableLister(onTxnErr);
   }

   public Map<String, Pair<Long, Long>> getSnapshotDetails() {
      List<File> snapshots = this.listSnapshots();
      Map<String, Pair<Long, Long>> snapshotSpaceMap = Maps.newHashMapWithExpectedSize(snapshots.size());

      File snapshot;
      Pair spaceUsed;
      for(Iterator var3 = snapshots.iterator(); var3.hasNext(); snapshotSpaceMap.put(snapshot.getName(), spaceUsed)) {
         snapshot = (File)var3.next();
         long sizeOnDisk = FileUtils.folderSize(snapshot);
         long trueSize = this.getTrueAllocatedSizeIn(snapshot);
         spaceUsed = (Pair)snapshotSpaceMap.get(snapshot.getName());
         if(spaceUsed == null) {
            spaceUsed = Pair.create(Long.valueOf(sizeOnDisk), Long.valueOf(trueSize));
         } else {
            spaceUsed = Pair.create(Long.valueOf(((Long)spaceUsed.left).longValue() + sizeOnDisk), Long.valueOf(((Long)spaceUsed.right).longValue() + trueSize));
         }
      }

      return snapshotSpaceMap;
   }

   public List<String> listEphemeralSnapshots() {
      List<String> ephemeralSnapshots = new LinkedList();
      Iterator var2 = this.listSnapshots().iterator();

      while(var2.hasNext()) {
         File snapshot = (File)var2.next();
         if(getEphemeralSnapshotMarkerFile(snapshot).exists()) {
            ephemeralSnapshots.add(snapshot.getName());
         }
      }

      return ephemeralSnapshots;
   }

   private List<File> listSnapshots() {
      List<File> snapshots = new LinkedList();
      File[] var2 = this.dataPaths;
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         File dir = var2[var4];
         File snapshotDir = dir.getName().startsWith(".")?new File(dir.getParent(), "snapshots"):new File(dir, "snapshots");
         if(snapshotDir.exists() && snapshotDir.isDirectory()) {
            File[] snapshotDirs = snapshotDir.listFiles();
            if(snapshotDirs != null) {
               File[] var8 = snapshotDirs;
               int var9 = snapshotDirs.length;

               for(int var10 = 0; var10 < var9; ++var10) {
                  File snapshot = var8[var10];
                  if(snapshot.isDirectory()) {
                     snapshots.add(snapshot);
                  }
               }
            }
         }
      }

      return snapshots;
   }

   public boolean snapshotExists(String snapshotName) {
      File[] var2 = this.dataPaths;
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         File dir = var2[var4];
         File snapshotDir;
         if(dir.getName().startsWith(".")) {
            snapshotDir = new File(dir.getParentFile(), join(new String[]{"snapshots", snapshotName, dir.getName()}));
         } else {
            snapshotDir = new File(dir, join(new String[]{"snapshots", snapshotName}));
         }

         if(snapshotDir.exists()) {
            return true;
         }
      }

      return false;
   }

   public static void clearSnapshot(String snapshotName, List<File> snapshotDirectories) {
      String tag = snapshotName == null?"":snapshotName;
      Iterator var3 = snapshotDirectories.iterator();

      while(true) {
         File snapshotDir;
         do {
            if(!var3.hasNext()) {
               return;
            }

            File dir = (File)var3.next();
            snapshotDir = new File(dir, join(new String[]{"snapshots", tag}));
         } while(!snapshotDir.exists());

         logger.trace("Removing snapshot directory {}", snapshotDir);

         try {
            FileUtils.deleteRecursive(snapshotDir);
         } catch (FSWriteError var7) {
            if(!FBUtilities.isWindows) {
               throw var7;
            }

            SnapshotDeletingTask.addFailedSnapshot(snapshotDir);
         }
      }
   }

   public long snapshotCreationTime(String snapshotName) {
      File[] var2 = this.dataPaths;
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         File dir = var2[var4];
         File snapshotDir = getSnapshotDirectory(dir, snapshotName);
         if(snapshotDir.exists()) {
            return snapshotDir.lastModified();
         }
      }

      throw new RuntimeException("Snapshot " + snapshotName + " doesn't exist");
   }

   public long trueSnapshotsSize() {
      long result = 0L;
      File[] var3 = this.dataPaths;
      int var4 = var3.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         File dir = var3[var5];
         File snapshotDir = dir.getName().startsWith(".")?new File(dir.getParent(), "snapshots"):new File(dir, "snapshots");
         result += this.getTrueAllocatedSizeIn(snapshotDir);
      }

      return result;
   }

   public long getRawDiretoriesSize() {
      long totalAllocatedSize = 0L;
      File[] var3 = this.dataPaths;
      int var4 = var3.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         File path = var3[var5];
         totalAllocatedSize += FileUtils.folderSize(path);
      }

      return totalAllocatedSize;
   }

   public long getTrueAllocatedSizeIn(File input) {
      if(!input.isDirectory()) {
         return 0L;
      } else {
         Directories.SSTableSizeSummer visitor = new Directories.SSTableSizeSummer(input, this.sstableLister(Directories.OnTxnErr.THROW).listFiles());

         try {
            Files.walkFileTree(input.toPath(), visitor);
         } catch (IOException var4) {
            logger.error("Could not calculate the size of {}. {}", input, var4.getMessage());
         }

         return visitor.getAllocatedSize();
      }
   }

   public static List<File> getKSChildDirectories(String ksName) {
      return getKSChildDirectories(ksName, dataDirectories);
   }

   public static List<File> getKSChildDirectories(String ksName, Directories.DataDirectory[] directories) {
      List<File> result = new ArrayList();
      Directories.DataDirectory[] var3 = directories;
      int var4 = directories.length;

      for(int var5 = 0; var5 < var4; ++var5) {
         Directories.DataDirectory dataDirectory = var3[var5];
         File ksDir = new File(dataDirectory.location, ksName);
         File[] cfDirs = ksDir.listFiles();
         if(cfDirs != null) {
            File[] var9 = cfDirs;
            int var10 = cfDirs.length;

            for(int var11 = 0; var11 < var10; ++var11) {
               File cfDir = var9[var11];
               if(cfDir.isDirectory()) {
                  result.add(cfDir);
               }
            }
         }
      }

      return result;
   }

   public List<File> getCFDirectories() {
      List<File> result = new ArrayList();
      File[] var2 = this.dataPaths;
      int var3 = var2.length;

      for(int var4 = 0; var4 < var3; ++var4) {
         File dataDirectory = var2[var4];
         if(dataDirectory.isDirectory()) {
            result.add(dataDirectory);
         }
      }

      return result;
   }

   private static File getOrCreate(File base, String... subdirs) {
      File dir = subdirs != null && subdirs.length != 0?new File(base, join(subdirs)):base;
      if(dir.exists()) {
         if(!dir.isDirectory()) {
            throw new AssertionError(String.format("Invalid directory path %s: path exists but is not a directory", new Object[]{dir}));
         }
      } else if(!dir.mkdirs() && (!dir.exists() || !dir.isDirectory())) {
         throw new FSWriteError(new IOException("Unable to create directory " + dir), dir);
      }

      return dir;
   }

   private static String join(String... s) {
      return StringUtils.join(s, File.separator);
   }

   @VisibleForTesting
   static void overrideDataDirectoriesForTest(String loc) {
      for(int i = 0; i < dataDirectories.length; ++i) {
         dataDirectories[i] = new Directories.DataDirectory(new File(loc));
      }

   }

   @VisibleForTesting
   static void resetDataDirectoriesAfterTest() {
      String[] locations = DatabaseDescriptor.getAllDataFileLocations();

      for(int i = 0; i < locations.length; ++i) {
         dataDirectories[i] = new Directories.DataDirectory(new File(locations[i]));
      }

   }

   public boolean equals(Object o) {
      if(this == o) {
         return true;
      } else if(o != null && this.getClass() == o.getClass()) {
         Directories that = (Directories)o;
         return !this.metadata.id.equals(that.metadata.id)?false:Arrays.equals(this.paths, that.paths);
      } else {
         return false;
      }
   }

   public int hashCode() {
      int result = this.metadata.hashCode();
      result = 31 * result + Arrays.hashCode(this.paths);
      return result;
   }

   static {
      String[] locations = DatabaseDescriptor.getAllDataFileLocations();
      dataDirectories = new Directories.DataDirectory[locations.length];

      for(int i = 0; i < locations.length; ++i) {
         dataDirectories[i] = new Directories.DataDirectory(new File(locations[i]));
      }

   }

   private class SSTableSizeSummer extends DirectorySizeCalculator {
      private final HashSet<File> toSkip;

      SSTableSizeSummer(File path, List<File> files) {
         super(path);
         this.toSkip = new HashSet(files);
      }

      public boolean isAcceptable(Path path) {
         File file = path.toFile();
         Descriptor desc = SSTable.tryDescriptorFromFilename(file);
         return desc != null && desc.ksname.equals(Directories.this.metadata.keyspace) && desc.cfname.equals(Directories.this.metadata.name) && !this.toSkip.contains(file);
      }
   }

   public class SSTableLister {
      private final Directories.OnTxnErr onTxnErr;
      private boolean skipTemporary;
      private boolean includeBackups;
      private boolean onlyBackups;
      private int nbFiles;
      private final Map<Descriptor, Set<Component>> components;
      private boolean filtered;
      private String snapshotName;

      private SSTableLister(Directories.OnTxnErr onTxnErr) {
         this.components = new HashMap();
         this.onTxnErr = onTxnErr;
      }

      public Directories.SSTableLister skipTemporary(boolean b) {
         if(this.filtered) {
            throw new IllegalStateException("list() has already been called");
         } else {
            this.skipTemporary = b;
            return this;
         }
      }

      public Directories.SSTableLister includeBackups(boolean b) {
         if(this.filtered) {
            throw new IllegalStateException("list() has already been called");
         } else {
            this.includeBackups = b;
            return this;
         }
      }

      public Directories.SSTableLister onlyBackups(boolean b) {
         if(this.filtered) {
            throw new IllegalStateException("list() has already been called");
         } else {
            this.onlyBackups = b;
            this.includeBackups = b;
            return this;
         }
      }

      public Directories.SSTableLister snapshots(String sn) {
         if(this.filtered) {
            throw new IllegalStateException("list() has already been called");
         } else {
            this.snapshotName = sn;
            return this;
         }
      }

      public Map<Descriptor, Set<Component>> list() {
         this.filter();
         return ImmutableMap.copyOf(this.components);
      }

      public List<File> listFiles() {
         this.filter();
         List<File> l = new ArrayList(this.nbFiles);
         Iterator var2 = this.components.entrySet().iterator();

         while(var2.hasNext()) {
            Entry<Descriptor, Set<Component>> entry = (Entry)var2.next();
            Iterator var4 = ((Set)entry.getValue()).iterator();

            while(var4.hasNext()) {
               Component c = (Component)var4.next();
               l.add(new File(((Descriptor)entry.getKey()).filenameFor(c)));
            }
         }

         return l;
      }

      private void filter() {
         if(!this.filtered) {
            File[] var1 = Directories.this.dataPaths;
            int var2 = var1.length;

            for(int var3 = 0; var3 < var2; ++var3) {
               File location = var1[var3];
               if(!BlacklistedDirectories.isUnreadable(location)) {
                  if(this.snapshotName != null) {
                     LifecycleTransaction.getFiles(Directories.getSnapshotDirectory(location, this.snapshotName).toPath(), this.getFilter(), this.onTxnErr);
                  } else {
                     if(!this.onlyBackups) {
                        LifecycleTransaction.getFiles(location.toPath(), this.getFilter(), this.onTxnErr);
                     }

                     if(this.includeBackups) {
                        LifecycleTransaction.getFiles(Directories.getBackupsDirectory(location).toPath(), this.getFilter(), this.onTxnErr);
                     }
                  }
               }
            }

            this.filtered = true;
         }
      }

      private BiPredicate<File, Directories.FileType> getFilter() {
         return (file, type) -> {
            switch (type) {
               case TXN_LOG: {
                  return false;
               }
               case TEMPORARY: {
                  if (this.skipTemporary) {
                     return false;
                  }
               }
               case FINAL: {
                  Pair<Descriptor, Component> pair = SSTable.tryComponentFromFilename(file);
                  if (pair == null) {
                     return false;
                  }
                  if (!((Descriptor)pair.left).ksname.equals((Directories.this.metadata).keyspace) ||
                          !((Descriptor)pair.left).cfname.equals((Directories.this.metadata).name)) {
                     return false;
                  }
                  Set<Component> previous = this.components.get(pair.left);
                  if (previous == null) {
                     previous = new HashSet<Component>();
                     this.components.put((Descriptor)pair.left, previous);
                  }
                  previous.add((Component)pair.right);
                  ++this.nbFiles;
                  return false;
               }
            }
            throw new AssertionError();
         };
      }
   }

   public static enum OnTxnErr {
      THROW,
      IGNORE;

      private OnTxnErr() {
      }
   }

   public static enum FileType {
      FINAL,
      TEMPORARY,
      TXN_LOG;

      private FileType() {
      }
   }

   static final class DataDirectoryCandidate implements Comparable<Directories.DataDirectoryCandidate> {
      final Directories.DataDirectory dataDirectory;
      final long availableSpace;
      double perc;

      public DataDirectoryCandidate(Directories.DataDirectory dataDirectory) {
         this.dataDirectory = dataDirectory;
         this.availableSpace = dataDirectory.getAvailableSpace();
      }

      void calcFreePerc(long totalAvailableSpace) {
         double w = (double)this.availableSpace;
         w /= (double)totalAvailableSpace;
         this.perc = w;
      }

      public int compareTo(Directories.DataDirectoryCandidate o) {
         if(this == o) {
            return 0;
         } else {
            int r = Double.compare(this.perc, o.perc);
            return r != 0?-r:System.identityHashCode(this) - System.identityHashCode(o);
         }
      }
   }

   public static class DataDirectory {
      public final File location;

      public DataDirectory(File location) {
         this.location = location;
      }

      public long getAvailableSpace() {
         long availableSpace = FileUtils.getUsableSpace(this.location) - DatabaseDescriptor.getMinFreeSpacePerDriveInBytes();
         return availableSpace > 0L?availableSpace:0L;
      }

      public boolean equals(Object o) {
         if(this == o) {
            return true;
         } else if(o != null && this.getClass() == o.getClass()) {
            Directories.DataDirectory that = (Directories.DataDirectory)o;
            return this.location.equals(that.location);
         } else {
            return false;
         }
      }

      public int hashCode() {
         return this.location.hashCode();
      }

      public String toString() {
         return "DataDirectory{location=" + this.location + '}';
      }
   }

   public static enum FileAction {
      X,
      W,
      XW,
      R,
      XR,
      RW,
      XRW;

      private FileAction() {
      }

      public static boolean hasPrivilege(File file, Directories.FileAction action) {
         boolean privilege = false;
         switch (action) {
            case X: {
               privilege = file.canExecute();
               break;
            }
            case W: {
               privilege = file.canWrite();
               break;
            }
            case XW: {
               privilege = file.canExecute() && file.canWrite();
               break;
            }
            case R: {
               privilege = file.canRead();
               break;
            }
            case XR: {
               privilege = file.canExecute() && file.canRead();
               break;
            }
            case RW: {
               privilege = file.canRead() && file.canWrite();
               break;
            }
            case XRW: {
               privilege = file.canExecute() && file.canRead() && file.canWrite();
            }
         }
         return privilege;
      }
   }
}
