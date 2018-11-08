package org.apache.cassandra.io.sstable.metadata;

import com.google.common.collect.Lists;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.StandardOpenOption;
import java.util.Collections;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.zip.CRC32;
import org.apache.cassandra.io.sstable.Component;
import org.apache.cassandra.io.sstable.CorruptSSTableException;
import org.apache.cassandra.io.sstable.Descriptor;
import org.apache.cassandra.io.sstable.format.Version;
import org.apache.cassandra.io.util.BufferedDataOutputStreamPlus;
import org.apache.cassandra.io.util.DataInputBuffer;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.io.util.DataOutputPlus;
import org.apache.cassandra.io.util.DataOutputStreamPlus;
import org.apache.cassandra.io.util.FileDataInput;
import org.apache.cassandra.io.util.FileUtils;
import org.apache.cassandra.io.util.RandomAccessReader;
import org.apache.cassandra.utils.FBUtilities;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MetadataSerializer implements IMetadataSerializer {
   private static final Logger logger = LoggerFactory.getLogger(MetadataSerializer.class);
   private static final int CHECKSUM_LENGTH = 4;

   public MetadataSerializer() {
   }

   public void serialize(Map<MetadataType, MetadataComponent> components, DataOutputPlus out, Version version) throws IOException {
      boolean checksum = version.hasMetadataChecksum();
      CRC32 crc = new CRC32();
      List<MetadataComponent> sortedComponents = Lists.newArrayList(components.values());
      Collections.sort(sortedComponents);
      out.writeInt(components.size());
      FBUtilities.updateChecksumInt(crc, components.size());
      maybeWriteChecksum(crc, out, version);
      int lastPosition = 4 + 8 * sortedComponents.size() + (checksum?8:0);
      Map<MetadataType, Integer> sizes = new EnumMap(MetadataType.class);
      Iterator var9 = sortedComponents.iterator();

      MetadataComponent component;
      while(var9.hasNext()) {
         component = (MetadataComponent)var9.next();
         MetadataType type = component.getType();
         out.writeInt(type.ordinal());
         FBUtilities.updateChecksumInt(crc, type.ordinal());
         out.writeInt(lastPosition);
         FBUtilities.updateChecksumInt(crc, lastPosition);
         int size = type.serializer.serializedSize(version, component);
         lastPosition += size + (checksum?4:0);
         sizes.put(type, Integer.valueOf(size));
      }

      maybeWriteChecksum(crc, out, version);
      var9 = sortedComponents.iterator();

      while(var9.hasNext()) {
         component = (MetadataComponent)var9.next();
         DataOutputBuffer dob = new DataOutputBuffer(((Integer)sizes.get(component.getType())).intValue());
         Throwable var13 = null;

         byte[] bytes;
         try {
            component.getType().serializer.serialize(version, component, dob);
            bytes = dob.getData();
         } catch (Throwable var22) {
            var13 = var22;
            throw var22;
         } finally {
            if(dob != null) {
               if(var13 != null) {
                  try {
                     dob.close();
                  } catch (Throwable var21) {
                     var13.addSuppressed(var21);
                  }
               } else {
                  dob.close();
               }
            }

         }

         out.write(bytes);
         crc.reset();
         crc.update(bytes);
         maybeWriteChecksum(crc, out, version);
      }

   }

   private static void maybeWriteChecksum(CRC32 crc, DataOutputPlus out, Version version) throws IOException {
      if(version.hasMetadataChecksum()) {
         out.writeInt((int)crc.getValue());
      }

   }

   public Map<MetadataType, MetadataComponent> deserialize(Descriptor descriptor, EnumSet<MetadataType> types) throws IOException {
      logger.trace("Load metadata for {}", descriptor);
      File statsFile = new File(descriptor.filenameFor(Component.STATS));
      Object components;
      if(!statsFile.exists()) {
         logger.trace("No sstable stats for {}", descriptor);
         components = new EnumMap(MetadataType.class);
         ((Map)components).put(MetadataType.STATS, MetadataCollector.defaultStatsMetadata());
      } else {
         RandomAccessReader r = RandomAccessReader.open(statsFile);
         Throwable var6 = null;

         try {
            components = this.deserialize(descriptor, r, types);
         } catch (Throwable var15) {
            var6 = var15;
            throw var15;
         } finally {
            if(r != null) {
               if(var6 != null) {
                  try {
                     r.close();
                  } catch (Throwable var14) {
                     var6.addSuppressed(var14);
                  }
               } else {
                  r.close();
               }
            }

         }
      }

      return (Map)components;
   }

   public MetadataComponent deserialize(Descriptor descriptor, MetadataType type) throws IOException {
      return (MetadataComponent)this.deserialize(descriptor, EnumSet.of(type)).get(type);
   }

   public Map<MetadataType, MetadataComponent> deserialize(Descriptor descriptor, FileDataInput in, EnumSet<MetadataType> selectedTypes) throws IOException {
      boolean isChecksummed = descriptor.version.hasMetadataChecksum();
      CRC32 crc = new CRC32();
      int length = (int)in.bytesRemaining();
      int count = in.readInt();
      FBUtilities.updateChecksumInt(crc, count);
      maybeValidateChecksum(crc, in, descriptor);
      int[] ordinals = new int[count];
      int[] offsets = new int[count];
      int[] lengths = new int[count];

      int i;
      for(i = 0; i < count; ++i) {
         ordinals[i] = in.readInt();
         FBUtilities.updateChecksumInt(crc, ordinals[i]);
         offsets[i] = in.readInt();
         FBUtilities.updateChecksumInt(crc, offsets[i]);
      }

      maybeValidateChecksum(crc, in, descriptor);
      lengths[count - 1] = length - offsets[count - 1];

      for(i = 0; i < count - 1; ++i) {
         lengths[i] = offsets[i + 1] - offsets[i];
      }

      MetadataType[] allMetadataTypes = MetadataType.values();
      Map<MetadataType, MetadataComponent> components = new EnumMap(MetadataType.class);

      for(int i = 0; i < count; ++i) {
         MetadataType type = allMetadataTypes[ordinals[i]];
         if(!selectedTypes.contains(type)) {
            in.skipBytes(lengths[i]);
         } else {
            byte[] buffer = new byte[isChecksummed?lengths[i] - 4:lengths[i]];
            in.readFully(buffer);
            crc.reset();
            crc.update(buffer);
            maybeValidateChecksum(crc, in, descriptor);
            DataInputBuffer dataInputBuffer = new DataInputBuffer(buffer);
            Throwable var17 = null;

            try {
               components.put(type, type.serializer.deserialize(descriptor.version, dataInputBuffer));
            } catch (Throwable var26) {
               var17 = var26;
               throw var26;
            } finally {
               if(dataInputBuffer != null) {
                  if(var17 != null) {
                     try {
                        dataInputBuffer.close();
                     } catch (Throwable var25) {
                        var17.addSuppressed(var25);
                     }
                  } else {
                     dataInputBuffer.close();
                  }
               }

            }
         }
      }

      return components;
   }

   private static void maybeValidateChecksum(CRC32 crc, FileDataInput in, Descriptor descriptor) throws IOException {
      if(descriptor.version.hasMetadataChecksum()) {
         int actualChecksum = (int)crc.getValue();
         int expectedChecksum = in.readInt();
         if(actualChecksum != expectedChecksum) {
            String filename = descriptor.filenameFor(Component.STATS);
            throw new CorruptSSTableException(new IOException("Checksums do not match for " + filename), filename);
         }
      }
   }

   public void mutateLevel(Descriptor descriptor, int newLevel) throws IOException {
      logger.trace("Mutating {} to level {}", descriptor.filenameFor(Component.STATS), Integer.valueOf(newLevel));
      Map<MetadataType, MetadataComponent> currentComponents = this.deserialize(descriptor, EnumSet.allOf(MetadataType.class));
      StatsMetadata stats = (StatsMetadata)currentComponents.remove(MetadataType.STATS);
      currentComponents.put(MetadataType.STATS, stats.mutateLevel(newLevel));
      this.rewriteSSTableMetadata(descriptor, currentComponents);
   }

   public void mutateRepaired(Descriptor descriptor, long newRepairedAt, UUID newPendingRepair) throws IOException {
      logger.trace("Mutating {} to repairedAt time {} and pendingRepair {}", new Object[]{descriptor.filenameFor(Component.STATS), Long.valueOf(newRepairedAt), newPendingRepair});
      Map<MetadataType, MetadataComponent> currentComponents = this.deserialize(descriptor, EnumSet.allOf(MetadataType.class));
      StatsMetadata stats = (StatsMetadata)currentComponents.remove(MetadataType.STATS);
      currentComponents.put(MetadataType.STATS, stats.mutateRepairedAt(newRepairedAt).mutatePendingRepair(newPendingRepair));
      this.rewriteSSTableMetadata(descriptor, currentComponents);
   }

   private void rewriteSSTableMetadata(Descriptor descriptor, Map<MetadataType, MetadataComponent> currentComponents) throws IOException {
      String filePath = descriptor.tmpFilenameFor(Component.STATS);
      DataOutputStreamPlus out = new BufferedDataOutputStreamPlus(Files.newByteChannel((new File(filePath)).toPath(), new OpenOption[]{StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE}));
      Throwable var5 = null;

      try {
         this.serialize(currentComponents, out, descriptor.version);
         out.flush();
      } catch (Throwable var14) {
         var5 = var14;
         throw var14;
      } finally {
         if(out != null) {
            if(var5 != null) {
               try {
                  out.close();
               } catch (Throwable var13) {
                  var5.addSuppressed(var13);
               }
            } else {
               out.close();
            }
         }

      }

      if(FBUtilities.isWindows) {
         FileUtils.delete(descriptor.filenameFor(Component.STATS));
      }

      FileUtils.renameWithConfirm(filePath, descriptor.filenameFor(Component.STATS));
   }
}
