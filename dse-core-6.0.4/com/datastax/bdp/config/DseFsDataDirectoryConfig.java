package com.datastax.bdp.config;

public class DseFsDataDirectoryConfig {
   public static final DseFsDataDirectoryConfig DEFAULT_DSEFS_DATA_DIR = from((new DseFsOptions.DseFsDataDirectoryOption()).setDir("/var/lib/dsefs/data"));
   private ConfigUtil.StringParamResolver dirParamResolver = new ConfigUtil.StringParamResolver("data_directories.dir");
   private ConfigUtil.MemoryParamResolver minFreeSpaceParamResolver = (new ConfigUtil.MemoryParamResolver("data_directories.min_free_space", Long.valueOf(5368709120L))).withLowerBound(0L);
   private ConfigUtil.DoubleParamResolver storageWeightParamResolver = (new ConfigUtil.DoubleParamResolver("data_directories.storage_weight", Double.valueOf(1.0D))).withLowerBound(0.0D);

   private DseFsDataDirectoryConfig() {
   }

   public static DseFsDataDirectoryConfig from(DseFsOptions.DseFsDataDirectoryOption options) {
      DseFsDataDirectoryConfig result = new DseFsDataDirectoryConfig();
      result.dirParamResolver.withRawParam(options.dir);
      result.minFreeSpaceParamResolver.withRawParam(options.min_free_space);
      result.storageWeightParamResolver.withRawParam(options.storage_weight);
      return result;
   }

   public void validate() {
      this.dirParamResolver.check();
      this.minFreeSpaceParamResolver.check();
      this.storageWeightParamResolver.check();
   }

   public String getDir() {
      return (String)this.dirParamResolver.enable().get();
   }

   public Long getMinFreeSpace() {
      return (Long)this.minFreeSpaceParamResolver.enable().get();
   }

   public Double getStorageWeight() {
      return (Double)this.storageWeightParamResolver.enable().get();
   }
}
