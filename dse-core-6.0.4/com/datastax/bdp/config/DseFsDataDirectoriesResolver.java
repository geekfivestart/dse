package com.datastax.bdp.config;

import java.util.Iterator;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.commons.lang3.tuple.Pair;

public class DseFsDataDirectoriesResolver extends ConfigUtil.ParamResolver<Set<DseFsDataDirectoryConfig>> {
   public DseFsDataDirectoriesResolver(String name, Set<DseFsDataDirectoryConfig> defaultValue) {
      super(name, defaultValue);
   }

   protected boolean isNotEmpty(Object rawParam) {
      return rawParam != null && !((Set)rawParam).isEmpty();
   }

   protected Pair<Set<DseFsDataDirectoryConfig>, Boolean> convert(Object rawParam) throws ConfigurationException {
      if(rawParam instanceof Set) {
         Set<DseFsOptions.DseFsDataDirectoryOption> rawConfigs = (Set)rawParam;
         Set<DseFsDataDirectoryConfig> convertedConfigs = (Set)rawConfigs.stream().map(DseFsDataDirectoryConfig::from).collect(Collectors.toSet());
         return Pair.of(convertedConfigs, Boolean.valueOf(false));
      } else {
         return (Pair)this.fail("A set must have a type of set");
      }
   }

   protected Pair<Set<DseFsDataDirectoryConfig>, Boolean> validateOrGetDefault(Pair<Set<DseFsDataDirectoryConfig>, Boolean> value) throws ConfigurationException {
      if(value != null && this.isNotEmpty(value.getLeft())) {
         Iterator var2 = ((Set)value.getLeft()).iterator();

         while(var2.hasNext()) {
            DseFsDataDirectoryConfig directoriesOptions = (DseFsDataDirectoryConfig)var2.next();
            directoriesOptions.validate();
         }

         return value;
      } else {
         return (Pair)this.fail("Dsefs directories list must not be empty");
      }
   }
}
