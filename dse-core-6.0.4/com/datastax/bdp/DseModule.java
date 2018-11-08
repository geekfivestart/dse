package com.datastax.bdp;

import com.datastax.bdp.cassandra.db.tiered.TieredStorageYamlConfig;
import com.datastax.bdp.config.Config;
import com.datastax.bdp.config.DseFsOptions;
import com.datastax.bdp.config.DseYamlPropertyUtils;
import com.datastax.bdp.config.KmipHostOptions;
import com.datastax.bdp.ioc.DseInjector;
import com.datastax.bdp.server.AbstractDseModule;
import com.datastax.bdp.server.DseDaemon;
import com.datastax.bdp.server.SystemInfo;
import com.datastax.bdp.system.PerformanceObjectsKeyspace;
import com.google.inject.AbstractModule;
import com.google.inject.CreationException;
import java.util.LinkedList;
import java.util.List;
import javax.validation.constraints.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.constructor.Constructor;
import org.yaml.snakeyaml.representer.Representer;

public class DseModule extends AbstractDseModule {
   private static final Logger logger = LoggerFactory.getLogger(DseModule.class);
   private static final int ERROR_CODE = 1;

   public DseModule() {
   }

   @NotNull
   public static Yaml createParser() {
      Representer representer = new Representer();
      representer.setPropertyUtils(new DseYamlPropertyUtils());
      TypeDescription configDescription = new TypeDescription(Config.class);
      configDescription.putMapPropertyType("kmip_hosts", String.class, KmipHostOptions.class);
      configDescription.putMapPropertyType("tiered_storage_options", String.class, TieredStorageYamlConfig.class);
      TypeDescription tsDescription = new TypeDescription(TieredStorageYamlConfig.class);
      tsDescription.putListPropertyType("tiers", TieredStorageYamlConfig.Tier.class);
      tsDescription.putMapPropertyType("local_options", String.class, String.class);
      TypeDescription tierDescription = new TypeDescription(TieredStorageYamlConfig.Tier.class);
      tierDescription.putListPropertyType("paths", String.class);
      tierDescription.putMapPropertyType("compaction_options", String.class, String.class);
      TypeDescription dseFs = new TypeDescription(DseFsOptions.class);
      dseFs.putListPropertyType("data_directories", DseFsOptions.DseFsDataDirectoryOption.class);
      Constructor constructor = new Constructor(Config.class);
      constructor.addTypeDescription(tierDescription);
      constructor.addTypeDescription(tsDescription);
      constructor.addTypeDescription(configDescription);
      constructor.addTypeDescription(dseFs);
      return new Yaml(constructor, representer);
   }

   protected void configure() {
      super.configure();
      this.requestStaticInjection(new Class[]{PerformanceObjectsKeyspace.class});
   }

   public static void main(String[] args) {
      try {
         logger.info("Loading DSE module");
         DseDaemon dseDaemon = (DseDaemon)DseInjector.get().getInstance(DseDaemon.class);
         logger.debug("Activating DSE Daemon");
         dseDaemon.activate(true);
      } catch (CreationException var2) {
         logger.error("{}. Exiting...", var2);
         System.exit(1);
      } catch (NoClassDefFoundError var3) {
         logger.error("{}. Exiting...", var3.getLocalizedMessage());
         System.exit(1);
      } catch (ExceptionInInitializerError var4) {
         logger.error("Unable to start server. Exiting...", var4.getCause());
         System.exit(1);
      } catch (Throwable var5) {
         logger.error("Unable to start server. Exiting...", var5);
         System.exit(1);
      }

   }

   public String[] getOptionalModuleNames() {
      List<String> names = new LinkedList();
      names.add("com.datastax.bdp.DseSearchModule");
      if(SystemInfo.isGraphNode()) {
         names.add("com.datastax.bdp.DseGraphModule");
      } else {
         logger.debug("Not loading DSE Graph module");
      }

      names.add("com.datastax.bdp.DseAnalyticsModule");
      names.add("com.datastax.bdp.advrep.AdvRepModule");
      names.add("com.datastax.bdp.DseFsModule");
      return (String[])names.toArray(new String[names.size()]);
   }

   public AbstractModule[] getRequiredModules() {
      return new AbstractModule[]{new DseCoreModule()};
   }
}
