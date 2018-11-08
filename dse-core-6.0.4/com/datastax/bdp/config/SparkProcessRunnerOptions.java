package com.datastax.bdp.config;

public class SparkProcessRunnerOptions {
   public String runner_type;
   public RunAsRunnerOptions run_as_runner_options = new RunAsRunnerOptions();

   public SparkProcessRunnerOptions() {
   }
}
