package com.datastax.bdp.db.tools;

import com.datastax.bdp.db.nodesync.NodeSyncService;
import com.datastax.bdp.db.tools.nodesync.CancelValidation;
import com.datastax.bdp.db.tools.nodesync.DisableTracing;
import com.datastax.bdp.db.tools.nodesync.EnableTracing;
import com.datastax.bdp.db.tools.nodesync.InvalidOptionException;
import com.datastax.bdp.db.tools.nodesync.ListValidations;
import com.datastax.bdp.db.tools.nodesync.NodeSyncCommand;
import com.datastax.bdp.db.tools.nodesync.NodeSyncException;
import com.datastax.bdp.db.tools.nodesync.ShowTracing;
import com.datastax.bdp.db.tools.nodesync.SubmitValidation;
import com.datastax.bdp.db.tools.nodesync.Toggle;
import com.datastax.bdp.db.tools.nodesync.TracingStatus;
import com.datastax.driver.core.exceptions.AuthenticationException;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.OperationTimedOutException;
import com.datastax.driver.core.exceptions.ReadTimeoutException;
import com.google.common.base.Throwables;
import io.airlift.airline.Cli;
import io.airlift.airline.Help;
import io.airlift.airline.ParseException;
import io.airlift.airline.Cli.CliBuilder;

public class NodeSync {
   private static final String TOOL_NAME = "nodesync";

   public NodeSync() {
   }

   public static void main(String... args) {
      Runnable runnable = parse(args);
      byte status = 0;

      try {
         runnable.run();
      } catch (NoHostAvailableException | OperationTimedOutException | ReadTimeoutException | AuthenticationException | NodeSyncService.NodeSyncNotRunningException | NodeSyncException var4) {
         printExpectedError(var4);
         status = 1;
      } catch (Throwable var5) {
         printUnexpectedError(var5);
         status = 2;
      }

      System.exit(status);
   }

   private static Runnable parse(String... args) {
      Cli<Runnable> cli = createCli();
      Runnable runnable = null;

      try {
         runnable = (Runnable)cli.parse(args);
         if(runnable instanceof NodeSyncCommand) {
            ((NodeSyncCommand)runnable).validateOptions();
         }
      } catch (InvalidOptionException | ParseException var4) {
         printBadUse(var4);
         System.exit(1);
      } catch (Throwable var5) {
         printUnexpectedError(Throwables.getRootCause(var5));
         System.exit(2);
      }

      return runnable;
   }

   private static Cli<Runnable> createCli() {
      CliBuilder<Runnable> builder = Cli.builder("nodesync");
      builder.withDescription("Manage NodeSync service at cluster level").withDefaultCommand(Help.class).withCommand(Help.class).withCommand(Toggle.Enable.class).withCommand(Toggle.Disable.class);
      builder.withGroup("validation").withDescription("Monitor/manage user-triggered validations").withDefaultCommand(Help.class).withCommand(SubmitValidation.class).withCommand(CancelValidation.class).withCommand(ListValidations.class);
      builder.withGroup("tracing").withDescription("Enable/disable tracing for NodeSync").withDefaultCommand(Help.class).withCommand(EnableTracing.class).withCommand(DisableTracing.class).withCommand(TracingStatus.class).withCommand(ShowTracing.class);
      return builder.build();
   }

   private static void printBadUse(Exception e) {
      System.err.printf("%s: %s%n", new Object[]{"nodesync", e.getMessage()});
      System.err.printf("See '%s help' or '%s help <command>'.%n", new Object[]{"nodesync", "nodesync"});
   }

   private static void printExpectedError(Throwable e) {
      System.err.println("Error: " + e.getMessage());
   }

   private static void printUnexpectedError(Throwable e) {
      System.err.printf("Unexpected error: %s (this indicates a bug, please report to DataStax support along with the following stack trace)%n", new Object[]{e.getMessage()});
      System.err.println("-- StackTrace --");
      e.printStackTrace();
   }
}
