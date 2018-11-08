package org.apache.cassandra.tools.nodetool;

import io.airlift.airline.Arguments;
import io.airlift.airline.Command;
import io.airlift.airline.Option;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.cassandra.tools.NodeProbe;
import org.apache.cassandra.tools.NodeTool;

@Command(
   name = "compact",
   description = "Force a (major) compaction on one or more tables or user-defined compaction on given SSTables"
)
public class Compact extends NodeTool.NodeToolCmd {
   @Arguments(
      usage = "[<keyspace> <tables>...] or <SSTable file>...",
      description = "The keyspace followed by one or many tables or list of SSTable data files when using --user-defined"
   )
   private List<String> args = new ArrayList();
   @Option(
      title = "split_output",
      name = {"-s", "--split-output"},
      description = "Use -s to not create a single big file"
   )
   private boolean splitOutput = false;
   @Option(
      title = "user-defined",
      name = {"--user-defined"},
      description = "Use --user-defined to submit listed files for user-defined compaction"
   )
   private boolean userDefined = false;
   @Option(
      title = "start_token",
      name = {"-st", "--start-token"},
      description = "Use -st to specify a token at which the compaction range starts"
   )
   private String startToken = "";
   @Option(
      title = "end_token",
      name = {"-et", "--end-token"},
      description = "Use -et to specify a token at which compaction range ends"
   )
   private String endToken = "";

   public Compact() {
   }

   public void execute(NodeProbe probe) {
      boolean tokenProvided = !this.startToken.isEmpty() || !this.endToken.isEmpty();
      if(!this.splitOutput || !this.userDefined && !tokenProvided) {
         if(this.userDefined && tokenProvided) {
            throw new RuntimeException("Invalid option combination: Can not provide tokens when using user-defined");
         } else if(this.userDefined) {
            try {
               String userDefinedFiles = String.join(",", this.args);
               probe.forceUserDefinedCompaction(userDefinedFiles);
            } catch (Exception var8) {
               throw new RuntimeException("Error occurred during user defined compaction", var8);
            }
         } else {
            List<String> keyspaces = this.parseOptionalKeyspace(this.args, probe);
            String[] tableNames = this.parseOptionalTables(this.args);
            Iterator var5 = keyspaces.iterator();

            while(var5.hasNext()) {
               String keyspace = (String)var5.next();

               try {
                  if(tokenProvided) {
                     probe.forceKeyspaceCompactionForTokenRange(keyspace, this.startToken, this.endToken, tableNames);
                  } else {
                     probe.forceKeyspaceCompaction(this.splitOutput, keyspace, tableNames);
                  }
               } catch (Exception var9) {
                  throw new RuntimeException("Error occurred during compaction", var9);
               }
            }

         }
      } else {
         throw new RuntimeException("Invalid option combination: Can not use split-output here");
      }
   }
}
