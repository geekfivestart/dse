package com.datastax.bdp.db.audit.cql3;

import java.util.LinkedList;
import java.util.List;
import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.antlr.runtime.Token;
import org.apache.cassandra.cql3.CqlLexer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchStatementUtils {
   private static final Logger logger = LoggerFactory.getLogger(BatchStatementUtils.class);

   public BatchStatementUtils() {
   }

   public static List<BatchStatementUtils.Meta> decomposeBatchStatement(String queryStr) {
      CharStream stream = new ANTLRStringStream(queryStr);
      CqlLexer lexer = new CqlLexer(stream);
      boolean pastProlog = false;
      List<BatchStatementUtils.Meta> stmts = new LinkedList();
      StringBuilder builder = new StringBuilder();
      int numVars = 0;
      int varsOffset = 0;

      for(Token t = lexer.nextToken(); t.getType() != -1; t = lexer.nextToken()) {
         if(t.getType() != 188) {
            String query;
            if(isStatementStart(t)) {
               if(pastProlog) {
                  query = builder.toString().trim().replaceAll(" \\. ", "\\.");
                  stmts.add(new BatchStatementUtils.Meta(query, varsOffset, numVars - varsOffset));
                  builder = new StringBuilder();
               }

               pastProlog = true;
               varsOffset = numVars;
            }

            if(pastProlog) {
               if(t.getType() == 33) {
                  query = builder.toString().trim().replaceAll(" \\. ", "\\.");
                  stmts.add(new BatchStatementUtils.Meta(query, varsOffset, numVars - varsOffset));
                  break;
               }

               if(t.getType() == 182) {
                  builder.append('\'').append(t.getText()).append('\'').append(' ');
               } else {
                  builder.append(t.getText()).append(' ');
               }
            }

            if(t.getType() == 177) {
               ++numVars;
            }
         }
      }

      return stmts;
   }

   private static boolean isStatementStart(Token t) {
      switch(t.getType()) {
      case 57:
      case 85:
      case 156:
         return true;
      default:
         return false;
      }
   }

   public static class Meta {
      public final String query;
      public final int varsOffset;
      public final int varsSize;

      public Meta(String query, int varsOffset, int varsSize) {
         this.query = query;
         this.varsOffset = varsOffset;
         this.varsSize = varsSize;
      }

      public <T> List<T> getSubList(List<T> l) {
         return l.subList(this.varsOffset, this.varsOffset + this.varsSize);
      }
   }
}
