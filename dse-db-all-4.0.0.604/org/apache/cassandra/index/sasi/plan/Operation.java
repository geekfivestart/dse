package org.apache.cassandra.index.sasi.plan;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.ListMultimap;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import org.apache.cassandra.cql3.Operator;
import org.apache.cassandra.db.filter.RowFilter;
import org.apache.cassandra.db.rows.Row;
import org.apache.cassandra.db.rows.Unfiltered;
import org.apache.cassandra.index.sasi.analyzer.AbstractAnalyzer;
import org.apache.cassandra.index.sasi.conf.ColumnIndex;
import org.apache.cassandra.index.sasi.disk.Token;
import org.apache.cassandra.index.sasi.utils.RangeIntersectionIterator;
import org.apache.cassandra.index.sasi.utils.RangeIterator;
import org.apache.cassandra.index.sasi.utils.RangeUnionIterator;
import org.apache.cassandra.schema.ColumnMetadata;
import org.apache.cassandra.schema.IndexMetadata;
import org.apache.cassandra.utils.FBUtilities;

public class Operation extends RangeIterator<Long, Token> {
   private final QueryController controller;
   protected final Operation.OperationType op;
   protected final ListMultimap<ColumnMetadata, Expression> expressions;
   protected final RangeIterator<Long, Token> range;
   protected Operation left;
   protected Operation right;

   private Operation(Operation.OperationType operation, QueryController controller, ListMultimap<ColumnMetadata, Expression> expressions, RangeIterator<Long, Token> range, Operation left, Operation right) {
      super(range);
      this.op = operation;
      this.controller = controller;
      this.expressions = expressions;
      this.range = range;
      this.left = left;
      this.right = right;
   }

   public boolean satisfiedBy(Unfiltered currentCluster, Row staticRow, boolean allowMissingColumns) {
      boolean sideL;
      boolean sideR;
      if(this.expressions != null && !this.expressions.isEmpty()) {
         sideL = this.localSatisfiedBy(currentCluster, staticRow, allowMissingColumns);
         if(this.right == null) {
            return sideL;
         }

         sideR = this.right.satisfiedBy(currentCluster, staticRow, allowMissingColumns);
      } else {
         sideL = this.left != null && this.left.satisfiedBy(currentCluster, staticRow, allowMissingColumns);
         sideR = this.right != null && this.right.satisfiedBy(currentCluster, staticRow, allowMissingColumns);
         if(this.left == null) {
            return sideR;
         }
      }

      return this.op.apply(sideL, sideR);
   }

   private boolean localSatisfiedBy(Unfiltered currentCluster, Row staticRow, boolean allowMissingColumns) {
      if(currentCluster != null && currentCluster.isRow()) {
         int now = FBUtilities.nowInSeconds();
         boolean result = false;
         int idx = 0;
         Iterator var7 = this.expressions.keySet().iterator();

         while(true) {
            ColumnMetadata column;
            do {
               if(!var7.hasNext()) {
                  return idx == 0 || result;
               }

               column = (ColumnMetadata)var7.next();
            } while(column.kind == ColumnMetadata.Kind.PARTITION_KEY);

            ByteBuffer value = ColumnIndex.getValueOf(column, column.kind == ColumnMetadata.Kind.STATIC?staticRow:(Row)currentCluster, now);
            boolean isMissingColumn = value == null;
            if(!allowMissingColumns && isMissingColumn) {
               throw new IllegalStateException("All indexed columns should be included into the column slice, missing: " + column);
            }

            boolean isMatch = false;
            List<Expression> filters = this.expressions.get(column);

            for(int i = filters.size() - 1; i >= 0; --i) {
               Expression expression = (Expression)filters.get(i);
               isMatch = !isMissingColumn && expression.isSatisfiedBy(value);
               if(expression.getOp() == Expression.Op.NOT_EQ) {
                  isMatch = !isMatch;
                  if(!isMatch) {
                     break;
                  }
               } else if(isMatch || isMissingColumn) {
                  break;
               }
            }

            if(idx++ == 0) {
               result = isMatch;
            } else {
               result = this.op.apply(result, isMatch);
               if(this.op == Operation.OperationType.AND && !result) {
                  return false;
               }
            }
         }
      } else {
         return false;
      }
   }

   @VisibleForTesting
   protected static ListMultimap<ColumnMetadata, Expression> analyzeGroup(QueryController controller, Operation.OperationType op, List<RowFilter.Expression> expressions) {
      ListMultimap<ColumnMetadata, Expression> analyzed = ArrayListMultimap.create();
      Collections.sort(expressions, (a, b) -> {
         int cmp = a.column().compareTo(b.column());
         return cmp == 0?-Integer.compare(getPriority(a.operator()), getPriority(b.operator())):cmp;
      });
      Iterator var4 = expressions.iterator();

      while(true) {
         while(var4.hasNext()) {
            RowFilter.Expression e = (RowFilter.Expression)var4.next();
            ColumnIndex columnIndex = controller.getIndex(e);
            List<Expression> perColumn = analyzed.get(e.column());
            if(columnIndex == null) {
               columnIndex = new ColumnIndex(controller.getKeyValidator(), e.column(), (IndexMetadata)null);
            }

            AbstractAnalyzer analyzer = columnIndex.getAnalyzer();
            analyzer.reset(e.getIndexValue().duplicate());
            boolean isMultiExpression = false;
            switch(null.$SwitchMap$org$apache$cassandra$cql3$Operator[e.operator().ordinal()]) {
            case 1:
               isMultiExpression = false;
               break;
            case 2:
            case 3:
            case 4:
            case 5:
               isMultiExpression = true;
               break;
            case 6:
               isMultiExpression = perColumn.size() == 0 || perColumn.size() > 1 || perColumn.size() == 1 && ((Expression)perColumn.get(0)).getOp() == Expression.Op.NOT_EQ;
            }

            if(isMultiExpression) {
               while(analyzer.hasNext()) {
                  ByteBuffer token = analyzer.next();
                  perColumn.add((new Expression(controller, columnIndex)).add(e.operator(), token));
               }
            } else {
               Expression range;
               if(perColumn.size() != 0 && op == Operation.OperationType.AND) {
                  range = (Expression)Iterables.getLast(perColumn);
               } else {
                  perColumn.add(range = new Expression(controller, columnIndex));
               }

               while(analyzer.hasNext()) {
                  range.add(e.operator(), analyzer.next());
               }
            }
         }

         return analyzed;
      }
   }

   private static int getPriority(Operator op) {
      switch(null.$SwitchMap$org$apache$cassandra$cql3$Operator[op.ordinal()]) {
      case 1:
         return 5;
      case 2:
      case 3:
      case 4:
      case 5:
         return 4;
      case 6:
         return 1;
      case 7:
      case 8:
         return 3;
      case 9:
      case 10:
         return 2;
      default:
         return 0;
      }
   }

   protected Token computeNext() {
      return this.range != null && this.range.hasNext()?(Token)this.range.next():(Token)this.endOfData();
   }

   protected void performSkipTo(Long nextToken) {
      if(this.range != null) {
         this.range.skipTo(nextToken);
      }

   }

   public void close() throws IOException {
      this.controller.releaseIndexes(this);
   }

   public static class Builder {
      private final QueryController controller;
      protected final Operation.OperationType op;
      protected final List<RowFilter.Expression> expressions;
      protected Operation.Builder left;
      protected Operation.Builder right;

      public Builder(Operation.OperationType operation, QueryController controller, RowFilter.Expression... columns) {
         this.op = operation;
         this.controller = controller;
         this.expressions = new ArrayList();
         Collections.addAll(this.expressions, columns);
      }

      public Operation.Builder setRight(Operation.Builder operation) {
         this.right = operation;
         return this;
      }

      public Operation.Builder setLeft(Operation.Builder operation) {
         this.left = operation;
         return this;
      }

      public void add(RowFilter.Expression e) {
         this.expressions.add(e);
      }

      public void add(Collection<RowFilter.Expression> newExpressions) {
         if(this.expressions != null) {
            this.expressions.addAll(newExpressions);
         }

      }

      public Operation complete() {
         if(!this.expressions.isEmpty()) {
            ListMultimap<ColumnMetadata, Expression> analyzedExpressions = Operation.analyzeGroup(this.controller, this.op, this.expressions);
            RangeIterator.Builder<Long, Token> range = this.controller.getIndexes(this.op, analyzedExpressions.values());
            Operation rightOp = null;
            if(this.right != null) {
               rightOp = this.right.complete();
               range.add((RangeIterator)rightOp);
            }

            return new Operation(this.op, this.controller, analyzedExpressions, range.build(), (Operation)null, rightOp, null);
         } else {
            Operation leftOp = null;
            Operation rightOp = null;
            boolean leftIndexes = false;
            boolean rightIndexes = false;
            if(this.left != null) {
               leftOp = this.left.complete();
               leftIndexes = leftOp != null && leftOp.range != null;
            }

            if(this.right != null) {
               rightOp = this.right.complete();
               rightIndexes = rightOp != null && rightOp.range != null;
            }

            Object join;
            if(leftIndexes && !rightIndexes) {
               join = leftOp;
            } else if(!leftIndexes && rightIndexes) {
               join = rightOp;
            } else {
               if(!leftIndexes) {
                  throw new AssertionError("both sub-trees have 0 indexes.");
               }

               RangeIterator.Builder<Long, Token> builder = this.op == Operation.OperationType.OR?RangeUnionIterator.builder():RangeIntersectionIterator.builder();
               join = ((RangeIterator.Builder)builder).add((RangeIterator)leftOp).add((RangeIterator)rightOp).build();
            }

            return new Operation(this.op, this.controller, (ListMultimap)null, (RangeIterator)join, leftOp, rightOp, null);
         }
      }
   }

   public static class TreeBuilder {
      private final QueryController controller;
      final Operation.Builder root;
      Operation.Builder subtree;

      public TreeBuilder(QueryController controller) {
         this.controller = controller;
         this.root = new Operation.Builder(Operation.OperationType.AND, controller, new RowFilter.Expression[0]);
         this.subtree = this.root;
      }

      public Operation.TreeBuilder add(Collection<RowFilter.Expression> expressions) {
         if(expressions != null) {
            expressions.forEach(this::add);
         }

         return this;
      }

      public Operation.TreeBuilder add(RowFilter.Expression exp) {
         if(exp.operator().isLike()) {
            this.addToSubTree(exp);
         } else {
            this.root.add(exp);
         }

         return this;
      }

      private void addToSubTree(RowFilter.Expression exp) {
         Operation.Builder likeOperation = new Operation.Builder(Operation.OperationType.OR, this.controller, new RowFilter.Expression[0]);
         likeOperation.add(exp);
         if(this.subtree.right == null) {
            this.subtree.setRight(likeOperation);
         } else {
            if(this.subtree.left != null) {
               throw new IllegalStateException("Both trees are full");
            }

            Operation.Builder newSubtree = new Operation.Builder(Operation.OperationType.AND, this.controller, new RowFilter.Expression[0]);
            this.subtree.setLeft(newSubtree);
            newSubtree.setRight(likeOperation);
            this.subtree = newSubtree;
         }

      }

      public Operation complete() {
         return this.root.complete();
      }
   }

   public static enum OperationType {
      AND,
      OR;

      private OperationType() {
      }

      public boolean apply(boolean a, boolean b) {
         switch(null.$SwitchMap$org$apache$cassandra$index$sasi$plan$Operation$OperationType[this.ordinal()]) {
         case 1:
            return a | b;
         case 2:
            return a & b;
         default:
            throw new AssertionError();
         }
      }
   }
}
