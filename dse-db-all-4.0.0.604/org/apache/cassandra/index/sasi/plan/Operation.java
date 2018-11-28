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
        if (this.expressions != null && !this.expressions.isEmpty()) {
            sideL = this.localSatisfiedBy(currentCluster, staticRow, allowMissingColumns);
            if (this.right == null) {
                return sideL;
            }

            sideR = this.right.satisfiedBy(currentCluster, staticRow, allowMissingColumns);
        } else {
            sideL = this.left != null && this.left.satisfiedBy(currentCluster, staticRow, allowMissingColumns);
            sideR = this.right != null && this.right.satisfiedBy(currentCluster, staticRow, allowMissingColumns);
            if (this.left == null) {
                return sideR;
            }
        }

        return this.op.apply(sideL, sideR);
    }

    private boolean localSatisfiedBy(Unfiltered currentCluster, Row staticRow, boolean allowMissingColumns) {
        if (currentCluster != null && currentCluster.isRow()) {
            int now = FBUtilities.nowInSeconds();
            boolean result = false;
            int idx = 0;
            Iterator var7 = this.expressions.keySet().iterator();

            while (true) {
                ColumnMetadata column;
                do {
                    if (!var7.hasNext()) {
                        return idx == 0 || result;
                    }

                    column = (ColumnMetadata) var7.next();
                } while (column.kind == ColumnMetadata.Kind.PARTITION_KEY);

                ByteBuffer value = ColumnIndex.getValueOf(column, column.kind == ColumnMetadata.Kind.STATIC ? staticRow : (Row) currentCluster, now);
                boolean isMissingColumn = value == null;
                if (!allowMissingColumns && isMissingColumn) {
                    throw new IllegalStateException("All indexed columns should be included into the column slice, missing: " + column);
                }

                boolean isMatch = false;
                List<Expression> filters = this.expressions.get(column);

                for (int i = filters.size() - 1; i >= 0; --i) {
                    Expression expression = (Expression) filters.get(i);
                    isMatch = !isMissingColumn && expression.isSatisfiedBy(value);
                    if (expression.getOp() == Expression.Op.NOT_EQ) {
                        isMatch = !isMatch;
                        if (!isMatch) {
                            break;
                        }
                    } else if (isMatch || isMissingColumn) {
                        break;
                    }
                }

                if (idx++ == 0) {
                    result = isMatch;
                } else {
                    result = this.op.apply(result, isMatch);
                    if (this.op == Operation.OperationType.AND && !result) {
                        return false;
                    }
                }
            }
        } else {
            return false;
        }
    }

    @VisibleForTesting
    protected static ListMultimap<ColumnMetadata, Expression> analyzeGroup(QueryController controller, OperationType op, List<RowFilter.Expression> expressions) {
        ArrayListMultimap analyzed = ArrayListMultimap.create();
        Collections.sort(expressions, (a, b) -> {
            int cmp = a.column().compareTo(b.column());
            return cmp == 0 ? -Integer.compare(Operation.getPriority(a.operator()), Operation.getPriority(b.operator())) : cmp;
        });
        for (RowFilter.Expression e : expressions) {
            Expression range;
            ColumnIndex columnIndex = controller.getIndex(e);
            List perColumn = analyzed.get((Object) e.column());
            if (columnIndex == null) {
                columnIndex = new ColumnIndex(controller.getKeyValidator(), e.column(), null);
            }
            AbstractAnalyzer analyzer = columnIndex.getAnalyzer();
            analyzer.reset(e.getIndexValue().duplicate());
            boolean isMultiExpression = false;
            switch (e.operator()) {
                case EQ: {
                    isMultiExpression = false;
                    break;
                }
                case LIKE_PREFIX:
                case LIKE_SUFFIX:
                case LIKE_CONTAINS:
                case LIKE_MATCHES: {
                    isMultiExpression = true;
                    break;
                }
                case NEQ: {
                    isMultiExpression = perColumn.size() == 0 || perColumn.size() > 1 || perColumn.size() == 1 && ((Expression) perColumn.get(0)).getOp() == Expression.Op.NOT_EQ;
                }
            }
            if (isMultiExpression) {
                while (analyzer.hasNext()) {
                    ByteBuffer token = analyzer.next();
                    perColumn.add(new Expression(controller, columnIndex).add(e.operator(), token));
                }
                continue;
            }
            if (perColumn.size() == 0 || op != OperationType.AND) {
                range = new Expression(controller, columnIndex);
                perColumn.add(range);
            } else {
                range = (Expression) Iterables.getLast((Iterable) perColumn);
            }
            while (analyzer.hasNext()) {
                range.add(e.operator(), analyzer.next());
            }
        }
        return analyzed;
    }

    private static int getPriority(Operator op) {
        switch (op) {
            case EQ: {
                return 5;
            }
            case LIKE_PREFIX:
            case LIKE_SUFFIX:
            case LIKE_CONTAINS:
            case LIKE_MATCHES: {
                return 4;
            }
            case GTE:
            case GT: {
                return 3;
            }
            case LTE:
            case LT: {
                return 2;
            }
            case NEQ: {
                return 1;
            }
        }
        return 0;
    }

    protected Token computeNext() {
        return this.range != null && this.range.hasNext() ? (Token) this.range.next() : (Token) this.endOfData();
    }

    protected void performSkipTo(Long nextToken) {
        if (this.range != null) {
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
            if (this.expressions != null) {
                this.expressions.addAll(newExpressions);
            }

        }

        public Operation complete() {
            RangeIterator join;
            if (!this.expressions.isEmpty()) {
                ListMultimap<ColumnMetadata, Expression> analyzedExpressions = Operation.analyzeGroup(this.controller, this.op, this.expressions);
                RangeIterator.Builder<Long, Token> range = this.controller.getIndexes(this.op, analyzedExpressions.values());
                Operation rightOp = null;
                if (this.right != null) {
                    rightOp = this.right.complete();
                    range.add(rightOp);
                }
                return new Operation(this.op, this.controller, analyzedExpressions, range.build(), null, rightOp);
            }
            Operation leftOp = null;
            Operation rightOp = null;
            boolean leftIndexes = false;
            boolean rightIndexes = false;
            if (this.left != null) {
                leftOp = this.left.complete();
                boolean bl = leftIndexes = leftOp != null && leftOp.range != null;
            }
            if (this.right != null) {
                rightOp = this.right.complete();
                boolean bl = rightIndexes = rightOp != null && rightOp.range != null;
            }
            if (leftIndexes && !rightIndexes) {
                join = leftOp;
            } else if (!leftIndexes && rightIndexes) {
                join = rightOp;
            } else if (leftIndexes) {
                RangeIterator.Builder builder = this.op == OperationType.OR ? RangeUnionIterator.builder() : RangeIntersectionIterator.builder();
                join = builder.add(leftOp).add(rightOp).build();
            } else {
                throw new AssertionError((Object) "both sub-trees have 0 indexes.");
            }
            return new Operation(this.op, this.controller, null, join, leftOp, rightOp);
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
            if (expressions != null) {
                expressions.forEach(this::add);
            }

            return this;
        }

        public Operation.TreeBuilder add(RowFilter.Expression exp) {
            if (exp.operator().isLike()) {
                this.addToSubTree(exp);
            } else {
                this.root.add(exp);
            }

            return this;
        }

        private void addToSubTree(RowFilter.Expression exp) {
            Operation.Builder likeOperation = new Operation.Builder(Operation.OperationType.OR, this.controller, new RowFilter.Expression[0]);
            likeOperation.add(exp);
            if (this.subtree.right == null) {
                this.subtree.setRight(likeOperation);
            } else {
                if (this.subtree.left != null) {
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
            switch (this) {
                case OR: {
                    return a | b;
                }
                case AND: {
                    return a & b;
                }
            }
            throw new AssertionError();
        }
    }
}
