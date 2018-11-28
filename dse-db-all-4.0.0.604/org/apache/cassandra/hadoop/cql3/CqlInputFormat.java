package org.apache.cassandra.hadoop.cql3;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.TokenRange;
import com.google.common.collect.Maps;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.cassandra.dht.ByteOrderedPartitioner;
import org.apache.cassandra.dht.IPartitioner;
import org.apache.cassandra.dht.OrderPreservingPartitioner;
import org.apache.cassandra.dht.Range;
import org.apache.cassandra.dht.Token;
import org.apache.cassandra.hadoop.ColumnFamilySplit;
import org.apache.cassandra.hadoop.ConfigHelper;
import org.apache.cassandra.hadoop.HadoopCompat;
import org.apache.cassandra.hadoop.ReporterWrapper;
import org.apache.cassandra.utils.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CqlInputFormat extends InputFormat<Long, Row> implements org.apache.hadoop.mapred.InputFormat<Long, Row> {
    public static final String MAPRED_TASK_ID = "mapred.task.id";
    private static final Logger logger = LoggerFactory.getLogger(CqlInputFormat.class);
    private String keyspace;
    private String cfName;
    private IPartitioner partitioner;

    public CqlInputFormat() {
    }

    public RecordReader<Long, Row> getRecordReader(InputSplit split, JobConf jobConf, Reporter reporter) throws IOException {
        TaskAttemptContext tac = HadoopCompat.newMapContext(jobConf, TaskAttemptID.forName(jobConf.get("mapred.task.id")), (org.apache.hadoop.mapreduce.RecordReader) null, (RecordWriter) null, (OutputCommitter) null, new ReporterWrapper(reporter), (org.apache.hadoop.mapreduce.InputSplit) null);
        CqlRecordReader recordReader = new CqlRecordReader();
        recordReader.initialize((org.apache.hadoop.mapreduce.InputSplit) split, tac);
        return recordReader;
    }

    public org.apache.hadoop.mapreduce.RecordReader<Long, Row> createRecordReader(org.apache.hadoop.mapreduce.InputSplit arg0, TaskAttemptContext arg1) throws IOException, InterruptedException {
        return new CqlRecordReader();
    }

    protected void validateConfiguration(Configuration conf) {
        if (ConfigHelper.getInputKeyspace(conf) != null && ConfigHelper.getInputColumnFamily(conf) != null) {
            if (ConfigHelper.getInputInitialAddress(conf) == null) {
                throw new UnsupportedOperationException("You must set the initial output address to a Cassandra node with setInputInitialAddress");
            } else if (ConfigHelper.getInputPartitioner(conf) == null) {
                throw new UnsupportedOperationException("You must set the Cassandra partitioner class with setInputPartitioner");
            }
        } else {
            throw new UnsupportedOperationException("you must set the keyspace and table with setInputColumnFamily()");
        }
    }

    public List<org.apache.hadoop.mapreduce.InputSplit> getSplits(final JobContext context) throws IOException {
        final Configuration conf = HadoopCompat.getConfiguration(context);
        this.validateConfiguration(conf);
        this.keyspace = ConfigHelper.getInputKeyspace(conf);
        this.cfName = ConfigHelper.getInputColumnFamily(conf);
        this.partitioner = ConfigHelper.getInputPartitioner(conf);
        CqlInputFormat.logger.trace("partitioner is {}", (Object) this.partitioner);
        final ExecutorService executor = new ThreadPoolExecutor(0, 128, 60L, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        final List<org.apache.hadoop.mapreduce.InputSplit> splits = new ArrayList<org.apache.hadoop.mapreduce.InputSplit>();
        try (final Cluster cluster = CqlConfigHelper.getInputCluster(ConfigHelper.getInputInitialAddress(conf).split(","), conf);
             final Session session = cluster.connect()) {
            final List<Future<List<org.apache.hadoop.mapreduce.InputSplit>>> splitfutures = new ArrayList<Future<List<org.apache.hadoop.mapreduce.InputSplit>>>();
            final Pair<String, String> jobKeyRange = ConfigHelper.getInputKeyRange(conf);
            Range<Token> jobRange = null;
            if (jobKeyRange != null) {
                jobRange = new Range<Token>(this.partitioner.getTokenFactory().fromString(jobKeyRange.left), this.partitioner.getTokenFactory().fromString(jobKeyRange.right));
            }
            final Metadata metadata = cluster.getMetadata();
            final Map<TokenRange, Set<Host>> masterRangeNodes = this.getRangeMap(this.keyspace, metadata);
            for (final TokenRange range : masterRangeNodes.keySet()) {
                if (jobRange == null) {
                    splitfutures.add(executor.submit((Callable<List<org.apache.hadoop.mapreduce.InputSplit>>) new SplitCallable(range, masterRangeNodes.get(range), conf, session)));
                } else {
                    final TokenRange jobTokenRange = this.rangeToTokenRange(metadata, jobRange);
                    if (!range.intersects(jobTokenRange)) {
                        continue;
                    }
                    for (final TokenRange intersection : range.intersectWith(jobTokenRange)) {
                        splitfutures.add(executor.submit((Callable<List<org.apache.hadoop.mapreduce.InputSplit>>) new SplitCallable(intersection, masterRangeNodes.get(range), conf, session)));
                    }
                }
            }
            for (final Future<List<org.apache.hadoop.mapreduce.InputSplit>> futureInputSplits : splitfutures) {
                try {
                    splits.addAll(futureInputSplits.get());
                } catch (Exception e) {
                    throw new IOException("Could not get input splits", e);
                }
            }
        } finally {
            executor.shutdownNow();
        }
        assert splits.size() > 0;
        Collections.shuffle(splits, new Random(System.nanoTime()));
        return splits;
    }

    private TokenRange rangeToTokenRange(Metadata metadata, Range<Token> range) {
        return metadata.newTokenRange(metadata.newToken(this.partitioner.getTokenFactory().toString((Token) range.left)), metadata.newToken(this.partitioner.getTokenFactory().toString((Token) range.right)));
    }

    private Map<TokenRange, Long> getSubSplits(String keyspace, String cfName, TokenRange range, Configuration conf, Session session) {
        int splitSize = ConfigHelper.getInputSplitSize(conf);
        int splitSizeMb = ConfigHelper.getInputSplitSizeInMb(conf);

        try {
            return this.describeSplits(keyspace, cfName, range, splitSize, splitSizeMb, session);
        } catch (Exception var9) {
            throw new RuntimeException(var9);
        }
    }

    private Map<TokenRange, Set<Host>> getRangeMap(String keyspace, Metadata metadata) {
        return (Map) metadata.getTokenRanges().stream().collect(Collectors.toMap((p) -> {
            return p;
        }, (p) -> {
            return metadata.getReplicas('"' + keyspace + '"', p);
        }));
    }

    private Map<TokenRange, Long> describeSplits(final String keyspace, final String table, final TokenRange tokenRange, final int splitSize, final int splitSizeMb, final Session session) {
        final String query = String.format("SELECT mean_partition_size, partitions_count FROM %s.%s WHERE keyspace_name = ? AND table_name = ? AND range_start = ? AND range_end = ?", "system", "size_estimates");
        final ResultSet resultSet = session.execute(query, new Object[]{keyspace, table, tokenRange.getStart().toString(), tokenRange.getEnd().toString()});
        final Row row = resultSet.one();
        long meanPartitionSize = 0L;
        long partitionCount = 0L;
        int splitCount = 0;
        if (row != null) {
            meanPartitionSize = row.getLong("mean_partition_size");
            partitionCount = row.getLong("partitions_count");
            splitCount = ((splitSizeMb > 0) ? ((int) (meanPartitionSize * partitionCount / splitSizeMb / 1024L / 1024L)) : ((int) (partitionCount / splitSize)));
        }
        if (splitCount == 0) {
            final Map<TokenRange, Long> wrappedTokenRange = new HashMap<TokenRange, Long>();
            wrappedTokenRange.put(tokenRange, 128L);
            return wrappedTokenRange;
        }
        final List<TokenRange> splitRanges = (List<TokenRange>) tokenRange.splitEvenly(splitCount);
        final Map<TokenRange, Long> rangesWithLength = Maps.newHashMapWithExpectedSize(splitRanges.size());
        for (final TokenRange range : splitRanges) {
            rangesWithLength.put(range, partitionCount / splitCount);
        }
        return rangesWithLength;
    }

    public InputSplit[] getSplits(JobConf jobConf, int numSplits) throws IOException {
        TaskAttemptContext tac = HadoopCompat.newTaskAttemptContext(jobConf, new TaskAttemptID());
        List<org.apache.hadoop.mapreduce.InputSplit> newInputSplits = this.getSplits(tac);
        InputSplit[] oldInputSplits = new InputSplit[newInputSplits.size()];

        for (int i = 0; i < newInputSplits.size(); ++i) {
            oldInputSplits[i] = (ColumnFamilySplit) newInputSplits.get(i);
        }

        return oldInputSplits;
    }

    class SplitCallable implements Callable<List<org.apache.hadoop.mapreduce.InputSplit>> {
        private final TokenRange tokenRange;
        private final Set<Host> hosts;
        private final Configuration conf;
        private final Session session;

        public SplitCallable(TokenRange tr, Set<Host> hosts, Configuration conf, Session session) {
            this.tokenRange = tr;
            this.hosts = hosts;
            this.conf = conf;
            this.session = session;
        }

        public List<org.apache.hadoop.mapreduce.InputSplit> call() throws Exception {
            ArrayList<org.apache.hadoop.mapreduce.InputSplit> splits = new ArrayList<org.apache.hadoop.mapreduce.InputSplit>();
            Map<TokenRange, Long> subSplits = CqlInputFormat.this.getSubSplits(CqlInputFormat.this.keyspace, CqlInputFormat.this.cfName, this.tokenRange, this.conf, this.session);
            String[] endpoints = new String[this.hosts.size()];
            int endpointIndex = 0;
            for (Host endpoint : this.hosts) {
                endpoints[endpointIndex++] = endpoint.getAddress().getHostName();
            }
            boolean partitionerIsOpp = CqlInputFormat.this.partitioner instanceof OrderPreservingPartitioner || CqlInputFormat.this.partitioner instanceof ByteOrderedPartitioner;
            for (Map.Entry subSplitEntry : subSplits.entrySet()) {
                List<TokenRange> ranges = ((TokenRange) subSplitEntry.getKey()).unwrap();
                for (TokenRange subrange : ranges) {
                    ColumnFamilySplit split = new ColumnFamilySplit(partitionerIsOpp ? subrange.getStart().toString().substring(2) : subrange.getStart().toString(), partitionerIsOpp ? subrange.getEnd().toString().substring(2) : subrange.getEnd().toString(), (Long) subSplitEntry.getValue(), endpoints);
                    logger.trace("adding {}", (Object) split);
                    splits.add(split);
                }
            }
            return splits;
        }
    }
}
