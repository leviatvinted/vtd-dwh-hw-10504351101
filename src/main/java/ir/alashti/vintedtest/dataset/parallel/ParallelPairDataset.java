package ir.alashti.vintedtest.dataset.parallel;

import ir.alashti.vintedtest.dataset.Dataset;
import ir.alashti.vintedtest.dataset.PairDataset;
import ir.alashti.vintedtest.dataset.aop.TriFunction;
import ir.alashti.vintedtest.dataset.udf.Tuple2;
import ir.alashti.vintedtest.dataset.udf.Tuple3;
import ir.alashti.vintedtest.pool.Executor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

public class ParallelPairDataset<K, V> {
    Logger logger = LogManager.getLogger(ParallelPairDataset.class);
    List<PairDataset<K, V>> parallelPairDatasets = new ArrayList<>();
    ThreadPoolExecutor executorPool = Executor.getExecutorPool();

    public ParallelPairDataset<K, V> reduce(TriFunction<K, V> reducer) {
        logger.info("starting reduce...");
        ParallelPairDataset<K, V> parallelPairDataset = new ParallelPairDataset<>();
        List<Future<PairDataset<K, V>>> futures = new ArrayList<>();
        Map<Integer, PairDataset<K, V>> shuffleDataset = Shuffler.shuffleByKey(this.collect());
        logger.info("number of tasks are: {}", shuffleDataset.size());
        for (PairDataset<K, V> pairDataset : shuffleDataset.values()) {
            PairDatasetManager pairDatasetManager = new PairDatasetManager(reducer, null, PairDataset::reduce, null, pairDataset, null);
            futures.add((Future<PairDataset<K, V>>) executorPool.submit(pairDatasetManager));
        }
        ParallelPairDataset<K, V> futureDatasets = getFutureDatasets(parallelPairDataset, futures);
        logger.info("all the tasks are finished.");
        return futureDatasets;
    }


    public ParallelPairDataset<K, V> filter(Function<V, Boolean> filter) {
        logger.info("starting filter...");
        ParallelPairDataset<K, V> parallelPairDataset = new ParallelPairDataset<>();
        List<Future<PairDataset<K, V>>> futures = new ArrayList<>();
        logger.info("number of tasks are: {}", parallelPairDatasets.size());

        for (PairDataset<K, V> pairDataset : parallelPairDatasets) {
            PairDatasetManager pairDatasetManager = new PairDatasetManager(null, filter, null, PairDataset::filter, pairDataset, null);
            futures.add(executorPool.submit(pairDatasetManager));
        }
        ParallelPairDataset<K, V> futureDatasets = getFutureDatasets(parallelPairDataset, futures);
        logger.info("all the tasks are finished.");
        return futureDatasets;
    }

    private ParallelPairDataset<K, V> getFutureDatasets(ParallelPairDataset<K, V> parallelPairDataset, List<Future<PairDataset<K, V>>> futures) {
        for (Future<PairDataset<K, V>> future : futures) {
            try {
                parallelPairDataset.add(future.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        return parallelPairDataset;
    }

    public ParallelDataset<Tuple3<K, V>> join(ParallelPairDataset<K, V> right) {
        logger.info("starting join...");
        ParallelDataset<Tuple3<K, V>> joinParallelPairDataset = new ParallelDataset<>();
        List<Future<Dataset<Tuple3<K, V>>>> futures = new ArrayList<>();
        Map<Integer, PairDataset<K, V>> shuffleDataset = Shuffler.shuffleByKey(this.collect());
        Map<Integer, PairDataset<K, V>> shuffleJoinDataset = Shuffler.shuffleByKey(right.collect());
        Map<Integer, PairDataset<K, V>> smaller;
        Map<Integer, PairDataset<K, V>> larger;
        if (shuffleDataset.size() >= shuffleJoinDataset.size()) {
            smaller = shuffleJoinDataset;
            larger = shuffleDataset;
        } else {
            smaller = shuffleDataset;
            larger = shuffleJoinDataset;
        }
        logger.info("number of tasks are: {}", smaller.size() * larger.size());
        for (Map.Entry<Integer, PairDataset<K, V>> pairDataset : smaller.entrySet()) {
            if (larger.containsKey(pairDataset.getKey())) {
                PairDatasetManager pairDatasetManager = new PairDatasetManager(null, null, null,
                        null, pairDataset.getValue(), larger.get(pairDataset.getKey()));
                pairDatasetManager.setJoinTransformer((pairDataset1, right1) -> pairDataset1.join(right1));
                futures.add((Future<Dataset<Tuple3<K, V>>>) executorPool.submit(pairDatasetManager));
            }
        }
        for (Future<Dataset<Tuple3<K, V>>> future : futures) {
            try {
                joinParallelPairDataset.add(future.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
        logger.info("all the tasks are finished.");
        return joinParallelPairDataset;
    }

    public List<Tuple2<K, V>> collect() {
        List<Tuple2<K, V>> output = new ArrayList<>();
        for (PairDataset<K, V> parallelPairDataset : parallelPairDatasets) {
            for (Map.Entry<K, List<V>> kListEntry : parallelPairDataset.getRows().entrySet()) {
                for (V featureValue : kListEntry.getValue()) {
                    output.add(new Tuple2<>(kListEntry.getKey(), featureValue));
                }
            }
        }
        return output;
    }

    public long count() {
        long count = 0;
        for (PairDataset<K, V> parallelPairDataset : parallelPairDatasets) {
            if (parallelPairDataset.getRows() == null || parallelPairDataset.getRows().size() == 0)
                return count;
            for (List<V> value : parallelPairDataset.getRows().values()) {
                count += value.size();
            }
        }
        return count;
    }

    public boolean add(PairDataset<K, V> kvPairDataset) {
        return parallelPairDatasets.add(kvPairDataset);
    }

    public ParallelPairDataset setHeader(String[] header) {
        parallelPairDatasets.forEach(parallelPairDataset -> parallelPairDataset.setHeader(header));
        return this;
    }
}
