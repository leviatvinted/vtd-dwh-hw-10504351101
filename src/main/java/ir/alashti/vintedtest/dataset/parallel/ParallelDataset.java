package ir.alashti.vintedtest.dataset.parallel;

import ir.alashti.vintedtest.dataset.Dataset;
import ir.alashti.vintedtest.dataset.PairDataset;
import ir.alashti.vintedtest.dataset.Row;
import ir.alashti.vintedtest.dataset.udf.Tuple2;
import ir.alashti.vintedtest.pool.Executor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.function.Function;

public class ParallelDataset<T> {
    Logger logger = LogManager.getLogger(ParallelDataset.class);
    List<Dataset<T>> parallelDataset = new ArrayList<>();
    ThreadPoolExecutor executorPool = Executor.getExecutorPool();

    public <I, O> ParallelDataset<O> map(Function<I, O> mapper) {
        logger.info("starting map...");
        ParallelDataset<O> mappedDataset = new ParallelDataset<>();
        List<Future<Dataset<O>>> futures = new ArrayList<>();
        logger.info("number of tasks are: {}", parallelDataset.size());
        for (Dataset<T> dataset : parallelDataset) {
            DatasetManager datasetManager = new DatasetManager(mapper, Dataset::map, null, dataset);
            futures.add((Future<Dataset<O>>) executorPool.submit(datasetManager));
        }
        getFutureDataset(mappedDataset, futures);
        logger.info("all the tasks are finished.");
        return mappedDataset;
    }

    public <I, K, V> ParallelPairDataset<K, V> mapToPair(Function<I, Tuple2<K, V>> mapper) {
        logger.info("starting mapToPair...");
        ParallelPairDataset<K, V> mappedPairDataset = new ParallelPairDataset<>();
        List<Future<PairDataset<K, V>>> futures = new ArrayList<>();
        logger.info("number of tasks are: {}", parallelDataset.size());
        for (Dataset<T> dataset : parallelDataset) {
            DatasetManager datasetManager = new DatasetManager(mapper, null, Dataset::mapToPair, dataset);
            futures.add((Future<PairDataset<K, V>>) executorPool.submit(datasetManager));
        }
        getFutures(mappedPairDataset, futures);
        logger.info("all the tasks are finished.");
        return mappedPairDataset;
    }

    public <V> ParallelDataset<V> filter(Function<V, Boolean> filter) {
        logger.info("starting map...");
        ParallelDataset<V> output = new ParallelDataset<>();
        List<Future<Dataset<V>>> futures = new ArrayList<>();
        logger.info("number of tasks are: {}", parallelDataset.size());
        for (Dataset<T> dataset : parallelDataset) {
            DatasetManager datasetManager = new DatasetManager(filter, Dataset::filter, null, dataset);
            futures.add((Future<Dataset<V>>) executorPool.submit(datasetManager));
        }
        getFutureDataset(output, futures);
        logger.info("all the tasks are finished.");
        return output;
    }

    public long count() {
        long count = 0;
        for (Dataset<T> dataset : parallelDataset) {
            count += dataset.count();
        }
        return count;
    }

    public List<Row> collect() {
        List<Row> output = new ArrayList<>();
        for (Dataset dataset : parallelDataset) {
            output.addAll(dataset.getRows());
        }
        return output;
    }

    public boolean add(Dataset<T> tDataset) {
        return parallelDataset.add(tDataset);
    }

    public ParallelDataset setHeader(String[] header) {
        parallelDataset.forEach(parallelDataset -> parallelDataset.setHeader(header));
        return this;
    }

    private <K, V> void getFutures(ParallelPairDataset<K, V> mappedPairDataset, List<Future<PairDataset<K, V>>> futures) {
        for (Future<PairDataset<K, V>> future : futures) {
            try {
                mappedPairDataset.parallelPairDatasets.add(future.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    private <V> void getFutureDataset(ParallelDataset<V> mappedDataset, List<Future<Dataset<V>>> futures) {
        for (Future<Dataset<V>> future : futures) {
            try {
                mappedDataset.parallelDataset.add(future.get());
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (ExecutionException e) {
                e.printStackTrace();
            }
        }
    }

    public List<Dataset<T>> getParallelDataset() {
        return parallelDataset;
    }
}
