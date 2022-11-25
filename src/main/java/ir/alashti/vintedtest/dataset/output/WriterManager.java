package ir.alashti.vintedtest.dataset.output;

import com.google.common.collect.Lists;
import ir.alashti.vintedtest.cli.Arguments;
import ir.alashti.vintedtest.pool.Executor;

import java.util.List;
import java.util.concurrent.ThreadPoolExecutor;

public class WriterManager {
    public static <T> void writerManager(List<T> rows, String outputPath) {
        List<List<T>> partition = Lists.partition(rows, Arguments.cores);
        ThreadPoolExecutor executorPool = Executor.getExecutorPool();
        for (List<T> ts : partition) {
            Writer writer = new Writer(ts, outputPath);
            executorPool.execute(writer);
        }
    }
}
