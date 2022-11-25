package ir.alashti.vintedtest.dataset.input;

import ir.alashti.vintedtest.dataset.Dataset;
import ir.alashti.vintedtest.dataset.parallel.ParallelDataset;
import ir.alashti.vintedtest.dataset.Row;
import ir.alashti.vintedtest.pool.Executor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Collectors;

public class LoaderManager {
    public static ParallelDataset<Row> textLoaderManager(String datasetPath, final List<String> header, boolean hasHeader, String splitter) {
        ParallelDataset<Row> parallelDataset = new ParallelDataset<>();
        try {
            Set<Path> filesPaths = Files.list(Paths.get(datasetPath)).collect(Collectors.toSet());
            ThreadPoolExecutor executorPool = Executor.getExecutorPool();
            List<Future<Dataset<Row>>> futures = new ArrayList<>();
            for (Path path : filesPaths) {
                Loader<Dataset> loader = new Loader(path, header, hasHeader, splitter);
                futures.add(executorPool.submit(loader));
            }
            for (int i = 0; i < futures.size(); i++) {
                parallelDataset.add(futures.get(i).get());
            }
        } catch (IOException ioException) {
            ioException.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
        return parallelDataset;
    }
}
