package ir.alashti.vintedtest.dataset.parallel;

import com.google.common.collect.Maps;
import ir.alashti.vintedtest.dataset.Dataset;
import ir.alashti.vintedtest.dataset.PairDataset;

import java.util.concurrent.Callable;
import java.util.function.Function;

public class DatasetManager implements Callable {
    Function function;
    Maps.EntryTransformer<Dataset, Function, Dataset> operationEntry;
    Maps.EntryTransformer<Dataset, Function, PairDataset> datasetFunPairDatasetOp;
    Dataset dataset;

    public DatasetManager(Function function, Maps.EntryTransformer<Dataset, Function, Dataset> mapOp, Maps.EntryTransformer<Dataset, Function, PairDataset> mapToPairOp, Dataset dataset) {
        this.function = function;
        this.operationEntry = mapOp;
        this.datasetFunPairDatasetOp = mapToPairOp;
        this.dataset = dataset;
    }

    @Override
    public Object call() throws Exception {
        Dataset dataset = null;
        PairDataset pairDataset = null;
        if (this.operationEntry != null)
            dataset = this.operationEntry.transformEntry(this.dataset, this.function);
        else
            pairDataset = this.datasetFunPairDatasetOp.transformEntry(this.dataset, this.function);
        return dataset == null ? pairDataset : dataset;
    }
}
