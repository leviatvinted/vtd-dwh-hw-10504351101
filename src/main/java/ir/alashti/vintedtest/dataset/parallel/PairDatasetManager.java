package ir.alashti.vintedtest.dataset.parallel;

import com.google.common.collect.Maps;
import ir.alashti.vintedtest.dataset.Dataset;
import ir.alashti.vintedtest.dataset.PairDataset;
import ir.alashti.vintedtest.dataset.aop.TriFunction;
import ir.alashti.vintedtest.dataset.udf.Tuple3;

import java.util.concurrent.Callable;
import java.util.function.Function;

public class PairDatasetManager implements Callable {
    TriFunction trifunction;
    Function function;
    Maps.EntryTransformer<PairDataset, TriFunction, PairDataset> transformer;
    Maps.EntryTransformer<PairDataset, Function, PairDataset> datasetFunPairDatasetOp;
    Maps.EntryTransformer<PairDataset, PairDataset, Dataset<Tuple3>> joinTransformer;
    PairDataset dataset, joinDataset;

    public PairDatasetManager(TriFunction trifunction, Function function,
                              Maps.EntryTransformer<PairDataset, TriFunction, PairDataset> transformer,
                              Maps.EntryTransformer<PairDataset, Function, PairDataset> datasetFunPairDatasetOp,
                              PairDataset dataset,
                              PairDataset joinDataset) {
        this.trifunction = trifunction;
        this.function = function;
        this.transformer = transformer;
        this.datasetFunPairDatasetOp = datasetFunPairDatasetOp;
        this.dataset = dataset;
        this.joinDataset = joinDataset;
    }

    public PairDatasetManager setJoinTransformer(Maps.EntryTransformer<PairDataset, PairDataset, Dataset<Tuple3>> joinTransformer) {
        this.joinTransformer = joinTransformer;
        return this;
    }

    @Override
    public Object call() throws Exception {
        PairDataset pairDataset = null;
        Dataset dataset = null;
        if (transformer != null)
            pairDataset = this.transformer.transformEntry(this.dataset, this.trifunction);
        else if(datasetFunPairDatasetOp != null)
            pairDataset = this.datasetFunPairDatasetOp.transformEntry(this.dataset, this.function);
        else
            dataset = this.joinTransformer.transformEntry(this.dataset, this.joinDataset);
        return pairDataset != null ? pairDataset : dataset;
    }
}
