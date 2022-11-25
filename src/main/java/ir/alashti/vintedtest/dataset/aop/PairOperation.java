package ir.alashti.vintedtest.dataset.aop;

import ir.alashti.vintedtest.dataset.Dataset;
import ir.alashti.vintedtest.dataset.PairDataset;
import ir.alashti.vintedtest.dataset.udf.Tuple3;

import java.util.function.Function;

public interface PairOperation<K, V> extends Operation {
    PairDataset<K, V> reduce(TriFunction<K, V> reducer);

    PairDataset<K, V> filter(Function<V, Boolean> mapper);

    Dataset<Tuple3<K, V>> join(PairDataset<K, V> right);

    PairDataset<K, V> select(String... columns);
}
