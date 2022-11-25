package ir.alashti.vintedtest.dataset.aop;

import ir.alashti.vintedtest.dataset.Dataset;
import ir.alashti.vintedtest.dataset.PairDataset;
import ir.alashti.vintedtest.dataset.udf.Tuple2;

import java.util.function.Function;

public interface SingleOperation extends Operation {
    <I, K, V> PairDataset<K, V> mapToPair(Function<I, Tuple2<K, V>> mapper);

    <I, O> Dataset<O> map(Function<I, O> mapper);

    <T> Dataset<T> select(String... columns);

    <T> Dataset<T> filter(Function<T, Boolean> filter);
}
