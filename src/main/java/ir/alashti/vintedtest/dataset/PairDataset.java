package ir.alashti.vintedtest.dataset;

import ir.alashti.vintedtest.dataset.udf.Tuple2;
import ir.alashti.vintedtest.dataset.udf.Tuple3;
import ir.alashti.vintedtest.dataset.aop.PairOperation;
import ir.alashti.vintedtest.dataset.aop.TriFunction;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.function.Function;

public class PairDataset<K, V> implements PairOperation<K, V> {
    List<String> header = new ArrayList<>();
    Map<K, List<V>> rows = new HashMap<>();

    public List<String> getHeader() {
        return header;
    }

    public PairDataset setHeader(List<String> header) {
        this.header = header;
        return this;
    }

    public PairDataset setHeader(String[] header) {
        this.header = Arrays.asList(header);
        return this;
    }

    public Map<K, List<V>> getRows() {
        return rows;
    }

    public PairDataset setRows(Map<K, List<V>> rows) {
        this.rows = rows;
        return this;
    }

    @Override
    public PairDataset<K, V> select(String... columns) {
        PairDataset<K, V> output = new PairDataset<>();
        Set<String> columnsList = new HashSet<>(Arrays.asList(columns));
        Set<Integer> selectIndices = new HashSet<>();
        for (int i = 0; i < this.getHeader().size(); i++) {
            String next = this.getHeader().get(i);
            if (columnsList.contains(next)) {
                selectIndices.add(i);
                output.getHeader().add(next);
            }
        }
        for (Map.Entry<K, List<V>> kListEntry : this.getRows().entrySet()) {
            List<V> list = new ArrayList<>();
            for (int selectIndex : selectIndices) {
                for (V v : kListEntry.getValue()) {
                    list.add((V) ((Row) v).getValues().get(selectIndex));
                }
            }
            output.getRows().put(kListEntry.getKey(), list);
        }
        return output;
    }

    @Override
    public PairDataset<K, V> reduce(TriFunction<K, V> reducer) {
        PairDataset<K, V> pairDataset = new PairDataset<>();
        Map<K, List<V>> output = new HashMap<>();
        for (Map.Entry<K, List<V>> kListEntry : getRows().entrySet()) {
            V agg = kListEntry.getValue().get(0);
            for (int i = 1; i < kListEntry.getValue().size(); i++) {
                V v = kListEntry.getValue().get(i);
                agg = reducer.apply(kListEntry.getKey(), v, agg);
            }
            output.put(kListEntry.getKey(), Collections.singletonList(agg));
        }
        pairDataset.setRows(output);
        return pairDataset;
    }

    @Override
    public PairDataset<K, V> filter(Function<V, Boolean> filter) {
        PairDataset<K, V> clone = this.clone();
        for (Map.Entry<K, List<V>> kListEntry : clone.getRows().entrySet()) {
            Iterator<V> iterator = kListEntry.getValue().iterator();
            while (iterator.hasNext()) {
                V v = iterator.next();
                if (!filter.apply(v))
                    iterator.remove();
            }
            if (kListEntry.getValue().size() < 0)
                clone.getRows().remove(kListEntry.getKey());
        }
        return clone;
    }

    @Override
    public Dataset<Tuple3<K, V>> join(PairDataset<K, V> right) {
        Dataset<Tuple3<K, V>> tuple3Dataset = new Dataset<>();
        for (Map.Entry<K, List<V>> kListEntry : this.getRows().entrySet()) {
            if (right.getRows().containsKey(kListEntry.getKey())) {
                for (V leftValue : kListEntry.getValue()) {
                    for (V rightValue : right.getRows().get(kListEntry.getKey())) {
                        tuple3Dataset.getRows().add(new Tuple3<>(kListEntry.getKey(), leftValue, rightValue));
                    }
                }
            }
        }
        return tuple3Dataset;
    }

    public List<Tuple2<K, V>> collect() {
        List<Tuple2<K, V>> output = new ArrayList<>();
        for (Map.Entry<K, List<V>> kListEntry : getRows().entrySet()) {
            for (V v : kListEntry.getValue()) {
                output.add(new Tuple2<>(kListEntry.getKey(), v));
            }
        }
        return output;
    }

    public String toStringHeader(String splitter) {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < getHeader().size(); i++) {
            String column = getHeader().get(i);
            builder.append(column);
            if (i < getHeader().size() - 1)
                builder.append(splitter);
        }
        return builder.toString();
    }

    public PairDataset<K, V> clone() {
        PairDataset<K, V> kvPairDataset = new PairDataset<>();
        kvPairDataset.setHeader(this.getHeader());
        kvPairDataset.setRows(this.getRows());
        return kvPairDataset;
    }

    @Override
    public long count() {
        long count = 0;
        if (this.getRows() == null || this.getRows().size() == 0)
            return count;
        for (List<V> value : this.getRows().values()) {
            count += value.size();
        }
        return count;
    }
}
