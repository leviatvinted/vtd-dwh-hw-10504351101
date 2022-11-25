package ir.alashti.vintedtest.dataset;

import ir.alashti.vintedtest.dataset.aop.SingleOperation;
import ir.alashti.vintedtest.dataset.udf.Tuple2;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

public class Dataset<T> implements SingleOperation {
    List<String> header = new ArrayList<>();
    List<T> rows = new ArrayList<>();

    public List<String> getHeader() {
        return header;
    }

    public Dataset<T> setHeader(List<String> header) {
        this.header = header;
        return this;
    }

    public Dataset<T> setHeader(String[] header) {
        this.header = Arrays.asList(header);
        return this;
    }

    public List<T> getRows() {
        return rows;
    }

    public Dataset<T> setRows(List<T> rows) {
        this.rows = rows;
        return this;
    }

    @Override
    public <I, O> Dataset<O> map(Function<I, O> mapper) {
        Dataset<O> mappedDataset = new Dataset<>();
        List<O> output = new ArrayList<>(rows.size());
        for (T row : rows) {
            output.add(mapper.apply((I) row));
        }
        mappedDataset.setRows(output);
        return mappedDataset;
    }

    @Override
    public <I, K, V> PairDataset<K, V> mapToPair(Function<I, Tuple2<K, V>> mapper) {
        PairDataset<K, V> pairDataset = new PairDataset<>();
        Map<K, List<V>> output = new HashMap<>();
        for (T row : rows) {
            Tuple2<K, V> apply = mapper.apply((I) row);
            if (output.containsKey(apply.getKey())) {
                output.get(apply.getKey()).add(apply.getValue());
            } else {
                List<V> list = new ArrayList<>();
                list.add(apply.getValue());
                output.put(apply.getKey(), list);
            }
        }
        pairDataset.setRows(output);
        return pairDataset;
    }

    @Override
    public <T1> Dataset<T1> select(String... columns) {
        Dataset<T1> output = new Dataset<>();
        Set<String> columnsList = new HashSet<>(Arrays.asList(columns));
        Set<Integer> selectIndices = new HashSet<>();
        for (int i = 0; i < this.getHeader().size(); i++) {
            String next = this.getHeader().get(i);
            if (columnsList.contains(next)) {
                selectIndices.add(i);
                output.getHeader().add(next);
            }
        }
        List<Row> list = new ArrayList<>();
        for (T row : this.getRows()) {
            Row selRow = new Row();
            for (int selectIndex : selectIndices) {
                selRow.add(((Row) row).getValues().get(selectIndex));
            }
            list.add(selRow);
        }
        output.setRows((List<T1>) list);
        return output;
    }

    @Override
    public <V> Dataset<V> filter(Function<V, Boolean> filter) {
        Dataset<T> dataset = this.clone();
        List<T> collect = dataset.getRows().stream().filter(d -> filter.apply((V) d)).collect(Collectors.toList());
        dataset.setRows(collect);
        return (Dataset<V>) dataset;
    }

    @Override
    public long count() {
        return rows.size();
    }

    public Dataset<T> clone() {
        Dataset<T> dataset = new Dataset<>();
        dataset.setRows(this.getRows());
        dataset.setHeader(this.getHeader());
        return dataset;
    }

    public void merge(Dataset<T> rowDataset) {
        this.getRows().addAll(rowDataset.getRows());
    }
}
