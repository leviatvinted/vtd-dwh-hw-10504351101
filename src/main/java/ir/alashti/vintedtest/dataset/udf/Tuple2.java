package ir.alashti.vintedtest.dataset.udf;

public class Tuple2<K, V> {
    K key;
    V value;

    public Tuple2(K key, V value) {
        this.key = key;
        this.value = value;
    }

    public K getKey() {
        return key;
    }

    public Tuple2 setKey(K key) {
        this.key = key;
        return this;
    }

    public V getValue() {
        return value;
    }

    public Tuple2 setValue(V value) {
        this.value = value;
        return this;
    }

    @Override
    public String toString() {
        return key + "," + value;
    }
}
