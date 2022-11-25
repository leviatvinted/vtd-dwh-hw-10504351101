package ir.alashti.vintedtest.dataset.udf;

public class Tuple3<K, V> {
    K key;
    V value1;
    V value2;

    public Tuple3(K key, V value1, V value2) {
        this.key = key;
        this.value1 = value1;
        this.value2 = value2;
    }

    public K getKey() {
        return key;
    }

    public Tuple3 setKey(K key) {
        this.key = key;
        return this;
    }

    public V getValue1() {
        return value1;
    }

    public Tuple3 setValue1(V value) {
        this.value1 = value;
        return this;
    }

    public V getValue2() {
        return value2;
    }

    public Tuple3<K, V> setValue2(V value2) {
        this.value2 = value2;
        return this;
    }

    @Override
    public String toString() {
        return key + "," + value1 + "," + value2;
    }
}
