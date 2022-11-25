package ir.alashti.vintedtest.dataset.aop;

@FunctionalInterface
public interface TriFunction<K, V> {
    V apply(K key, V value1, V value2);
}
