package codes.writeonce.deltastore.api;

import codes.writeonce.deltastore.api.map.AbstractTreeMap;

import java.util.function.Consumer;

public abstract class AbstractKey<E extends Entity<?>> extends AbstractFilter<E> {

    protected <K1, K2, V, M1 extends AbstractTreeMap<K1, M2, M1>, M2 extends AbstractTreeMap<K2, V, M2>> void remove(
            M1 map, K1 key, Consumer<M2> consumer) {
        final M2 map1 = map.get(key);
        if (map1 != null) {
            consumer.accept(map1);
            if (map1.isEmpty()) {
                map.remove(key);
            }
        }
    }

    protected <T> void with(T value, Consumer<T> consumer) {
        consumer.accept(value);
    }
}
