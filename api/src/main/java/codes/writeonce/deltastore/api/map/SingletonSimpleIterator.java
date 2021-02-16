package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

final class SingletonSimpleIterator<K, V, M extends AbstractTreeMap<K, V, M>> extends NestedIterator<M, V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<SingletonSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, SingletonSimpleIterator::new);

    private K key;

    protected M map;

    private boolean index;

    @SuppressWarnings("unchecked")
    public static <K, V, M extends AbstractTreeMap<K, V, M>> SingletonSimpleIterator<K, V, M> create(@Nullable K key) {
        final SingletonSimpleIterator<K, V, M> value = POOL.get();
        value.init(key);
        return value;
    }

    private SingletonSimpleIterator() {
        // empty
    }

    private void init(@Nullable K key) {
        this.key = key;
    }

    @Nullable
    @Override
    public V get() {

        if (!index) {
            final var next = map.get(key);
            index = true;
            return next;
        }

        return null;
    }

    @Override
    public boolean hasNext() {

        if (!index) {
            final var next = map.containsKey(key);
            if (next) {
                return true;
            }
            index = true;
        }

        return false;
    }

    @Override
    public void reset(@Nonnull M map) {

        this.map = map;
        this.index = false;
    }

    @Override
    public void map(@Nonnull M map) {
        this.map = map;
    }

    @Override
    public void close() {
        super.close();
        this.key = null;
        this.map = null;
        POOL.put(this);
    }
}
