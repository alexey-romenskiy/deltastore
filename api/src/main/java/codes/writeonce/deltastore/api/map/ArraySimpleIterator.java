package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

final class ArraySimpleIterator<K, V, M extends AbstractTreeMap<K, V, M>> extends NestedIterator<M, V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ArraySimpleIterator> POOL = new ArrayPool<>(POOL_SIZE, ArraySimpleIterator::new);

    private K[] keys;

    protected M map;

    private int index;

    @SuppressWarnings("unchecked")
    public static <K, V, M extends AbstractTreeMap<K, V, M>> ArraySimpleIterator<K, V, M> create(@Nonnull K[] keys) {
        final ArraySimpleIterator<K, V, M> value = POOL.get();
        value.init(keys);
        return value;
    }

    private ArraySimpleIterator() {
        // empty
    }

    private void init(@Nonnull K[] keys) {
        this.keys = keys;
    }

    @Nullable
    @Override
    public V get() {

        while (index < keys.length) {
            final var next = map.get(keys[index++]);
            if (next != null) {
                return next;
            }
        }

        return null;
    }

    @Override
    public boolean hasNext() {

        while (index < keys.length) {
            final var next = map.containsKey(keys[index]);
            if (next) {
                return true;
            }
            index++;
        }

        return false;
    }

    @Override
    public void reset(@Nonnull M map) {

        this.map = map;
        this.index = 0;
    }

    @Override
    public void map(@Nonnull M map) {
        this.map = map;
    }

    @Override
    public void close() {
        super.close();
        this.keys = null;
        this.map = null;
        POOL.put(this);
    }
}
