package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class ToKeyExclusiveEnumAscendingChainedIterator<V, T> extends AbstractEnumAscendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyExclusiveEnumAscendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyExclusiveEnumAscendingChainedIterator::new);

    private int toKey;

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyExclusiveEnumAscendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive, int toKey) {
        final ToKeyExclusiveEnumAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyExclusiveEnumAscendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive, int fromKey, int toKey) {
        final ToKeyExclusiveEnumAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyExclusiveEnumAscendingChainedIterator() {
        // empty
    }

    protected void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, int toKey) {
        super.init(iterator, exclusive);
        this.toKey = toKey;
    }

    private void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, int fromKey, int toKey) {
        super.init(iterator, exclusive, fromKey);
        this.toKey = toKey;
    }

    @Override
    protected boolean ended() {
        return toKey <= map.keys[index];
    }

    @Override
    protected boolean nullEnded() {
        return false;
    }

    @Override
    protected boolean ended(int key) {
        return toKey <= key;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
