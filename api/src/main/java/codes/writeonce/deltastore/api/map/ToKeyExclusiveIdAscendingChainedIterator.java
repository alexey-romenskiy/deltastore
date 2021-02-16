package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class ToKeyExclusiveIdAscendingChainedIterator<V, T> extends AbstractIdAscendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyExclusiveIdAscendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyExclusiveIdAscendingChainedIterator::new);

    private long toKey;

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyExclusiveIdAscendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive, long toKey) {
        final ToKeyExclusiveIdAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyExclusiveIdAscendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive, long fromKey, long toKey) {
        final ToKeyExclusiveIdAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyExclusiveIdAscendingChainedIterator() {
        // empty
    }

    protected void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, long toKey) {
        super.init(iterator, exclusive);
        this.toKey = toKey;
    }

    private void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, long fromKey, long toKey) {
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
    protected boolean ended(long key) {
        return toKey <= key;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
