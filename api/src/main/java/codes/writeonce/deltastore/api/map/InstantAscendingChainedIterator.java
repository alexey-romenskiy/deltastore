package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class InstantAscendingChainedIterator<V, T> extends AbstractInstantAscendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<InstantAscendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, InstantAscendingChainedIterator::new);

    @SuppressWarnings("unchecked")
    public static <V, T> InstantAscendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive) {
        final InstantAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> InstantAscendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive, long fromKey1, int fromKey2) {
        final InstantAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey1, fromKey2);
        return value;
    }

    private InstantAscendingChainedIterator() {
        // empty
    }

    @Override
    protected boolean ended() {
        return false;
    }

    @Override
    protected boolean nullEnded() {
        return false;
    }

    @Override
    protected boolean ended(long key1, int key2) {
        return false;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
