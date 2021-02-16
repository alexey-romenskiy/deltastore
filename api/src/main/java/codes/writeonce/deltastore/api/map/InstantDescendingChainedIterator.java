package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class InstantDescendingChainedIterator<V, T> extends AbstractInstantDescendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<InstantDescendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, InstantDescendingChainedIterator::new);

    @SuppressWarnings("unchecked")
    public static <V, T> InstantDescendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator) {
        final InstantDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> InstantDescendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive, long fromKey1, int fromKey2) {
        final InstantDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey1, fromKey2);
        return value;
    }

    private InstantDescendingChainedIterator() {
        // empty
    }

    @Override
    protected boolean ended(long key1, int key2) {
        return false;
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
    public void close() {
        super.close();
        POOL.put(this);
    }
}
