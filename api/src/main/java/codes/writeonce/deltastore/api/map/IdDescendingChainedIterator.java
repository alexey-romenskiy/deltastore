package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class IdDescendingChainedIterator<V, T> extends AbstractIdDescendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<IdDescendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, IdDescendingChainedIterator::new);

    @SuppressWarnings("unchecked")
    public static <V, T> IdDescendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator) {
        final IdDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> IdDescendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive, long fromKey) {
        final IdDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey);
        return value;
    }

    private IdDescendingChainedIterator() {
        // empty
    }

    @Override
    protected boolean ended(long key) {
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
