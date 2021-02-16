package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class LongDescendingChainedIterator<V, T> extends AbstractLongDescendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<LongDescendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, LongDescendingChainedIterator::new);

    @SuppressWarnings("unchecked")
    public static <V, T> LongDescendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator) {
        final LongDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> LongDescendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive, long fromKey) {
        final LongDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey);
        return value;
    }

    private LongDescendingChainedIterator() {
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
