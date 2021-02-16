package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class IntegerAscendingChainedIterator<V, T> extends AbstractIntegerAscendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<IntegerAscendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, IntegerAscendingChainedIterator::new);

    @SuppressWarnings("unchecked")
    public static <V, T> IntegerAscendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive) {
        final IntegerAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> IntegerAscendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive, int fromKey) {
        final IntegerAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey);
        return value;
    }

    private IntegerAscendingChainedIterator() {
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
    protected boolean ended(int key) {
        return false;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
