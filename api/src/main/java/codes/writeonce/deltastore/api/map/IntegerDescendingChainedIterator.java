package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class IntegerDescendingChainedIterator<V, T> extends AbstractIntegerDescendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<IntegerDescendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, IntegerDescendingChainedIterator::new);

    @SuppressWarnings("unchecked")
    public static <V, T> IntegerDescendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator) {
        final IntegerDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> IntegerDescendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive, int fromKey) {
        final IntegerDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey);
        return value;
    }

    private IntegerDescendingChainedIterator() {
        // empty
    }

    @Override
    protected boolean ended(int key) {
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
