package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class BooleanDescendingChainedIterator<V, T> extends AbstractBooleanDescendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<BooleanDescendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, BooleanDescendingChainedIterator::new);

    @SuppressWarnings("unchecked")
    public static <V, T> BooleanDescendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator) {
        final BooleanDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> BooleanDescendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive, boolean fromKey) {
        final BooleanDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey);
        return value;
    }

    private BooleanDescendingChainedIterator() {
        // empty
    }

    @Override
    protected boolean ended(boolean key) {
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
