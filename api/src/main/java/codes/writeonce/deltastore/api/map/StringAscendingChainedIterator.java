package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class StringAscendingChainedIterator<V, T> extends AbstractStringAscendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<StringAscendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, StringAscendingChainedIterator::new);

    @SuppressWarnings("unchecked")
    public static <V, T> StringAscendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive) {
        final StringAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> StringAscendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive, @Nonnull String fromKey) {
        final StringAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey);
        return value;
    }

    private StringAscendingChainedIterator() {
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
    protected boolean ended(@Nonnull String key) {
        return false;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
