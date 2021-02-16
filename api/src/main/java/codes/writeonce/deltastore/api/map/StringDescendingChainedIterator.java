package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class StringDescendingChainedIterator<V, T> extends AbstractStringDescendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<StringDescendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, StringDescendingChainedIterator::new);

    @SuppressWarnings("unchecked")
    public static <V, T> StringDescendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator) {
        final StringDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> StringDescendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive, @Nonnull String fromKey) {
        final StringDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey);
        return value;
    }

    private StringDescendingChainedIterator() {
        // empty
    }

    @Override
    protected boolean ended(@Nonnull String key) {
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
