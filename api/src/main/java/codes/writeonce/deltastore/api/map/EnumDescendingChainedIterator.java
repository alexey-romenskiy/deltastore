package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class EnumDescendingChainedIterator<V, T> extends AbstractEnumDescendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<EnumDescendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, EnumDescendingChainedIterator::new);

    @SuppressWarnings("unchecked")
    public static <V, T> EnumDescendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator) {
        final EnumDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> EnumDescendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive, int fromKey) {
        final EnumDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey);
        return value;
    }

    private EnumDescendingChainedIterator() {
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
