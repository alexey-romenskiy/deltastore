package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class EnumAscendingChainedIterator<V, T> extends AbstractEnumAscendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<EnumAscendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, EnumAscendingChainedIterator::new);

    @SuppressWarnings("unchecked")
    public static <V, T> EnumAscendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive) {
        final EnumAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> EnumAscendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive, int fromKey) {
        final EnumAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey);
        return value;
    }

    private EnumAscendingChainedIterator() {
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
