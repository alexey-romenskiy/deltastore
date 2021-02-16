package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class ToKeyInclusiveBooleanAscendingChainedIterator<V, T> extends AbstractBooleanAscendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyInclusiveBooleanAscendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyInclusiveBooleanAscendingChainedIterator::new);

    private boolean toKey;

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyInclusiveBooleanAscendingChainedIterator<V, T> create(
            @Nonnull NestedIterator<V, T> iterator, boolean exclusive, boolean toKey) {
        final ToKeyInclusiveBooleanAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyInclusiveBooleanAscendingChainedIterator<V, T> create(
            @Nonnull NestedIterator<V, T> iterator, boolean exclusive, boolean fromKey, boolean toKey) {
        final ToKeyInclusiveBooleanAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyInclusiveBooleanAscendingChainedIterator() {
        // empty
    }

    protected void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, boolean toKey) {
        super.init(iterator, exclusive);
        this.toKey = toKey;
    }

    private void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, boolean fromKey, boolean toKey) {
        super.init(iterator, exclusive, fromKey);
        this.toKey = toKey;
    }

    @Override
    protected boolean ended() {
        return !toKey && map.keys[index];
    }

    @Override
    protected boolean nullEnded() {
        return false;
    }

    @Override
    protected boolean ended(boolean key) {
        return !toKey && key;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
