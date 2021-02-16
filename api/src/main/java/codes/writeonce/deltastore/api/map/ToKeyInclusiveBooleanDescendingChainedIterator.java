package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class ToKeyInclusiveBooleanDescendingChainedIterator<V, T>
        extends AbstractBooleanDescendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyInclusiveBooleanDescendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyInclusiveBooleanDescendingChainedIterator::new);

    private boolean toKey;

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyInclusiveBooleanDescendingChainedIterator<V, T> create(
            @Nonnull NestedIterator<V, T> iterator, boolean toKey) {
        final ToKeyInclusiveBooleanDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyInclusiveBooleanDescendingChainedIterator<V, T> create(
            @Nonnull NestedIterator<V, T> iterator, boolean exclusive, boolean fromKey, boolean toKey) {
        final ToKeyInclusiveBooleanDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyInclusiveBooleanDescendingChainedIterator() {
        // empty
    }

    private void init(@Nonnull NestedIterator<V, T> iterator, boolean toKey) {
        super.init(iterator);
        this.toKey = toKey;
    }

    private void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, boolean fromKey, boolean toKey) {
        super.init(iterator, exclusive, fromKey);
        this.toKey = toKey;
    }

    @Override
    protected boolean ended(boolean key) {
        return toKey && !key;
    }

    @Override
    protected boolean ended() {
        return toKey && !map.keys[index];
    }

    @Override
    protected boolean nullEnded() {
        return true;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
