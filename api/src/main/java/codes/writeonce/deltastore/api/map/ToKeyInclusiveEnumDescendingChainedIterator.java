package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class ToKeyInclusiveEnumDescendingChainedIterator<V, T> extends AbstractEnumDescendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyInclusiveEnumDescendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyInclusiveEnumDescendingChainedIterator::new);

    private int toKey;

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyInclusiveEnumDescendingChainedIterator<V, T> create(
            @Nonnull NestedIterator<V, T> iterator, int toKey) {
        final ToKeyInclusiveEnumDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyInclusiveEnumDescendingChainedIterator<V, T> create(
            @Nonnull NestedIterator<V, T> iterator, boolean exclusive, int fromKey, int toKey) {
        final ToKeyInclusiveEnumDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyInclusiveEnumDescendingChainedIterator() {
        // empty
    }

    private void init(@Nonnull NestedIterator<V, T> iterator, int toKey) {
        super.init(iterator);
        this.toKey = toKey;
    }

    private void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, int fromKey, int toKey) {
        super.init(iterator, exclusive, fromKey);
        this.toKey = toKey;
    }

    @Override
    protected boolean ended(int key) {
        return toKey > key;
    }

    @Override
    protected boolean ended() {
        return toKey > map.keys[index];
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
