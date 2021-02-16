package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class ToKeyInclusiveInstantDescendingChainedIterator<V, T>
        extends AbstractInstantDescendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyInclusiveInstantDescendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyInclusiveInstantDescendingChainedIterator::new);

    private long toKey1;

    private int toKey2;

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyInclusiveInstantDescendingChainedIterator<V, T> create(
            @Nonnull NestedIterator<V, T> iterator, long toKey1, int toKey2) {
        final ToKeyInclusiveInstantDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, toKey1, toKey2);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyInclusiveInstantDescendingChainedIterator<V, T> create(
            @Nonnull NestedIterator<V, T> iterator, boolean exclusive, long fromKey1, int fromKey2, long toKey1,
            int toKey2) {
        final ToKeyInclusiveInstantDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey1, fromKey2, toKey1, toKey2);
        return value;
    }

    private ToKeyInclusiveInstantDescendingChainedIterator() {
        // empty
    }

    private void init(@Nonnull NestedIterator<V, T> iterator, long toKey1, int toKey2) {
        super.init(iterator);
        this.toKey1 = toKey1;
        this.toKey2 = toKey2;
    }

    private void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, long fromKey1, int fromKey2,
            long toKey1, int toKey2) {
        super.init(iterator, exclusive, fromKey1, fromKey2);
        this.toKey1 = toKey1;
        this.toKey2 = toKey2;
    }

    @Override
    protected boolean ended(long key1, int key2) {
        return toKey1 > key1 || toKey1 == key1 && toKey2 > key2;
    }

    @Override
    protected boolean ended() {
        return toKey1 > map.keys1[index] || toKey1 == map.keys1[index] && toKey2 > map.keys2[index];
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
