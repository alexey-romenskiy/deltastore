package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class ToKeyExclusiveInstantAscendingChainedIterator<V, T> extends AbstractInstantAscendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyExclusiveInstantAscendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyExclusiveInstantAscendingChainedIterator::new);

    private long toKey1;

    private int toKey2;

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyExclusiveInstantAscendingChainedIterator<V, T> create(
            @Nonnull NestedIterator<V, T> iterator, boolean exclusive, long toKey1, int toKey2) {
        final ToKeyExclusiveInstantAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, toKey1, toKey2);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyExclusiveInstantAscendingChainedIterator<V, T> create(
            @Nonnull NestedIterator<V, T> iterator, boolean exclusive, long fromKey1, int fromKey2, long toKey1,
            int toKey2) {
        final ToKeyExclusiveInstantAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey1, fromKey2, toKey1, toKey2);
        return value;
    }

    private ToKeyExclusiveInstantAscendingChainedIterator() {
        // empty
    }

    protected void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, long toKey1, int toKey2) {
        super.init(iterator, exclusive);
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
    protected boolean ended() {
        return toKey1 < map.keys1[index] || toKey1 == map.keys1[index] && toKey2 <= map.keys2[index];
    }

    @Override
    protected boolean nullEnded() {
        return false;
    }

    @Override
    protected boolean ended(long key1, int key2) {
        return toKey1 < key1 || toKey1 == key1 && toKey2 <= key2;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
