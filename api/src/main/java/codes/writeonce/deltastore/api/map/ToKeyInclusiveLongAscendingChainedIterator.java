package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class ToKeyInclusiveLongAscendingChainedIterator<V, T> extends AbstractLongAscendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyInclusiveLongAscendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyInclusiveLongAscendingChainedIterator::new);

    private long toKey;

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyInclusiveLongAscendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive, long toKey) {
        final ToKeyInclusiveLongAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyInclusiveLongAscendingChainedIterator<V, T> create(@Nonnull NestedIterator<V, T> iterator,
            boolean exclusive, long fromKey, long toKey) {
        final ToKeyInclusiveLongAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyInclusiveLongAscendingChainedIterator() {
        // empty
    }

    protected void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, long toKey) {
        super.init(iterator, exclusive);
        this.toKey = toKey;
    }

    private void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, long fromKey, long toKey) {
        super.init(iterator, exclusive, fromKey);
        this.toKey = toKey;
    }

    @Override
    protected boolean ended() {
        return toKey < map.keys[index];
    }

    @Override
    protected boolean nullEnded() {
        return false;
    }

    @Override
    protected boolean ended(long key) {
        return toKey < key;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
