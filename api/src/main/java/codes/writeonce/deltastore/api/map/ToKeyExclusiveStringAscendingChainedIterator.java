package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class ToKeyExclusiveStringAscendingChainedIterator<V, T> extends AbstractStringAscendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyExclusiveStringAscendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyExclusiveStringAscendingChainedIterator::new);

    private String toKey;

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyExclusiveStringAscendingChainedIterator<V, T> create(
            @Nonnull NestedIterator<V, T> iterator, boolean exclusive, @Nonnull String toKey) {
        final ToKeyExclusiveStringAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyExclusiveStringAscendingChainedIterator<V, T> create(
            @Nonnull NestedIterator<V, T> iterator, boolean exclusive, @Nonnull String fromKey, @Nonnull String toKey) {
        final ToKeyExclusiveStringAscendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyExclusiveStringAscendingChainedIterator() {
        // empty
    }

    protected void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, @Nonnull String toKey) {
        super.init(iterator, exclusive);
        this.toKey = toKey;
    }

    private void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, @Nonnull String fromKey,
            @Nonnull String toKey) {
        super.init(iterator, exclusive, fromKey);
        this.toKey = toKey;
    }

    @Override
    protected boolean ended() {
        return toKey.compareTo((String) map.keys[index]) <= 0;
    }

    @Override
    protected boolean nullEnded() {
        return false;
    }

    @Override
    protected boolean ended(@Nonnull String key) {
        return toKey.compareTo(key) <= 0;
    }

    @Override
    public void close() {
        super.close();
        this.toKey = null;
        POOL.put(this);
    }
}
