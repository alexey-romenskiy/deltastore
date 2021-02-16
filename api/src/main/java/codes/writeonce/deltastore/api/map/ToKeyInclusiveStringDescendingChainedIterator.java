package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class ToKeyInclusiveStringDescendingChainedIterator<V, T> extends AbstractStringDescendingChainedIterator<V, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyInclusiveStringDescendingChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyInclusiveStringDescendingChainedIterator::new);

    private String toKey;

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyInclusiveStringDescendingChainedIterator<V, T> create(
            @Nonnull NestedIterator<V, T> iterator, @Nonnull String toKey) {
        final ToKeyInclusiveStringDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V, T> ToKeyInclusiveStringDescendingChainedIterator<V, T> create(
            @Nonnull NestedIterator<V, T> iterator, boolean exclusive, @Nonnull String fromKey, @Nonnull String toKey) {
        final ToKeyInclusiveStringDescendingChainedIterator<V, T> value = POOL.get();
        value.init(iterator, exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyInclusiveStringDescendingChainedIterator() {
        // empty
    }

    private void init(@Nonnull NestedIterator<V, T> iterator, @Nonnull String toKey) {
        super.init(iterator);
        this.toKey = toKey;
    }

    private void init(@Nonnull NestedIterator<V, T> iterator, boolean exclusive, @Nonnull String fromKey,
            @Nonnull String toKey) {
        super.init(iterator, exclusive, fromKey);
        this.toKey = toKey;
    }

    @Override
    protected boolean ended(@Nonnull String key) {
        return toKey.compareTo(key) > 0;
    }

    @Override
    protected boolean ended() {
        return toKey.compareTo((String) map.keys[index]) > 0;
    }

    @Override
    protected boolean nullEnded() {
        return true;
    }

    @Override
    public void close() {
        super.close();
        this.toKey = null;
        POOL.put(this);
    }
}
