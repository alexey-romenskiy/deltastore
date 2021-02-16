package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class ToKeyInclusiveStringDescendingSimpleIterator<V> extends AbstractStringDescendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyInclusiveStringDescendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyInclusiveStringDescendingSimpleIterator::new);

    private String toKey;

    @SuppressWarnings("unchecked")
    public static <V> ToKeyInclusiveStringDescendingSimpleIterator<V> create(@Nonnull String toKey) {
        final ToKeyInclusiveStringDescendingSimpleIterator<V> value = POOL.get();
        value.init(toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> ToKeyInclusiveStringDescendingSimpleIterator<V> create(boolean exclusive, @Nonnull String fromKey,
            @Nonnull String toKey) {
        final ToKeyInclusiveStringDescendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyInclusiveStringDescendingSimpleIterator() {
        // empty
    }

    private void init(@Nonnull String toKey) {
        super.init();
        this.toKey = toKey;
    }

    private void init(boolean exclusive, @Nonnull String fromKey, @Nonnull String toKey) {
        super.init(exclusive, fromKey);
        this.toKey = toKey;
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
    protected boolean ended(@Nonnull String key) {
        return toKey.compareTo(key) > 0;
    }

    @Override
    public void close() {
        super.close();
        this.toKey = null;
        POOL.put(this);
    }
}
