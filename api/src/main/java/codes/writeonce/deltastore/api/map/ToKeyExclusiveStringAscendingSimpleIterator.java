package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class ToKeyExclusiveStringAscendingSimpleIterator<V> extends AbstractStringAscendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyExclusiveStringAscendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyExclusiveStringAscendingSimpleIterator::new);

    private String toKey;

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveStringAscendingSimpleIterator<V> create(boolean exclusive, @Nonnull String toKey) {
        final ToKeyExclusiveStringAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveStringAscendingSimpleIterator<V> create(boolean exclusive, @Nonnull String fromKey,
            @Nonnull String toKey) {
        final ToKeyExclusiveStringAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyExclusiveStringAscendingSimpleIterator() {
        // empty
    }

    protected void init(boolean exclusive, @Nonnull String toKey) {
        super.init(exclusive);
        this.toKey = toKey;
    }

    private void init(boolean exclusive, @Nonnull String fromKey, @Nonnull String toKey) {
        super.init(exclusive, fromKey);
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
