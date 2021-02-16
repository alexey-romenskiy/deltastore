package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class ToKeyExclusiveIdDescendingSimpleIterator<V> extends AbstractIdDescendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyExclusiveIdDescendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyExclusiveIdDescendingSimpleIterator::new);

    private long toKey;

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveIdDescendingSimpleIterator<V> create(long toKey) {
        final ToKeyExclusiveIdDescendingSimpleIterator<V> value = POOL.get();
        value.init(toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveIdDescendingSimpleIterator<V> create(boolean exclusive, long fromKey, long toKey) {
        final ToKeyExclusiveIdDescendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyExclusiveIdDescendingSimpleIterator() {
        // empty
    }

    private void init(long toKey) {
        super.init();
        this.toKey = toKey;
    }

    private void init(boolean exclusive, long fromKey, long toKey) {
        super.init(exclusive, fromKey);
        this.toKey = toKey;
    }

    @Override
    protected boolean ended() {
        return toKey >= map.keys[index];
    }

    @Override
    protected boolean nullEnded() {
        return true;
    }

    @Override
    protected boolean ended(long key) {
        return toKey >= key;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
