package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class ToKeyExclusiveLongDescendingSimpleIterator<V> extends AbstractLongDescendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyExclusiveLongDescendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyExclusiveLongDescendingSimpleIterator::new);

    private long toKey;

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveLongDescendingSimpleIterator<V> create(long toKey) {
        final ToKeyExclusiveLongDescendingSimpleIterator<V> value = POOL.get();
        value.init(toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveLongDescendingSimpleIterator<V> create(boolean exclusive, long fromKey,
            long toKey) {
        final ToKeyExclusiveLongDescendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyExclusiveLongDescendingSimpleIterator() {
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
