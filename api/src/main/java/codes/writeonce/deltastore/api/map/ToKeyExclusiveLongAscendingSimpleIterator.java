package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class ToKeyExclusiveLongAscendingSimpleIterator<V> extends AbstractLongAscendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyExclusiveLongAscendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyExclusiveLongAscendingSimpleIterator::new);

    private long toKey;

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveLongAscendingSimpleIterator<V> create(boolean exclusive, long toKey) {
        final ToKeyExclusiveLongAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveLongAscendingSimpleIterator<V> create(boolean exclusive, long fromKey, long toKey) {
        final ToKeyExclusiveLongAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyExclusiveLongAscendingSimpleIterator() {
        // empty
    }

    protected void init(boolean exclusive, long toKey) {
        super.init(exclusive);
        this.toKey = toKey;
    }

    private void init(boolean exclusive, long fromKey, long toKey) {
        super.init(exclusive, fromKey);
        this.toKey = toKey;
    }

    @Override
    protected boolean ended() {
        return toKey <= map.keys[index];
    }

    @Override
    protected boolean nullEnded() {
        return false;
    }

    @Override
    protected boolean ended(long key) {
        return toKey <= key;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
