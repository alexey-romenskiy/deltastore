package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class ToKeyExclusiveInstantAscendingSimpleIterator<V> extends AbstractInstantAscendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyExclusiveInstantAscendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyExclusiveInstantAscendingSimpleIterator::new);

    private long toKey1;

    private int toKey2;

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveInstantAscendingSimpleIterator<V> create(boolean exclusive, long toKey1,
            int toKey2) {
        final ToKeyExclusiveInstantAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, toKey1, toKey2);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveInstantAscendingSimpleIterator<V> create(boolean exclusive, long fromKey1,
            int fromKey2, long toKey1, int toKey2) {
        final ToKeyExclusiveInstantAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey1, fromKey2, toKey1, toKey2);
        return value;
    }

    private ToKeyExclusiveInstantAscendingSimpleIterator() {
        // empty
    }

    protected void init(boolean exclusive, long toKey1, int toKey2) {
        super.init(exclusive);
        this.toKey1 = toKey1;
        this.toKey2 = toKey2;
    }

    private void init(boolean exclusive, long fromKey1, int fromKey2, long toKey1, int toKey2) {
        super.init(exclusive, fromKey1, fromKey2);
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
