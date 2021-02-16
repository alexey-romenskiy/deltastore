package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class ToKeyExclusiveEnumAscendingSimpleIterator<V> extends AbstractEnumAscendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyExclusiveEnumAscendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyExclusiveEnumAscendingSimpleIterator::new);

    private int toKey;

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveEnumAscendingSimpleIterator<V> create(boolean exclusive, int toKey) {
        final ToKeyExclusiveEnumAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveEnumAscendingSimpleIterator<V> create(boolean exclusive, int fromKey, int toKey) {
        final ToKeyExclusiveEnumAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyExclusiveEnumAscendingSimpleIterator() {
        // empty
    }

    protected void init(boolean exclusive, int toKey) {
        super.init(exclusive);
        this.toKey = toKey;
    }

    private void init(boolean exclusive, int fromKey, int toKey) {
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
    protected boolean ended(int key) {
        return toKey <= key;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
