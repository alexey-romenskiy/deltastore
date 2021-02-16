package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class ToKeyExclusiveIntegerAscendingSimpleIterator<V> extends AbstractIntegerAscendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyExclusiveIntegerAscendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyExclusiveIntegerAscendingSimpleIterator::new);

    private int toKey;

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveIntegerAscendingSimpleIterator<V> create(boolean exclusive, int toKey) {
        final ToKeyExclusiveIntegerAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveIntegerAscendingSimpleIterator<V> create(boolean exclusive, int fromKey,
            int toKey) {
        final ToKeyExclusiveIntegerAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyExclusiveIntegerAscendingSimpleIterator() {
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
