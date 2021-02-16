package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class ToKeyExclusiveEnumDescendingSimpleIterator<V> extends AbstractEnumDescendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyExclusiveEnumDescendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyExclusiveEnumDescendingSimpleIterator::new);

    private int toKey;

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveEnumDescendingSimpleIterator<V> create(int toKey) {
        final ToKeyExclusiveEnumDescendingSimpleIterator<V> value = POOL.get();
        value.init(toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveEnumDescendingSimpleIterator<V> create(boolean exclusive, int fromKey, int toKey) {
        final ToKeyExclusiveEnumDescendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyExclusiveEnumDescendingSimpleIterator() {
        // empty
    }

    private void init(int toKey) {
        super.init();
        this.toKey = toKey;
    }

    private void init(boolean exclusive, int fromKey, int toKey) {
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
    protected boolean ended(int key) {
        return toKey >= key;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
