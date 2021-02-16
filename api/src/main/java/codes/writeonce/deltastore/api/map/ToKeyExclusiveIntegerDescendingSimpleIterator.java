package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class ToKeyExclusiveIntegerDescendingSimpleIterator<V> extends AbstractIntegerDescendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyExclusiveIntegerDescendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyExclusiveIntegerDescendingSimpleIterator::new);

    private int toKey;

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveIntegerDescendingSimpleIterator<V> create(int toKey) {
        final ToKeyExclusiveIntegerDescendingSimpleIterator<V> value = POOL.get();
        value.init(toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveIntegerDescendingSimpleIterator<V> create(boolean exclusive, int fromKey,
            int toKey) {
        final ToKeyExclusiveIntegerDescendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyExclusiveIntegerDescendingSimpleIterator() {
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
