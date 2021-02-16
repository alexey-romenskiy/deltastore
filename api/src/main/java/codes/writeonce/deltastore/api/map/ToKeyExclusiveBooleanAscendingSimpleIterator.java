package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class ToKeyExclusiveBooleanAscendingSimpleIterator<V> extends AbstractBooleanAscendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyExclusiveBooleanAscendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyExclusiveBooleanAscendingSimpleIterator::new);

    private boolean toKey;

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveBooleanAscendingSimpleIterator<V> create(boolean exclusive, boolean toKey) {
        final ToKeyExclusiveBooleanAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> ToKeyExclusiveBooleanAscendingSimpleIterator<V> create(boolean exclusive, boolean fromKey,
            boolean toKey) {
        final ToKeyExclusiveBooleanAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyExclusiveBooleanAscendingSimpleIterator() {
        // empty
    }

    protected void init(boolean exclusive, boolean toKey) {
        super.init(exclusive);
        this.toKey = toKey;
    }

    private void init(boolean exclusive, boolean fromKey, boolean toKey) {
        super.init(exclusive, fromKey);
        this.toKey = toKey;
    }

    @Override
    protected boolean ended() {
        return !toKey || map.keys[index];
    }

    @Override
    protected boolean nullEnded() {
        return false;
    }

    @Override
    protected boolean ended(boolean key) {
        return !toKey || key;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
