package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class ToKeyInclusiveBooleanAscendingSimpleIterator<V> extends AbstractBooleanAscendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyInclusiveBooleanAscendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyInclusiveBooleanAscendingSimpleIterator::new);

    private boolean toKey;

    @SuppressWarnings("unchecked")
    public static <V> ToKeyInclusiveBooleanAscendingSimpleIterator<V> create(boolean exclusive, boolean toKey) {
        final ToKeyInclusiveBooleanAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> ToKeyInclusiveBooleanAscendingSimpleIterator<V> create(boolean exclusive, boolean fromKey,
            boolean toKey) {
        final ToKeyInclusiveBooleanAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyInclusiveBooleanAscendingSimpleIterator() {
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
        return !toKey && map.keys[index];
    }

    @Override
    protected boolean nullEnded() {
        return false;
    }

    @Override
    protected boolean ended(boolean key) {
        return !toKey && key;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
