package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

final class ToKeyInclusiveBooleanDescendingSimpleIterator<V> extends AbstractBooleanDescendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ToKeyInclusiveBooleanDescendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, ToKeyInclusiveBooleanDescendingSimpleIterator::new);

    private boolean toKey;

    @SuppressWarnings("unchecked")
    public static <V> ToKeyInclusiveBooleanDescendingSimpleIterator<V> create(boolean toKey) {
        final ToKeyInclusiveBooleanDescendingSimpleIterator<V> value = POOL.get();
        value.init(toKey);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> ToKeyInclusiveBooleanDescendingSimpleIterator<V> create(boolean exclusive, boolean fromKey,
            boolean toKey) {
        final ToKeyInclusiveBooleanDescendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey, toKey);
        return value;
    }

    private ToKeyInclusiveBooleanDescendingSimpleIterator() {
        // empty
    }

    private void init(boolean toKey) {
        super.init();
        this.toKey = toKey;
    }

    private void init(boolean exclusive, boolean fromKey, boolean toKey) {
        super.init(exclusive, fromKey);
        this.toKey = toKey;
    }

    @Override
    protected boolean ended() {
        return toKey && !map.keys[index];
    }

    @Override
    protected boolean nullEnded() {
        return true;
    }

    @Override
    protected boolean ended(boolean key) {
        return toKey && !key;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
