package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class StringDescendingSimpleIterator<V> extends AbstractStringDescendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<StringDescendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, StringDescendingSimpleIterator::new);

    @SuppressWarnings("unchecked")
    public static <V> StringDescendingSimpleIterator<V> create() {
        final StringDescendingSimpleIterator<V> value = POOL.get();
        value.init();
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> StringDescendingSimpleIterator<V> create(boolean exclusive, @Nonnull String fromKey) {
        final StringDescendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey);
        return value;
    }

    private StringDescendingSimpleIterator() {
        // empty
    }

    @Override
    protected boolean ended() {
        return false;
    }

    @Override
    protected boolean nullEnded() {
        return false;
    }

    @Override
    protected boolean ended(@Nonnull String key) {
        return false;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
