package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;

final class StringAscendingSimpleIterator<V> extends AbstractStringAscendingSimpleIterator<V> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<StringAscendingSimpleIterator> POOL =
            new ArrayPool<>(POOL_SIZE, StringAscendingSimpleIterator::new);

    @SuppressWarnings("unchecked")
    public static <V> StringAscendingSimpleIterator<V> create(boolean exclusive) {
        final StringAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive);
        return value;
    }

    @SuppressWarnings("unchecked")
    public static <V> StringAscendingSimpleIterator<V> create(boolean exclusive, @Nonnull String fromKey) {
        final StringAscendingSimpleIterator<V> value = POOL.get();
        value.init(exclusive, fromKey);
        return value;
    }

    private StringAscendingSimpleIterator() {
        // empty
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
    protected boolean ended() {
        return false;
    }

    @Override
    public void close() {
        super.close();
        POOL.put(this);
    }
}
