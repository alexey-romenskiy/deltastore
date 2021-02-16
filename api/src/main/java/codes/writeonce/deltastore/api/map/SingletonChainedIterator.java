package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

final class SingletonChainedIterator<K, V, M extends AbstractTreeMap<K, V, M>, T> extends NestedIterator<M, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<SingletonChainedIterator> POOL =
            new ArrayPool<>(POOL_SIZE, SingletonChainedIterator::new);

    private NestedIterator<V, T> iterator;

    private K key;

    protected M map;

    protected int expectedModCount;

    private boolean index;

    private boolean cached;

    private boolean keep;

    @SuppressWarnings("unchecked")
    public static <K, V, M extends AbstractTreeMap<K, V, M>, T> SingletonChainedIterator<K, V, M, T> create(
            @Nonnull NestedIterator<V, T> iterator, @Nullable K key) {
        final SingletonChainedIterator<K, V, M, T> value = POOL.get();
        value.init(iterator, key);
        return value;
    }

    private SingletonChainedIterator() {
        // empty
    }

    public void init(@Nonnull NestedIterator<V, T> iterator, @Nullable K key) {
        this.iterator = iterator;
        this.key = key;
    }

    @Nullable
    @Override
    public T get() {

        if (cached && expectedModCount == map.modCount) {
            final var value = iterator.get();
            if (value != null) {
                return value;
            }
        }

        if (!index) {
            final var next = map.get(key);
            if (next != null) {
                if (keep) {
                    iterator.map(next);
                } else {
                    iterator.reset(next);
                }
                final var value = iterator.get();
                if (value != null) {
                    expectedModCount = map.modCount;
                    cached = true;
                    keep = true;
                    return value;
                }
            }
            index = true;
            keep = false;
        }

        cached = false;
        return null;
    }

    @Override
    public boolean hasNext() {

        if (cached && expectedModCount == map.modCount) {
            final var value = iterator.hasNext();
            if (value) {
                return true;
            }
        }

        if (!index) {
            final var next = map.get(key);
            if (next != null) {
                if (keep) {
                    iterator.map(next);
                } else {
                    iterator.reset(next);
                }
                final var value = iterator.hasNext();
                if (value) {
                    expectedModCount = map.modCount;
                    cached = true;
                    keep = true;
                    return true;
                }
            }
            index = true;
            keep = false;
        }

        cached = false;
        return false;
    }

    @Override
    public void reset(@Nonnull M map) {

        this.map = map;
        this.index = false;
        this.cached = false;
        this.keep = false;
    }

    @Override
    public void map(@Nonnull M map) {

        if (this.map != map) {
            this.map = map;
            this.cached = false;
        }
    }

    @Override
    public void close() {
        super.close();
        iterator.close();
        this.iterator = null;
        this.key = null;
        this.map = null;
        POOL.put(this);
    }
}
