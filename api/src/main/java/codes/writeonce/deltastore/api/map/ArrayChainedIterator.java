package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.ArrayPool;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

final class ArrayChainedIterator<K, V, M extends AbstractTreeMap<K, V, M>, T> extends NestedIterator<M, T> {

    @SuppressWarnings("rawtypes")
    private static final ArrayPool<ArrayChainedIterator> POOL = new ArrayPool<>(POOL_SIZE, ArrayChainedIterator::new);

    private NestedIterator<V, T> iterator;

    private K[] keys;

    protected M map;

    protected int expectedModCount;

    private int index;

    private boolean cached;

    private boolean keep;

    @SuppressWarnings("unchecked")
    public static <K, V, M extends AbstractTreeMap<K, V, M>, T> ArrayChainedIterator<K, V, M, T> create(
            @Nonnull NestedIterator<V, T> iterator, @Nonnull K[] keys) {
        final ArrayChainedIterator<K, V, M, T> value = POOL.get();
        value.init(iterator, keys);
        return value;
    }

    private ArrayChainedIterator() {
        // empty
    }

    private void init(@Nonnull NestedIterator<V, T> iterator, @Nonnull K[] keys) {
        this.iterator = iterator;
        this.keys = keys;
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

        while (index < keys.length) {
            final var next = map.get(keys[index]);
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
            index++;
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

        while (index < keys.length) {
            final var next = map.get(keys[index]);
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
            index++;
            keep = false;
        }

        cached = false;
        return false;
    }

    @Override
    public void reset(@Nonnull M map) {

        this.map = map;
        this.index = 0;
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
        this.keys = null;
        this.map = null;
        POOL.put(this);
    }
}
