package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.function.Function;
import java.util.function.LongFunction;

public final class LongTreeMap<V> extends AbstractTreeMap<Long, V, LongTreeMap<V>> {

    @SuppressWarnings("rawtypes")
    private static final LongTreeMap EMPTY_MAP = new LongTreeMap<>(1);

    protected long[] keys;

    @SuppressWarnings("unchecked")
    public static <V> LongTreeMap<V> empty() {
        return (LongTreeMap<V>) EMPTY_MAP;
    }

    public LongTreeMap() {
        this(1);
    }

    public LongTreeMap(int capacity) {
        super(capacity);
        keys = new long[capacity];
    }

    private LongTreeMap(int root, int nullKey, int free, int end, int capacity, int size, int modCount,
            Object[] values, int[] flags, long[] keys) {
        super(root, nullKey, free, end, capacity, size, modCount, values, flags);
        this.keys = keys;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    protected LongTreeMap<V> clone() {
        return new LongTreeMap<>(root, nullKey, free, end, capacity, size, modCount, values.clone(), flags.clone(),
                keys.clone());
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V get(@Nullable Long key) {

        if (key == null) {
            if (nullKey == 0) {
                return null;
            } else {
                return (V) values[nullKey];
            }
        } else {
            return get((long) key);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V get(long key) {

        int p = root;

        while (p != 0) {
            final var k = keys[p];
            if (key < k) {
                p = left(p);
            } else if (key > k) {
                p = right(p);
            } else {
                return (V) values[p];
            }
        }

        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V remove(@Nullable Long key) {

        if (key == null) {
            final var n = nullKey;
            if (n == 0) {
                return null;
            } else {
                final var value = values[n];
                values[n] = null;
                nullKey = 0;
                size--;
                modCount++;
                free(n);
                return (V) value;
            }
        } else {
            return remove((long) key);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V remove(long key) {

        int p = root;

        while (p != 0) {
            final var k = keys[p];
            if (key < k) {
                p = flags[p * 3];
            } else if (key > k) {
                p = flags[p * 3 + 1];
            } else {
                final var value = values[p];

                // If strictly internal, copy successor's element to p and then make p
                // point to successor.
                if (left(p) != 0 && right(p) != 0) {
                    int s = successor(p);
                    keys[p] = keys[s];
                    values[p] = values[s];
                    p = s;
                } // p has 2 children

                // Start fixup at replacement node, if it exists.
                int r = left(p) == 0 ? right(p) : left(p);

                final var parent = parent(p);

                if (r != 0) {
                    // Link replacement to parent
                    final var iParentR = r * 3 + 2;
                    flags[iParentR] = flags[iParentR] & RED | parent;
                    if (parent == 0) {
                        root = r;
                    } else if (p == left(parent)) {
                        flags[parent * 3] = r;
                    } else {
                        flags[parent * 3 + 1] = r;
                    }

                    // Fix replacement
                    if (!red(p)) {
                        fixAfterDeletion(r);
                    }
                } else if (parent == 0) { // return if we are the only node.
                    root = 0;
                } else { //  No children. Use self as phantom replacement and unlink.
                    if (!red(p)) {
                        fixAfterDeletion(p);
                    }

                    if (p == left(parent)) {
                        flags[parent * 3] = 0;
                    } else if (p == right(parent)) {
                        flags[parent * 3 + 1] = 0;
                    }
                }

                keys[p] = 0;
                values[p] = null;
                flags[p * 3] = 0;
                flags[p * 3 + 1] = 0;
                flags[p * 3 + 2] = 0;
                size--;
                modCount++;
                free(p);
                return (V) value;
            }
        }

        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V put(@Nullable Long key, @Nullable V value) {

        if (key == null) {
            int n = nullKey;
            if (n == 0) {
                n = allocate();
                nullKey = n;
                values[n] = value;
                size++;
                modCount++;
                return null;
            } else {
                final var oldValue = values[n];
                values[n] = value;
                return (V) oldValue;
            }
        } else {
            return put((long) key, value);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V put(long key, @Nullable V value) {

        var t = root;
        if (t == 0) {
            final var n = allocate();
            root = n;
            keys[n] = key;
            values[n] = value;
            size++;
            modCount++;
            return null;
        } else {

            long k;
            int parent;

            do {
                parent = t;
                k = keys[t];
                if (key < k) {
                    t = left(t);
                } else if (key > k) {
                    t = right(t);
                } else {
                    final var oldValue = values[t];
                    values[t] = value;
                    return (V) oldValue;
                }
            } while (t != 0);

            final var e = allocate();
            keys[e] = key;
            values[e] = value;
            flags[e * 3 + 2] = parent;
            if (key < k) {
                flags[parent * 3] = e;
            } else {
                flags[parent * 3 + 1] = e;
            }

            fixAfterInsertion(e);
            size++;
            modCount++;
            return null;
        }
    }

    @Override
    protected void realloc(int capacity) {
        keys = Arrays.copyOf(keys, capacity);
    }

    @Override
    protected void copyKey(int from, int to) {
        keys[to] = keys[from];
        keys[from] = 0;
    }

    @Override
    public boolean containsKey(@Nullable Long key) {

        if (key == null) {
            return nullKey != 0;
        } else {
            return containsKey((long) key);
        }
    }

    public boolean containsKey(long key) {

        int p = root;

        while (p != 0) {
            final var k = keys[p];
            if (key < k) {
                p = left(p);
            } else if (key > k) {
                p = right(p);
            } else {
                return true;
            }
        }

        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V getOrDefault(@Nullable Long key, @Nullable V defaultValue) {

        if (key == null) {
            if (nullKey == 0) {
                return defaultValue;
            } else {
                return (V) values[nullKey];
            }
        } else {
            return getOrDefault((long) key, defaultValue);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V getOrDefault(long key, @Nullable V defaultValue) {

        int p = root;

        while (p != 0) {
            final var k = keys[p];
            if (key < k) {
                p = left(p);
            } else if (key > k) {
                p = right(p);
            } else {
                return (V) values[p];
            }
        }

        return defaultValue;
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V computeIfAbsent(@Nullable Long key, @Nonnull Function<? super Long, ? extends V> mappingFunction) {

        if (key == null) {
            if (nullKey == 0) {
                final V v = mappingFunction.apply(key);
                if (v != null) {
                    put(key, v);
                }
                return v;
            } else {
                return (V) values[nullKey];
            }
        } else {
            return computeIfAbsent((long) key, k -> mappingFunction.apply(key));
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V computeIfAbsent(long key, @Nonnull LongFunction<? extends V> mappingFunction) {

        int p = root;

        while (p != 0) {
            final var k = keys[p];
            if (key < k) {
                p = left(p);
            } else if (key > k) {
                p = right(p);
            } else {
                return (V) values[p];
            }
        }

        final V v = mappingFunction.apply(key);
        if (v != null) {
            put(key, v);
        }
        return v;
    }

    protected int getCeilingEntry(long key) {

        int p = root;
        while (p != 0) {
            final var k = keys[p];
            if (key < k) {
                final var left = left(p);
                if (left != 0) {
                    p = left;
                } else {
                    return p;
                }
            } else if (key > k) {
                final var right = right(p);
                if (right != 0) {
                    p = right;
                } else {
                    int parent = parent(p);
                    int ch = p;
                    while (parent != 0 && ch == right(parent)) {
                        ch = parent;
                        parent = parent(parent);
                    }
                    return parent;
                }
            } else {
                return p;
            }
        }
        return 0;
    }

    protected int getFloorEntry(long key) {

        int p = root;
        while (p != 0) {
            final var k = keys[p];
            if (key > k) {
                final var right = right(p);
                if (right != 0) {
                    p = right;
                } else {
                    return p;
                }
            } else if (key < k) {
                final var left = left(p);
                if (left != 0) {
                    p = left;
                } else {
                    int parent = parent(p);
                    int ch = p;
                    while (parent != 0 && ch == left(parent)) {
                        ch = parent;
                        parent = parent(parent);
                    }
                    return parent;
                }
            } else {
                return p;
            }
        }
        return 0;
    }

    protected int getHigherEntry(long key) {

        int p = root;
        while (p != 0) {
            final var k = keys[p];
            if (key < k) {
                final var left = left(p);
                if (left != 0) {
                    p = left;
                } else {
                    return p;
                }
            } else {
                final var right = right(p);
                if (right != 0) {
                    p = right;
                } else {
                    int parent = parent(p);
                    int ch = p;
                    while (parent != 0 && ch == right(parent)) {
                        ch = parent;
                        parent = parent(parent);
                    }
                    return parent;
                }
            }
        }
        return 0;
    }

    protected int getLowerEntry(long key) {

        int p = root;
        while (p != 0) {
            final var k = keys[p];
            if (key > k) {
                final var right = right(p);
                if (right != 0) {
                    p = right;
                } else {
                    return p;
                }
            } else {
                final var left = left(p);
                if (left != 0) {
                    p = left;
                } else {
                    int parent = parent(p);
                    int ch = p;
                    while (parent != 0 && ch == left(parent)) {
                        ch = parent;
                        parent = parent(parent);
                    }
                    return parent;
                }
            }
        }
        return 0;
    }

    @Nonnull
    @Override
    public NestedIterator<LongTreeMap<V>, V> iterator(boolean reversed) {

        final NestedIterator<LongTreeMap<V>, V> it = newIterator(reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<LongTreeMap<V>, T> iterator(@Nonnull NestedIterator<V, T> iterator,
            boolean reversed) {

        final NestedIterator<LongTreeMap<V>, T> it = newIterator(iterator, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<LongTreeMap<V>, V> iteratorByKey(@Nullable Long key) {

        final NestedIterator<LongTreeMap<V>, V> it = newIteratorByKey(key);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<LongTreeMap<V>, T> iteratorByKey(@Nonnull NestedIterator<V, T> iterator,
            @Nullable Long key) {

        final NestedIterator<LongTreeMap<V>, T> it = newIteratorByKey(iterator, key);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<LongTreeMap<V>, V> iteratorByKeys(@Nonnull Long[] keys) {

        final NestedIterator<LongTreeMap<V>, V> it = newIteratorByKeys(keys);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<LongTreeMap<V>, T> iteratorByKeys(@Nonnull NestedIterator<V, T> iterator,
            @Nonnull Long[] keys) {

        final NestedIterator<LongTreeMap<V>, T> it = newIteratorByKeys(iterator, keys);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<LongTreeMap<V>, V> iteratorByRange(@Nullable Long fromKey, boolean fromExclusive,
            @Nullable Long toKey, boolean toExclusive, boolean reversed) {

        final NestedIterator<LongTreeMap<V>, V> it =
                newIteratorByRange(fromKey, fromExclusive, toKey, toExclusive, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<LongTreeMap<V>, T> iteratorByRange(@Nonnull NestedIterator<V, T> iterator,
            @Nullable Long fromKey, boolean fromExclusive, @Nullable Long toKey, boolean toExclusive,
            boolean reversed) {

        final NestedIterator<LongTreeMap<V>, T> it =
                newIteratorByRange(iterator, fromKey, fromExclusive, toKey, toExclusive, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    public static <V> NestedIterator<LongTreeMap<V>, V> newIterator(boolean reversed) {
        return reversed ? LongDescendingSimpleIterator.create() : LongAscendingSimpleIterator.create(false);
    }

    @Nonnull
    public static <V, T> NestedIterator<LongTreeMap<V>, T> newIterator(
            @Nonnull NestedIterator<V, T> iterator, boolean reversed) {

        return reversed
                ? LongDescendingChainedIterator.create(iterator)
                : LongAscendingChainedIterator.create(iterator, false);
    }

    @Nonnull
    public static <V> NestedIterator<LongTreeMap<V>, V> newIteratorByKey(@Nullable Long key) {
        return SingletonSimpleIterator.create(key);
    }

    @Nonnull
    public static <V, T> NestedIterator<LongTreeMap<V>, T> newIteratorByKey(
            @Nonnull NestedIterator<V, T> iterator, @Nullable Long key) {
        return SingletonChainedIterator.create(iterator, key);
    }

    @Nonnull
    public static <V> NestedIterator<LongTreeMap<V>, V> newIteratorByKeys(@Nonnull Long[] keys) {
        return ArraySimpleIterator.create(keys);
    }

    @Nonnull
    public static <V, T> NestedIterator<LongTreeMap<V>, T> newIteratorByKeys(
            @Nonnull NestedIterator<V, T> iterator, @Nonnull Long[] keys) {

        return ArrayChainedIterator.create(iterator, keys);
    }

    @Nonnull
    public static <V> NestedIterator<LongTreeMap<V>, V> newIteratorByRange(
            @Nullable Long fromKey, boolean fromExclusive, @Nullable Long toKey, boolean toExclusive,
            boolean reversed) {

        if (reversed) {
            if (toKey == null) {
                if (fromKey == null) {
                    return LongDescendingSimpleIterator.create();
                } else if (fromExclusive) {
                    return ToKeyExclusiveLongDescendingSimpleIterator.create(fromKey);
                } else {
                    return ToKeyInclusiveLongDescendingSimpleIterator.create(fromKey);
                }
            } else {
                if (fromKey == null) {
                    return LongDescendingSimpleIterator.create(toExclusive, toKey);
                } else if (fromExclusive) {
                    return ToKeyExclusiveLongDescendingSimpleIterator.create(toExclusive, toKey, fromKey);
                } else {
                    return ToKeyInclusiveLongDescendingSimpleIterator.create(toExclusive, toKey, fromKey);
                }
            }
        } else {
            if (fromKey == null) {
                if (toKey == null) {
                    return LongAscendingSimpleIterator.create(false);
                } else if (toExclusive) {
                    return ToKeyExclusiveLongAscendingSimpleIterator.create(false, toKey);
                } else {
                    return ToKeyInclusiveLongAscendingSimpleIterator.create(false, toKey);
                }
            } else {
                if (toKey == null) {
                    return LongAscendingSimpleIterator.create(fromExclusive, fromKey);
                } else if (toExclusive) {
                    return ToKeyExclusiveLongAscendingSimpleIterator.create(fromExclusive, fromKey, toKey);
                } else {
                    return ToKeyInclusiveLongAscendingSimpleIterator.create(fromExclusive, fromKey, toKey);
                }
            }
        }
    }

    @Nonnull
    public static <V, T> NestedIterator<LongTreeMap<V>, T> newIteratorByRange(
            @Nonnull NestedIterator<V, T> iterator, @Nullable Long fromKey, boolean fromExclusive,
            @Nullable Long toKey, boolean toExclusive, boolean reversed) {

        if (reversed) {
            if (toKey == null) {
                if (fromKey == null) {
                    return LongDescendingChainedIterator.create(iterator);
                } else if (fromExclusive) {
                    return ToKeyExclusiveLongDescendingChainedIterator.create(iterator, fromKey);
                } else {
                    return ToKeyInclusiveLongDescendingChainedIterator.create(iterator, fromKey);
                }
            } else {
                if (fromKey == null) {
                    return LongDescendingChainedIterator.create(iterator, toExclusive, toKey);
                } else if (fromExclusive) {
                    return ToKeyExclusiveLongDescendingChainedIterator.create(iterator, toExclusive, toKey, fromKey);
                } else {
                    return ToKeyInclusiveLongDescendingChainedIterator.create(iterator, toExclusive, toKey, fromKey);
                }
            }
        } else {
            if (fromKey == null) {
                if (toKey == null) {
                    return LongAscendingChainedIterator.create(iterator, false);
                } else if (toExclusive) {
                    return ToKeyExclusiveLongAscendingChainedIterator.create(iterator, false, toKey);
                } else {
                    return ToKeyInclusiveLongAscendingChainedIterator.create(iterator, false, toKey);
                }
            } else {
                if (toKey == null) {
                    return LongAscendingChainedIterator.create(iterator, fromExclusive, fromKey);
                } else if (toExclusive) {
                    return ToKeyExclusiveLongAscendingChainedIterator.create(iterator, fromExclusive, fromKey, toKey);
                } else {
                    return ToKeyInclusiveLongAscendingChainedIterator.create(iterator, fromExclusive, fromKey, toKey);
                }
            }
        }
    }
}
