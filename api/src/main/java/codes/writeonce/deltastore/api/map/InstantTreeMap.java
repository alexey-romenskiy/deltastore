package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.time.Instant;
import java.util.Arrays;
import java.util.function.Function;

public final class InstantTreeMap<V> extends AbstractTreeMap<Instant, V, InstantTreeMap<V>> {

    @SuppressWarnings("rawtypes")
    private static final InstantTreeMap EMPTY_MAP = new InstantTreeMap<>(1);

    protected long[] keys1;
    protected int[] keys2;

    @SuppressWarnings("unchecked")
    public static <V> InstantTreeMap<V> empty() {
        return (InstantTreeMap<V>) EMPTY_MAP;
    }

    public InstantTreeMap() {
        this(1);
    }

    public InstantTreeMap(int capacity) {
        super(capacity);
        keys1 = new long[capacity];
        keys2 = new int[capacity];
    }

    private InstantTreeMap(int root, int nullKey, int free, int end, int capacity, int size, int modCount,
            Object[] values, int[] flags, long[] keys1, int[] keys2) {
        super(root, nullKey, free, end, capacity, size, modCount, values, flags);
        this.keys1 = keys1;
        this.keys2 = keys2;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    protected InstantTreeMap<V> clone() {
        return new InstantTreeMap<>(root, nullKey, free, end, capacity, size, modCount, values.clone(), flags.clone(),
                keys1.clone(), keys2.clone());
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V get(@Nullable Instant key) {

        if (key == null) {
            if (nullKey == 0) {
                return null;
            } else {
                return (V) values[nullKey];
            }
        } else {
            return get(key.getEpochSecond(), key.getNano());
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V get(long key1, int key2) {

        int p = root;

        while (p != 0) {
            final var k1 = keys1[p];
            if (key1 < k1) {
                p = left(p);
            } else if (key1 > k1) {
                p = right(p);
            } else {
                final var k2 = keys2[p];
                if (key2 < k2) {
                    p = left(p);
                } else if (key2 > k2) {
                    p = right(p);
                } else {
                    return (V) values[p];
                }
            }
        }

        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V remove(@Nullable Instant key) {

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
            return remove(key.getEpochSecond(), key.getNano());
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V remove(long key1, int key2) {

        int p = root;

        while (p != 0) {
            final var k1 = keys1[p];
            if (key1 < k1) {
                p = flags[p * 3];
            } else if (key1 > k1) {
                p = flags[p * 3 + 1];
            } else {
                final var k2 = keys2[p];
                if (key2 < k2) {
                    p = flags[p * 3];
                } else if (key2 > k2) {
                    p = flags[p * 3 + 1];
                } else {
                    final var value = values[p];

                    // If strictly internal, copy successor's element to p and then make p
                    // point to successor.
                    if (left(p) != 0 && right(p) != 0) {
                        int s = successor(p);
                        keys1[p] = keys1[s];
                        keys2[p] = keys2[s];
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

                    keys1[p] = 0;
                    keys2[p] = 0;
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
        }

        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V put(@Nullable Instant key, @Nullable V value) {

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
            return put(key.getEpochSecond(), key.getNano(), value);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V put(long key1, int key2, @Nullable V value) {

        var t = root;
        if (t == 0) {
            final var n = allocate();
            root = n;
            keys1[n] = key1;
            keys2[n] = key2;
            values[n] = value;
            size++;
            modCount++;
            return null;
        } else {

            boolean less;
            int parent;

            do {
                parent = t;
                final long k1 = keys1[t];
                if (key1 < k1) {
                    t = left(t);
                    less = true;
                } else if (key1 > k1) {
                    t = right(t);
                    less = false;
                } else {
                    final long k2 = keys2[t];
                    if (key2 < k2) {
                        t = left(t);
                        less = true;
                    } else if (key2 > k2) {
                        t = right(t);
                        less = false;
                    } else {
                        final var oldValue = values[t];
                        values[t] = value;
                        return (V) oldValue;
                    }
                }
            } while (t != 0);

            final var e = allocate();
            keys1[e] = key1;
            keys2[e] = key2;
            values[e] = value;
            flags[e * 3 + 2] = parent;
            if (less) {
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

        keys1 = Arrays.copyOf(keys1, capacity);
        keys2 = Arrays.copyOf(keys2, capacity);
    }

    @Override
    protected void copyKey(int from, int to) {
        keys1[to] = keys1[from];
        keys2[to] = keys2[from];
        keys1[from] = 0;
        keys2[from] = 0;
    }

    @Override
    public boolean containsKey(@Nullable Instant key) {

        if (key == null) {
            return nullKey != 0;
        } else {
            return containsKey(key.getEpochSecond(), key.getNano());
        }
    }

    public boolean containsKey(long key1, int key2) {

        int p = root;

        while (p != 0) {
            final var k1 = keys1[p];
            if (key1 < k1) {
                p = left(p);
            } else if (key1 > k1) {
                p = right(p);
            } else {
                final var k2 = keys2[p];
                if (key2 < k2) {
                    p = left(p);
                } else if (key2 > k2) {
                    p = right(p);
                } else {
                    return true;
                }
            }
        }

        return false;
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V getOrDefault(@Nullable Instant key, @Nullable V defaultValue) {

        if (key == null) {
            if (nullKey == 0) {
                return defaultValue;
            } else {
                return (V) values[nullKey];
            }
        } else {
            return getOrDefault(key.getEpochSecond(), key.getNano(), defaultValue);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V getOrDefault(long key1, int key2, @Nullable V defaultValue) {

        int p = root;

        while (p != 0) {
            final var k1 = keys1[p];
            if (key1 < k1) {
                p = left(p);
            } else if (key1 > k1) {
                p = right(p);
            } else {
                final var k2 = keys2[p];
                if (key2 < k2) {
                    p = left(p);
                } else if (key2 > k2) {
                    p = right(p);
                } else {
                    return (V) values[p];
                }
            }
        }

        return defaultValue;
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V computeIfAbsent(@Nullable Instant key, @Nonnull Function<? super Instant, ? extends V> mappingFunction) {

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
            return computeIfAbsent(key.getEpochSecond(), key.getNano(), key, mappingFunction);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V computeIfAbsent(long key1, int key2, @Nonnull Instant key,
            @Nonnull Function<? super Instant, ? extends V> mappingFunction) {

        int p = root;

        while (p != 0) {
            final var k1 = keys1[p];
            if (key1 < k1) {
                p = left(p);
            } else if (key1 > k1) {
                p = right(p);
            } else {
                final var k2 = keys2[p];
                if (key2 < k2) {
                    p = left(p);
                } else if (key2 > k2) {
                    p = right(p);
                } else {
                    return (V) values[p];
                }
            }
        }

        final V v = mappingFunction.apply(key);
        if (v != null) {
            put(key1, key2, v);
        }
        return v;
    }

    protected int getCeilingEntry(long key1, int key2) {

        int p = root;
        while (p != 0) {
            final var k1 = keys1[p];
            if (key1 < k1) {
                final var left = left(p);
                if (left != 0) {
                    p = left;
                } else {
                    return p;
                }
            } else if (key1 > k1) {
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
                final var k2 = keys2[p];
                if (key2 < k2) {
                    final var left = left(p);
                    if (left != 0) {
                        p = left;
                    } else {
                        return p;
                    }
                } else if (key2 > k2) {
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
        }
        return 0;
    }

    protected int getFloorEntry(long key1, int key2) {

        int p = root;
        while (p != 0) {
            final var k1 = keys1[p];
            if (key1 > k1) {
                final var right = right(p);
                if (right != 0) {
                    p = right;
                } else {
                    return p;
                }
            } else if (key1 < k1) {
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
                final var k2 = keys2[p];
                if (key2 > k2) {
                    final var right = right(p);
                    if (right != 0) {
                        p = right;
                    } else {
                        return p;
                    }
                } else if (key2 < k2) {
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
        }
        return 0;
    }

    protected int getHigherEntry(long key1, int key2) {

        int p = root;
        while (p != 0) {
            final var k1 = keys1[p];
            if (key1 < k1 || key1 == k1 && key2 < keys2[p]) {
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

    protected int getLowerEntry(long key1, int key2) {

        int p = root;
        while (p != 0) {
            final var k1 = keys1[p];
            if (key1 > k1 || key1 == k1 && key2 > keys2[p]) {
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
    public NestedIterator<InstantTreeMap<V>, V> iterator(boolean reversed) {

        final NestedIterator<InstantTreeMap<V>, V> it = newIterator(reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<InstantTreeMap<V>, T> iterator(@Nonnull NestedIterator<V, T> iterator,
            boolean reversed) {

        final NestedIterator<InstantTreeMap<V>, T> it = newIterator(iterator, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<InstantTreeMap<V>, V> iteratorByKey(@Nullable Instant key) {

        final NestedIterator<InstantTreeMap<V>, V> it = newIteratorByKey(key);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<InstantTreeMap<V>, T> iteratorByKey(@Nonnull NestedIterator<V, T> iterator,
            @Nullable Instant key) {

        final NestedIterator<InstantTreeMap<V>, T> it = newIteratorByKey(iterator, key);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<InstantTreeMap<V>, V> iteratorByKeys(@Nonnull Instant[] keys) {

        final NestedIterator<InstantTreeMap<V>, V> it = newIteratorByKeys(keys);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<InstantTreeMap<V>, T> iteratorByKeys(@Nonnull NestedIterator<V, T> iterator,
            @Nonnull Instant[] keys) {

        final NestedIterator<InstantTreeMap<V>, T> it = newIteratorByKeys(iterator, keys);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<InstantTreeMap<V>, V> iteratorByRange(@Nullable Instant fromKey, boolean fromExclusive,
            @Nullable Instant toKey, boolean toExclusive, boolean reversed) {

        final NestedIterator<InstantTreeMap<V>, V> it =
                newIteratorByRange(fromKey, fromExclusive, toKey, toExclusive, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<InstantTreeMap<V>, T> iteratorByRange(@Nonnull NestedIterator<V, T> iterator,
            @Nullable Instant fromKey, boolean fromExclusive, @Nullable Instant toKey, boolean toExclusive,
            boolean reversed) {

        final NestedIterator<InstantTreeMap<V>, T> it =
                newIteratorByRange(iterator, fromKey, fromExclusive, toKey, toExclusive, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    public static <V> NestedIterator<InstantTreeMap<V>, V> newIterator(boolean reversed) {
        return reversed ? InstantDescendingSimpleIterator.create() : InstantAscendingSimpleIterator.create(false);
    }

    @Nonnull
    public static <V, T> NestedIterator<InstantTreeMap<V>, T> newIterator(
            @Nonnull NestedIterator<V, T> iterator, boolean reversed) {

        return reversed
                ? InstantDescendingChainedIterator.create(iterator)
                : InstantAscendingChainedIterator.create(iterator, false);
    }

    @Nonnull
    public static <V> NestedIterator<InstantTreeMap<V>, V> newIteratorByKey(@Nullable Instant key) {
        return SingletonSimpleIterator.create(key);
    }

    @Nonnull
    public static <V, T> NestedIterator<InstantTreeMap<V>, T> newIteratorByKey(
            @Nonnull NestedIterator<V, T> iterator, @Nullable Instant key) {
        return SingletonChainedIterator.create(iterator, key);
    }

    @Nonnull
    public static <V> NestedIterator<InstantTreeMap<V>, V> newIteratorByKeys(@Nonnull Instant[] keys) {
        return ArraySimpleIterator.create(keys);
    }

    @Nonnull
    public static <V, T> NestedIterator<InstantTreeMap<V>, T> newIteratorByKeys(
            @Nonnull NestedIterator<V, T> iterator, @Nonnull Instant[] keys) {

        return ArrayChainedIterator.create(iterator, keys);
    }

    @Nonnull
    public static <V> NestedIterator<InstantTreeMap<V>, V> newIteratorByRange(
            @Nullable Instant fromKey, boolean fromExclusive, @Nullable Instant toKey, boolean toExclusive,
            boolean reversed) {

        if (reversed) {
            if (toKey == null) {
                if (fromKey == null) {
                    return InstantDescendingSimpleIterator.create();
                } else if (fromExclusive) {
                    return ToKeyExclusiveInstantDescendingSimpleIterator.create(fromKey.getEpochSecond(),
                            fromKey.getNano());
                } else {
                    return ToKeyInclusiveInstantDescendingSimpleIterator.create(fromKey.getEpochSecond(),
                            fromKey.getNano());
                }
            } else {
                if (fromKey == null) {
                    return InstantDescendingSimpleIterator.create(toExclusive, toKey.getEpochSecond(), toKey.getNano());
                } else if (fromExclusive) {
                    return ToKeyExclusiveInstantDescendingSimpleIterator.create(toExclusive, toKey.getEpochSecond(),
                            toKey.getNano(), fromKey.getEpochSecond(), fromKey.getNano());
                } else {
                    return ToKeyInclusiveInstantDescendingSimpleIterator.create(toExclusive, toKey.getEpochSecond(),
                            toKey.getNano(), fromKey.getEpochSecond(), fromKey.getNano());
                }
            }
        } else {
            if (fromKey == null) {
                if (toKey == null) {
                    return InstantAscendingSimpleIterator.create(false);
                } else if (toExclusive) {
                    return ToKeyExclusiveInstantAscendingSimpleIterator.create(false, toKey.getEpochSecond(),
                            toKey.getNano());
                } else {
                    return ToKeyInclusiveInstantAscendingSimpleIterator.create(false, toKey.getEpochSecond(),
                            toKey.getNano());
                }
            } else {
                if (toKey == null) {
                    return InstantAscendingSimpleIterator.create(fromExclusive, fromKey.getEpochSecond(),
                            fromKey.getNano());
                } else if (toExclusive) {
                    return ToKeyExclusiveInstantAscendingSimpleIterator.create(fromExclusive, fromKey.getEpochSecond(),
                            fromKey.getNano(), toKey.getEpochSecond(), toKey.getNano());
                } else {
                    return ToKeyInclusiveInstantAscendingSimpleIterator.create(fromExclusive, fromKey.getEpochSecond(),
                            fromKey.getNano(), toKey.getEpochSecond(), toKey.getNano());
                }
            }
        }
    }

    @Nonnull
    public static <V, T> NestedIterator<InstantTreeMap<V>, T> newIteratorByRange(
            @Nonnull NestedIterator<V, T> iterator, @Nullable Instant fromKey, boolean fromExclusive,
            @Nullable Instant toKey, boolean toExclusive, boolean reversed) {

        if (reversed) {
            if (toKey == null) {
                if (fromKey == null) {
                    return InstantDescendingChainedIterator.create(iterator);
                } else if (fromExclusive) {
                    return ToKeyExclusiveInstantDescendingChainedIterator.create(iterator, fromKey.getEpochSecond(),
                            fromKey.getNano());
                } else {
                    return ToKeyInclusiveInstantDescendingChainedIterator.create(iterator, fromKey.getEpochSecond(),
                            fromKey.getNano());
                }
            } else {
                if (fromKey == null) {
                    return InstantDescendingChainedIterator.create(iterator, toExclusive, toKey.getEpochSecond(),
                            toKey.getNano());
                } else if (fromExclusive) {
                    return ToKeyExclusiveInstantDescendingChainedIterator.create(iterator, toExclusive,
                            toKey.getEpochSecond(), toKey.getNano(), fromKey.getEpochSecond(), fromKey.getNano());
                } else {
                    return ToKeyInclusiveInstantDescendingChainedIterator.create(iterator, toExclusive,
                            toKey.getEpochSecond(), toKey.getNano(), fromKey.getEpochSecond(), fromKey.getNano());
                }
            }
        } else {
            if (fromKey == null) {
                if (toKey == null) {
                    return InstantAscendingChainedIterator.create(iterator, false);
                } else if (toExclusive) {
                    return ToKeyExclusiveInstantAscendingChainedIterator.create(iterator, false, toKey.getEpochSecond(),
                            toKey.getNano());
                } else {
                    return ToKeyInclusiveInstantAscendingChainedIterator.create(iterator, false, toKey.getEpochSecond(),
                            toKey.getNano());
                }
            } else {
                if (toKey == null) {
                    return InstantAscendingChainedIterator.create(iterator, fromExclusive, fromKey.getEpochSecond(),
                            fromKey.getNano());
                } else if (toExclusive) {
                    return ToKeyExclusiveInstantAscendingChainedIterator.create(iterator, fromExclusive,
                            fromKey.getEpochSecond(), fromKey.getNano(), toKey.getEpochSecond(), toKey.getNano());
                } else {
                    return ToKeyInclusiveInstantAscendingChainedIterator.create(iterator, fromExclusive,
                            fromKey.getEpochSecond(), fromKey.getNano(), toKey.getEpochSecond(), toKey.getNano());
                }
            }
        }
    }
}
