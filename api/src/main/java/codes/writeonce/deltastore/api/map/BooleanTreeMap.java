package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.function.Function;

public final class BooleanTreeMap<V> extends AbstractTreeMap<Boolean, V, BooleanTreeMap<V>> {

    @SuppressWarnings("rawtypes")
    private static final BooleanTreeMap EMPTY_MAP = new BooleanTreeMap<>(1);

    protected boolean[] keys;

    @SuppressWarnings("unchecked")
    public static <V> BooleanTreeMap<V> empty() {
        return (BooleanTreeMap<V>) EMPTY_MAP;
    }

    public BooleanTreeMap() {
        this(1);
    }

    public BooleanTreeMap(int capacity) {
        super(capacity);
        keys = new boolean[capacity];
    }

    private BooleanTreeMap(int root, int nullKey, int free, int end, int capacity, int size, int modCount,
            Object[] values, int[] flags, boolean[] keys) {
        super(root, nullKey, free, end, capacity, size, modCount, values, flags);
        this.keys = keys;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    protected BooleanTreeMap<V> clone() {
        return new BooleanTreeMap<>(root, nullKey, free, end, capacity, size, modCount, values.clone(), flags.clone(),
                keys.clone());
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V get(@Nullable Boolean key) {

        if (key == null) {
            if (nullKey == 0) {
                return null;
            } else {
                return (V) values[nullKey];
            }
        } else {
            return get((boolean) key);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V get(boolean key) {

        int p = root;

        while (p != 0) {
            final var k = keys[p];
            if (!key && k) {
                p = left(p);
            } else if (key && !k) {
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
    public V remove(@Nullable Boolean key) {

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
            return remove((boolean) key);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V remove(boolean key) {

        int p = root;

        while (p != 0) {
            final var k = keys[p];
            if (!key && k) {
                p = flags[p * 3];
            } else if (key && !k) {
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

                keys[p] = false;
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
    public V put(@Nullable Boolean key, @Nullable V value) {

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
            return put((boolean) key, value);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V put(boolean key, @Nullable V value) {

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

            boolean k;
            int parent;

            do {
                parent = t;
                k = keys[t];
                if (!key && k) {
                    t = left(t);
                } else if (key && !k) {
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
            if (!key/* && k*/) {
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
    protected int addCapacity() {
        return this.capacity + 1;
    }

    @Override
    protected boolean compact(int used) {
        return capacity > used;
    }

    @Override
    protected void realloc(int capacity) {
        keys = Arrays.copyOf(keys, capacity);
    }

    @Override
    protected void copyKey(int from, int to) {
        keys[to] = keys[from];
        keys[from] = false;
    }

    @Override
    public boolean containsKey(@Nullable Boolean key) {

        if (key == null) {
            return nullKey != 0;
        } else {
            return containsKey((boolean) key);
        }
    }

    public boolean containsKey(boolean key) {

        int p = root;

        while (p != 0) {
            final var k = keys[p];
            if (!key && k) {
                p = left(p);
            } else if (key && !k) {
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
    public V getOrDefault(@Nullable Boolean key, @Nullable V defaultValue) {

        if (key == null) {
            if (nullKey == 0) {
                return defaultValue;
            } else {
                return (V) values[nullKey];
            }
        } else {
            return getOrDefault((boolean) key, defaultValue);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V getOrDefault(boolean key, @Nullable V defaultValue) {

        int p = root;

        while (p != 0) {
            final var k = keys[p];
            if (!key && k) {
                p = left(p);
            } else if (key && !k) {
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
    public V computeIfAbsent(@Nullable Boolean key, @Nonnull Function<? super Boolean, ? extends V> mappingFunction) {

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
            return computeIfAbsent((boolean) key, mappingFunction);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V computeIfAbsent(boolean key, @Nonnull Function<? super Boolean, ? extends V> mappingFunction) {

        int p = root;

        while (p != 0) {
            final var k = keys[p];
            if (!key && k) {
                p = left(p);
            } else if (key && !k) {
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

    protected int getCeilingEntry(boolean key) {

        int p = root;
        while (p != 0) {
            final var k = keys[p];
            if (!key && k) {
                final var left = left(p);
                if (left != 0) {
                    p = left;
                } else {
                    return p;
                }
            } else if (key && !k) {
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

    protected int getFloorEntry(boolean key) {

        int p = root;
        while (p != 0) {
            final var k = keys[p];
            if (key && !k) {
                final var right = right(p);
                if (right != 0) {
                    p = right;
                } else {
                    return p;
                }
            } else if (!key && k) {
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

    protected int getHigherEntry(boolean key) {

        int p = root;
        while (p != 0) {
            final var k = keys[p];
            if (!key && k) {
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

    protected int getLowerEntry(boolean key) {

        int p = root;
        while (p != 0) {
            final var k = keys[p];
            if (key && !k) {
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
    public NestedIterator<BooleanTreeMap<V>, V> iterator(boolean reversed) {

        final NestedIterator<BooleanTreeMap<V>, V> it = newIterator(reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<BooleanTreeMap<V>, T> iterator(@Nonnull NestedIterator<V, T> iterator,
            boolean reversed) {

        final NestedIterator<BooleanTreeMap<V>, T> it = newIterator(iterator, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<BooleanTreeMap<V>, V> iteratorByKey(@Nullable Boolean key) {

        final NestedIterator<BooleanTreeMap<V>, V> it = newIteratorByKey(key);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<BooleanTreeMap<V>, T> iteratorByKey(@Nonnull NestedIterator<V, T> iterator,
            @Nullable Boolean key) {

        final NestedIterator<BooleanTreeMap<V>, T> it = newIteratorByKey(iterator, key);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<BooleanTreeMap<V>, V> iteratorByKeys(@Nonnull Boolean[] keys) {

        final NestedIterator<BooleanTreeMap<V>, V> it = newIteratorByKeys(keys);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<BooleanTreeMap<V>, T> iteratorByKeys(@Nonnull NestedIterator<V, T> iterator,
            @Nonnull Boolean[] keys) {

        final NestedIterator<BooleanTreeMap<V>, T> it = newIteratorByKeys(iterator, keys);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<BooleanTreeMap<V>, V> iteratorByRange(@Nullable Boolean fromKey, boolean fromExclusive,
            @Nullable Boolean toKey, boolean toExclusive, boolean reversed) {

        final NestedIterator<BooleanTreeMap<V>, V> it =
                newIteratorByRange(fromKey, fromExclusive, toKey, toExclusive, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<BooleanTreeMap<V>, T> iteratorByRange(@Nonnull NestedIterator<V, T> iterator,
            @Nullable Boolean fromKey, boolean fromExclusive, @Nullable Boolean toKey, boolean toExclusive,
            boolean reversed) {

        final NestedIterator<BooleanTreeMap<V>, T> it =
                newIteratorByRange(iterator, fromKey, fromExclusive, toKey, toExclusive, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    public static <V> NestedIterator<BooleanTreeMap<V>, V> newIterator(boolean reversed) {
        return reversed ? BooleanDescendingSimpleIterator.create() : BooleanAscendingSimpleIterator.create(false);
    }

    @Nonnull
    public static <V, T> NestedIterator<BooleanTreeMap<V>, T> newIterator(
            @Nonnull NestedIterator<V, T> iterator, boolean reversed) {

        return reversed
                ? BooleanDescendingChainedIterator.create(iterator)
                : BooleanAscendingChainedIterator.create(iterator, false);
    }

    @Nonnull
    public static <V> NestedIterator<BooleanTreeMap<V>, V> newIteratorByKey(@Nullable Boolean key) {
        return SingletonSimpleIterator.create(key);
    }

    @Nonnull
    public static <V, T> NestedIterator<BooleanTreeMap<V>, T> newIteratorByKey(
            @Nonnull NestedIterator<V, T> iterator, @Nullable Boolean key) {
        return SingletonChainedIterator.create(iterator, key);
    }

    @Nonnull
    public static <V> NestedIterator<BooleanTreeMap<V>, V> newIteratorByKeys(@Nonnull Boolean[] keys) {
        return ArraySimpleIterator.create(keys);
    }

    @Nonnull
    public static <V, T> NestedIterator<BooleanTreeMap<V>, T> newIteratorByKeys(
            @Nonnull NestedIterator<V, T> iterator, @Nonnull Boolean[] keys) {

        return ArrayChainedIterator.create(iterator, keys);
    }

    @Nonnull
    public static <V> NestedIterator<BooleanTreeMap<V>, V> newIteratorByRange(
            @Nullable Boolean fromKey, boolean fromExclusive, @Nullable Boolean toKey, boolean toExclusive,
            boolean reversed) {

        if (reversed) {
            if (toKey == null) {
                if (fromKey == null) {
                    return BooleanDescendingSimpleIterator.create();
                } else if (fromExclusive) {
                    return ToKeyExclusiveBooleanDescendingSimpleIterator.create(fromKey);
                } else {
                    return ToKeyInclusiveBooleanDescendingSimpleIterator.create(fromKey);
                }
            } else {
                if (fromKey == null) {
                    return BooleanDescendingSimpleIterator.create(toExclusive, toKey);
                } else if (fromExclusive) {
                    return ToKeyExclusiveBooleanDescendingSimpleIterator.create(toExclusive, toKey, fromKey);
                } else {
                    return ToKeyInclusiveBooleanDescendingSimpleIterator.create(toExclusive, toKey, fromKey);
                }
            }
        } else {
            if (fromKey == null) {
                if (toKey == null) {
                    return BooleanAscendingSimpleIterator.create(false);
                } else if (toExclusive) {
                    return ToKeyExclusiveBooleanAscendingSimpleIterator.create(false, toKey);
                } else {
                    return ToKeyInclusiveBooleanAscendingSimpleIterator.create(false, toKey);
                }
            } else {
                if (toKey == null) {
                    return BooleanAscendingSimpleIterator.create(fromExclusive, fromKey);
                } else if (toExclusive) {
                    return ToKeyExclusiveBooleanAscendingSimpleIterator.create(fromExclusive, fromKey, toKey);
                } else {
                    return ToKeyInclusiveBooleanAscendingSimpleIterator.create(fromExclusive, fromKey, toKey);
                }
            }
        }
    }

    @Nonnull
    public static <V, T> NestedIterator<BooleanTreeMap<V>, T> newIteratorByRange(
            @Nonnull NestedIterator<V, T> iterator, @Nullable Boolean fromKey, boolean fromExclusive,
            @Nullable Boolean toKey, boolean toExclusive, boolean reversed) {

        if (reversed) {
            if (toKey == null) {
                if (fromKey == null) {
                    return BooleanDescendingChainedIterator.create(iterator);
                } else if (fromExclusive) {
                    return ToKeyExclusiveBooleanDescendingChainedIterator.create(iterator, fromKey);
                } else {
                    return ToKeyInclusiveBooleanDescendingChainedIterator.create(iterator, fromKey);
                }
            } else {
                if (fromKey == null) {
                    return BooleanDescendingChainedIterator.create(iterator, toExclusive, toKey);
                } else if (fromExclusive) {
                    return ToKeyExclusiveBooleanDescendingChainedIterator.create(iterator, toExclusive, toKey, fromKey);
                } else {
                    return ToKeyInclusiveBooleanDescendingChainedIterator.create(iterator, toExclusive, toKey, fromKey);
                }
            }
        } else {
            if (fromKey == null) {
                if (toKey == null) {
                    return BooleanAscendingChainedIterator.create(iterator, false);
                } else if (toExclusive) {
                    return ToKeyExclusiveBooleanAscendingChainedIterator.create(iterator, false, toKey);
                } else {
                    return ToKeyInclusiveBooleanAscendingChainedIterator.create(iterator, false, toKey);
                }
            } else {
                if (toKey == null) {
                    return BooleanAscendingChainedIterator.create(iterator, fromExclusive, fromKey);
                } else if (toExclusive) {
                    return ToKeyExclusiveBooleanAscendingChainedIterator
                            .create(iterator, fromExclusive, fromKey, toKey);
                } else {
                    return ToKeyInclusiveBooleanAscendingChainedIterator
                            .create(iterator, fromExclusive, fromKey, toKey);
                }
            }
        }
    }
}
