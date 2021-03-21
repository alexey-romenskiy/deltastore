package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.function.Function;
import java.util.function.IntFunction;

public final class IntegerTreeMap<V> extends AbstractTreeMap<Integer, V, IntegerTreeMap<V>> {

    @SuppressWarnings("rawtypes")
    private static final IntegerTreeMap EMPTY_MAP = new IntegerTreeMap<>(1);

    protected int[] keys;

    @SuppressWarnings("unchecked")
    public static <V> IntegerTreeMap<V> empty() {
        return (IntegerTreeMap<V>) EMPTY_MAP;
    }

    public IntegerTreeMap() {
        this(1);
    }

    private IntegerTreeMap(int capacity) {
        super(capacity);
        keys = new int[capacity];
    }

    private IntegerTreeMap(int root, int nullKey, int capacity, int size, int modCount, Object[] values, int[] flags,
            int[] keys) {
        super(root, nullKey, capacity, size, modCount, values, flags);
        this.keys = keys;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    protected IntegerTreeMap<V> clone() {
        return new IntegerTreeMap<>(root, nullKey, capacity, size, modCount, values.clone(), flags.clone(),
                keys.clone());
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V get(@Nullable Integer key) {

        if (key == null) {
            if (nullKey == 0) {
                return null;
            } else {
                return (V) values[nullKey];
            }
        } else {
            return get((int) key);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V get(int key) {

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
    public V remove(@Nullable Integer key) {

        if (key == null) {
            final var n = nullKey;
            if (n == 0) {
                return null;
            } else {
                final var value = values[n];
                nullKey = 0;
                free(n);
                size--;
                modCount++;
                return (V) value;
            }
        } else {
            return remove((int) key);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V remove(int key) {

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
                free(p);
                size--;
                modCount++;
                return (V) value;
            }
        }

        return null;
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V put(@Nullable Integer key, @Nullable V value) {

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
            return put((int) key, value);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V put(int key, @Nullable V value) {

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

            int k;
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
    public boolean containsKey(@Nullable Integer key) {

        if (key == null) {
            return nullKey != 0;
        } else {
            return containsKey((int) key);
        }
    }

    public boolean containsKey(int key) {

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
    public V getOrDefault(@Nullable Integer key, @Nullable V defaultValue) {

        if (key == null) {
            if (nullKey == 0) {
                return defaultValue;
            } else {
                return (V) values[nullKey];
            }
        } else {
            return getOrDefault((int) key, defaultValue);
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V getOrDefault(int key, @Nullable V defaultValue) {

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
    public V computeIfAbsent(@Nullable Integer key, @Nonnull Function<? super Integer, ? extends V> mappingFunction) {

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
            return computeIfAbsent((int) key, k -> mappingFunction.apply(key));
        }
    }

    @SuppressWarnings("unchecked")
    @Nullable
    public V computeIfAbsent(int key, @Nonnull IntFunction<? extends V> mappingFunction) {

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

    protected int getCeilingEntry(int key) {

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

    protected int getFloorEntry(int key) {

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

    protected int getHigherEntry(int key) {

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

    protected int getLowerEntry(int key) {

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
    public NestedIterator<IntegerTreeMap<V>, V> iterator(boolean reversed) {

        final NestedIterator<IntegerTreeMap<V>, V> it = newIterator(reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<IntegerTreeMap<V>, T> iterator(@Nonnull NestedIterator<V, T> iterator,
            boolean reversed) {

        final NestedIterator<IntegerTreeMap<V>, T> it = newIterator(iterator, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<IntegerTreeMap<V>, V> iteratorByKey(@Nullable Integer key) {

        final NestedIterator<IntegerTreeMap<V>, V> it = newIteratorByKey(key);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<IntegerTreeMap<V>, T> iteratorByKey(@Nonnull NestedIterator<V, T> iterator,
            @Nullable Integer key) {

        final NestedIterator<IntegerTreeMap<V>, T> it = newIteratorByKey(iterator, key);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<IntegerTreeMap<V>, V> iteratorByKeys(@Nonnull Integer[] keys) {

        final NestedIterator<IntegerTreeMap<V>, V> it = newIteratorByKeys(keys);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<IntegerTreeMap<V>, T> iteratorByKeys(@Nonnull NestedIterator<V, T> iterator,
            @Nonnull Integer[] keys) {

        final NestedIterator<IntegerTreeMap<V>, T> it = newIteratorByKeys(iterator, keys);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<IntegerTreeMap<V>, V> iteratorByRange(@Nullable Integer fromKey, boolean fromExclusive,
            @Nullable Integer toKey, boolean toExclusive, boolean reversed) {

        final NestedIterator<IntegerTreeMap<V>, V> it =
                newIteratorByRange(fromKey, fromExclusive, toKey, toExclusive, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<IntegerTreeMap<V>, T> iteratorByRange(@Nonnull NestedIterator<V, T> iterator,
            @Nullable Integer fromKey, boolean fromExclusive, @Nullable Integer toKey, boolean toExclusive,
            boolean reversed) {

        final NestedIterator<IntegerTreeMap<V>, T> it =
                newIteratorByRange(iterator, fromKey, fromExclusive, toKey, toExclusive, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    public static <V> NestedIterator<IntegerTreeMap<V>, V> newIterator(boolean reversed) {
        return reversed ? IntegerDescendingSimpleIterator.create() : IntegerAscendingSimpleIterator.create(false);
    }

    @Nonnull
    public static <V, T> NestedIterator<IntegerTreeMap<V>, T> newIterator(
            @Nonnull NestedIterator<V, T> iterator, boolean reversed) {

        return reversed
                ? IntegerDescendingChainedIterator.create(iterator)
                : IntegerAscendingChainedIterator.create(iterator, false);
    }

    @Nonnull
    public static <V> NestedIterator<IntegerTreeMap<V>, V> newIteratorByKey(@Nullable Integer key) {
        return SingletonSimpleIterator.create(key);
    }

    @Nonnull
    public static <V, T> NestedIterator<IntegerTreeMap<V>, T> newIteratorByKey(
            @Nonnull NestedIterator<V, T> iterator, @Nullable Integer key) {
        return SingletonChainedIterator.create(iterator, key);
    }

    @Nonnull
    public static <V> NestedIterator<IntegerTreeMap<V>, V> newIteratorByKeys(@Nonnull Integer[] keys) {
        return ArraySimpleIterator.create(keys);
    }

    @Nonnull
    public static <V, T> NestedIterator<IntegerTreeMap<V>, T> newIteratorByKeys(
            @Nonnull NestedIterator<V, T> iterator, @Nonnull Integer[] keys) {

        return ArrayChainedIterator.create(iterator, keys);
    }

    @Nonnull
    public static <V> NestedIterator<IntegerTreeMap<V>, V> newIteratorByRange(
            @Nullable Integer fromKey, boolean fromExclusive, @Nullable Integer toKey, boolean toExclusive,
            boolean reversed) {

        if (reversed) {
            if (toKey == null) {
                if (fromKey == null) {
                    return IntegerDescendingSimpleIterator.create();
                } else if (fromExclusive) {
                    return ToKeyExclusiveIntegerDescendingSimpleIterator.create(fromKey);
                } else {
                    return ToKeyInclusiveIntegerDescendingSimpleIterator.create(fromKey);
                }
            } else {
                if (fromKey == null) {
                    return IntegerDescendingSimpleIterator.create(toExclusive, toKey);
                } else if (fromExclusive) {
                    return ToKeyExclusiveIntegerDescendingSimpleIterator.create(toExclusive, toKey, fromKey);
                } else {
                    return ToKeyInclusiveIntegerDescendingSimpleIterator.create(toExclusive, toKey, fromKey);
                }
            }
        } else {
            if (fromKey == null) {
                if (toKey == null) {
                    return IntegerAscendingSimpleIterator.create(false);
                } else if (toExclusive) {
                    return ToKeyExclusiveIntegerAscendingSimpleIterator.create(false, toKey);
                } else {
                    return ToKeyInclusiveIntegerAscendingSimpleIterator.create(false, toKey);
                }
            } else {
                if (toKey == null) {
                    return IntegerAscendingSimpleIterator.create(fromExclusive, fromKey);
                } else if (toExclusive) {
                    return ToKeyExclusiveIntegerAscendingSimpleIterator.create(fromExclusive, fromKey, toKey);
                } else {
                    return ToKeyInclusiveIntegerAscendingSimpleIterator.create(fromExclusive, fromKey, toKey);
                }
            }
        }
    }

    @Nonnull
    public static <V, T> NestedIterator<IntegerTreeMap<V>, T> newIteratorByRange(
            @Nonnull NestedIterator<V, T> iterator, @Nullable Integer fromKey, boolean fromExclusive,
            @Nullable Integer toKey, boolean toExclusive, boolean reversed) {

        if (reversed) {
            if (toKey == null) {
                if (fromKey == null) {
                    return IntegerDescendingChainedIterator.create(iterator);
                } else if (fromExclusive) {
                    return ToKeyExclusiveIntegerDescendingChainedIterator.create(iterator, fromKey);
                } else {
                    return ToKeyInclusiveIntegerDescendingChainedIterator.create(iterator, fromKey);
                }
            } else {
                if (fromKey == null) {
                    return IntegerDescendingChainedIterator.create(iterator, toExclusive, toKey);
                } else if (fromExclusive) {
                    return ToKeyExclusiveIntegerDescendingChainedIterator.create(iterator, toExclusive, toKey, fromKey);
                } else {
                    return ToKeyInclusiveIntegerDescendingChainedIterator.create(iterator, toExclusive, toKey, fromKey);
                }
            }
        } else {
            if (fromKey == null) {
                if (toKey == null) {
                    return IntegerAscendingChainedIterator.create(iterator, false);
                } else if (toExclusive) {
                    return ToKeyExclusiveIntegerAscendingChainedIterator.create(iterator, false, toKey);
                } else {
                    return ToKeyInclusiveIntegerAscendingChainedIterator.create(iterator, false, toKey);
                }
            } else {
                if (toKey == null) {
                    return IntegerAscendingChainedIterator.create(iterator, fromExclusive, fromKey);
                } else if (toExclusive) {
                    return ToKeyExclusiveIntegerAscendingChainedIterator
                            .create(iterator, fromExclusive, fromKey, toKey);
                } else {
                    return ToKeyInclusiveIntegerAscendingChainedIterator
                            .create(iterator, fromExclusive, fromKey, toKey);
                }
            }
        }
    }
}
