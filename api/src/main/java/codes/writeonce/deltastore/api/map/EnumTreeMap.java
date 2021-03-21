package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.function.Function;
import java.util.function.IntFunction;

public final class EnumTreeMap<V> extends AbstractTreeMap<Enum<?>, V, EnumTreeMap<V>> {

    @SuppressWarnings("rawtypes")
    private static final EnumTreeMap EMPTY_MAP = new EnumTreeMap<>(1);

    protected int[] keys;

    @SuppressWarnings("unchecked")
    public static <V> EnumTreeMap<V> empty() {
        return (EnumTreeMap<V>) EMPTY_MAP;
    }

    public EnumTreeMap() {
        this(1);
    }

    public EnumTreeMap(int capacity) {
        super(capacity);
        keys = new int[capacity];
    }

    private EnumTreeMap(int root, int nullKey, int capacity, int size, int modCount, Object[] values, int[] flags,
            int[] keys) {
        super(root, nullKey, capacity, size, modCount, values, flags);
        this.keys = keys;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    protected EnumTreeMap<V> clone() {
        return new EnumTreeMap<>(root, nullKey, capacity, size, modCount, values.clone(), flags.clone(), keys.clone());
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V get(@Nullable Enum<?> key) {

        if (key == null) {
            if (nullKey == 0) {
                return null;
            } else {
                return (V) values[nullKey];
            }
        } else {
            return get(key.ordinal());
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
    public V remove(@Nullable Enum<?> key) {

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
            return remove(key.ordinal());
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
    public V put(@Nullable Enum<?> key, @Nullable V value) {

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
            return put(key.ordinal(), value);
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
    protected int addCapacity() {
        return this.capacity + 1;
    }

    @Override
    protected int shrinkedCapacity(int used) {
        return used;
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
        keys[from] = 0;
    }

    @Override
    public boolean containsKey(@Nullable Enum<?> key) {

        if (key == null) {
            return nullKey != 0;
        } else {
            return containsKey(key.ordinal());
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
    public V getOrDefault(@Nullable Enum<?> key, @Nullable V defaultValue) {

        if (key == null) {
            if (nullKey == 0) {
                return defaultValue;
            } else {
                return (V) values[nullKey];
            }
        } else {
            return getOrDefault(key.ordinal(), defaultValue);
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
    public V computeIfAbsent(@Nullable Enum<?> key, @Nonnull Function<? super Enum<?>, ? extends V> mappingFunction) {

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
            return computeIfAbsent(key.ordinal(), k -> mappingFunction.apply(key));
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
    public NestedIterator<EnumTreeMap<V>, V> iterator(boolean reversed) {

        final NestedIterator<EnumTreeMap<V>, V> it = newIterator(reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<EnumTreeMap<V>, T> iterator(@Nonnull NestedIterator<V, T> iterator,
            boolean reversed) {

        final NestedIterator<EnumTreeMap<V>, T> it = newIterator(iterator, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<EnumTreeMap<V>, V> iteratorByKey(@Nullable Enum<?> key) {

        final NestedIterator<EnumTreeMap<V>, V> it = newIteratorByKey(key);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<EnumTreeMap<V>, T> iteratorByKey(@Nonnull NestedIterator<V, T> iterator,
            @Nullable Enum<?> key) {

        final NestedIterator<EnumTreeMap<V>, T> it = newIteratorByKey(iterator, key);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<EnumTreeMap<V>, V> iteratorByKeys(@Nonnull Enum<?>[] keys) {

        final NestedIterator<EnumTreeMap<V>, V> it = newIteratorByKeys(keys);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<EnumTreeMap<V>, T> iteratorByKeys(@Nonnull NestedIterator<V, T> iterator,
            @Nonnull Enum<?>[] keys) {

        final NestedIterator<EnumTreeMap<V>, T> it = newIteratorByKeys(iterator, keys);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<EnumTreeMap<V>, V> iteratorByRange(@Nullable Enum<?> fromKey, boolean fromExclusive,
            @Nullable Enum<?> toKey, boolean toExclusive, boolean reversed) {

        final NestedIterator<EnumTreeMap<V>, V> it =
                newIteratorByRange(fromKey, fromExclusive, toKey, toExclusive, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<EnumTreeMap<V>, T> iteratorByRange(@Nonnull NestedIterator<V, T> iterator,
            @Nullable Enum<?> fromKey, boolean fromExclusive, @Nullable Enum<?> toKey, boolean toExclusive,
            boolean reversed) {

        final NestedIterator<EnumTreeMap<V>, T> it =
                newIteratorByRange(iterator, fromKey, fromExclusive, toKey, toExclusive, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    public static <V> NestedIterator<EnumTreeMap<V>, V> newIterator(boolean reversed) {
        return reversed ? EnumDescendingSimpleIterator.create() : EnumAscendingSimpleIterator.create(false);
    }

    @Nonnull
    public static <V, T> NestedIterator<EnumTreeMap<V>, T> newIterator(
            @Nonnull NestedIterator<V, T> iterator, boolean reversed) {

        return reversed
                ? EnumDescendingChainedIterator.create(iterator)
                : EnumAscendingChainedIterator.create(iterator, false);
    }

    @Nonnull
    public static <V> NestedIterator<EnumTreeMap<V>, V> newIteratorByKey(@Nullable Enum<?> key) {
        return SingletonSimpleIterator.create(key);
    }

    @Nonnull
    public static <V, T> NestedIterator<EnumTreeMap<V>, T> newIteratorByKey(
            @Nonnull NestedIterator<V, T> iterator, @Nullable Enum<?> key) {
        return SingletonChainedIterator.create(iterator, key);
    }

    @Nonnull
    public static <V> NestedIterator<EnumTreeMap<V>, V> newIteratorByKeys(@Nonnull Enum<?>[] keys) {
        return ArraySimpleIterator.create(keys);
    }

    @Nonnull
    public static <V, T> NestedIterator<EnumTreeMap<V>, T> newIteratorByKeys(
            @Nonnull NestedIterator<V, T> iterator, @Nonnull Enum<?>[] keys) {

        return ArrayChainedIterator.create(iterator, keys);
    }

    @Nonnull
    public static <V> NestedIterator<EnumTreeMap<V>, V> newIteratorByRange(
            @Nullable Enum<?> fromKey, boolean fromExclusive, @Nullable Enum<?> toKey, boolean toExclusive,
            boolean reversed) {

        if (reversed) {
            if (toKey == null) {
                if (fromKey == null) {
                    return EnumDescendingSimpleIterator.create();
                } else if (fromExclusive) {
                    return ToKeyExclusiveEnumDescendingSimpleIterator.create(fromKey.ordinal());
                } else {
                    return ToKeyInclusiveEnumDescendingSimpleIterator.create(fromKey.ordinal());
                }
            } else {
                if (fromKey == null) {
                    return EnumDescendingSimpleIterator.create(toExclusive, toKey.ordinal());
                } else if (fromExclusive) {
                    return ToKeyExclusiveEnumDescendingSimpleIterator.create(toExclusive, toKey.ordinal(),
                            fromKey.ordinal());
                } else {
                    return ToKeyInclusiveEnumDescendingSimpleIterator.create(toExclusive, toKey.ordinal(),
                            fromKey.ordinal());
                }
            }
        } else {
            if (fromKey == null) {
                if (toKey == null) {
                    return EnumAscendingSimpleIterator.create(false);
                } else if (toExclusive) {
                    return ToKeyExclusiveEnumAscendingSimpleIterator.create(false, toKey.ordinal());
                } else {
                    return ToKeyInclusiveEnumAscendingSimpleIterator.create(false, toKey.ordinal());
                }
            } else {
                if (toKey == null) {
                    return EnumAscendingSimpleIterator.create(fromExclusive, fromKey.ordinal());
                } else if (toExclusive) {
                    return ToKeyExclusiveEnumAscendingSimpleIterator.create(fromExclusive, fromKey.ordinal(),
                            toKey.ordinal());
                } else {
                    return ToKeyInclusiveEnumAscendingSimpleIterator.create(fromExclusive, fromKey.ordinal(),
                            toKey.ordinal());
                }
            }
        }
    }

    @Nonnull
    public static <V, T> NestedIterator<EnumTreeMap<V>, T> newIteratorByRange(
            @Nonnull NestedIterator<V, T> iterator, @Nullable Enum<?> fromKey, boolean fromExclusive,
            @Nullable Enum<?> toKey, boolean toExclusive, boolean reversed) {

        if (reversed) {
            if (toKey == null) {
                if (fromKey == null) {
                    return EnumDescendingChainedIterator.create(iterator);
                } else if (fromExclusive) {
                    return ToKeyExclusiveEnumDescendingChainedIterator.create(iterator, fromKey.ordinal());
                } else {
                    return ToKeyInclusiveEnumDescendingChainedIterator.create(iterator, fromKey.ordinal());
                }
            } else {
                if (fromKey == null) {
                    return EnumDescendingChainedIterator.create(iterator, toExclusive, toKey.ordinal());
                } else if (fromExclusive) {
                    return ToKeyExclusiveEnumDescendingChainedIterator.create(iterator, toExclusive, toKey.ordinal(),
                            fromKey.ordinal());
                } else {
                    return ToKeyInclusiveEnumDescendingChainedIterator.create(iterator, toExclusive, toKey.ordinal(),
                            fromKey.ordinal());
                }
            }
        } else {
            if (fromKey == null) {
                if (toKey == null) {
                    return EnumAscendingChainedIterator.create(iterator, false);
                } else if (toExclusive) {
                    return ToKeyExclusiveEnumAscendingChainedIterator.create(iterator, false, toKey.ordinal());
                } else {
                    return ToKeyInclusiveEnumAscendingChainedIterator.create(iterator, false, toKey.ordinal());
                }
            } else {
                if (toKey == null) {
                    return EnumAscendingChainedIterator.create(iterator, fromExclusive, fromKey.ordinal());
                } else if (toExclusive) {
                    return ToKeyExclusiveEnumAscendingChainedIterator.create(iterator, fromExclusive, fromKey.ordinal(),
                            toKey.ordinal());
                } else {
                    return ToKeyInclusiveEnumAscendingChainedIterator.create(iterator, fromExclusive, fromKey.ordinal(),
                            toKey.ordinal());
                }
            }
        }
    }
}
