package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.Id;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.function.Function;
import java.util.function.LongFunction;

public final class IdTreeMap<V> extends AbstractTreeMap<Id<?>, V, IdTreeMap<V>> {

    @SuppressWarnings("rawtypes")
    private static final IdTreeMap EMPTY_MAP = new IdTreeMap<>(1);

    protected long[] keys;

    @SuppressWarnings("unchecked")
    public static <V> IdTreeMap<V> empty() {
        return (IdTreeMap<V>) EMPTY_MAP;
    }

    public IdTreeMap() {
        this(1);
    }

    public IdTreeMap(int capacity) {
        super(capacity);
        keys = new long[capacity];
    }

    private IdTreeMap(int root, int nullKey, int capacity, int size, int modCount, Object[] values, int[] flags,
            long[] keys) {
        super(root, nullKey, capacity, size, modCount, values, flags);
        this.keys = keys;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    protected IdTreeMap<V> clone() {
        return new IdTreeMap<>(root, nullKey, capacity, size, modCount, values.clone(), flags.clone(), keys.clone());
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V get(@Nullable Id<?> key) {

        if (key == null) {
            if (nullKey == 0) {
                return null;
            } else {
                return (V) values[nullKey];
            }
        } else {
            return get(key.value());
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
    public V remove(@Nullable Id<?> key) {

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
            return remove(key.value());
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
    public V put(@Nullable Id<?> key, @Nullable V value) {

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
            return put(key.value(), value);
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
    public boolean containsKey(@Nullable Id<?> key) {

        if (key == null) {
            return nullKey != 0;
        } else {
            return containsKey(key.value());
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
    public V getOrDefault(@Nullable Id<?> key, @Nullable V defaultValue) {

        if (key == null) {
            if (nullKey == 0) {
                return defaultValue;
            } else {
                return (V) values[nullKey];
            }
        } else {
            return getOrDefault(key.value(), defaultValue);
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
    public V computeIfAbsent(@Nullable Id<?> key, @Nonnull Function<? super Id<?>, ? extends V> mappingFunction) {

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
            return computeIfAbsent(key.value(), k -> mappingFunction.apply(key));
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
    public NestedIterator<IdTreeMap<V>, V> iterator(boolean reversed) {

        final NestedIterator<IdTreeMap<V>, V> it = newIterator(reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<IdTreeMap<V>, T> iterator(@Nonnull NestedIterator<V, T> iterator,
            boolean reversed) {

        final NestedIterator<IdTreeMap<V>, T> it = newIterator(iterator, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<IdTreeMap<V>, V> iteratorByKey(@Nullable Id<?> key) {

        final NestedIterator<IdTreeMap<V>, V> it = newIteratorByKey(key);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<IdTreeMap<V>, T> iteratorByKey(@Nonnull NestedIterator<V, T> iterator,
            @Nullable Id<?> key) {

        final NestedIterator<IdTreeMap<V>, T> it = newIteratorByKey(iterator, key);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<IdTreeMap<V>, V> iteratorByKeys(@Nonnull Id<?>[] keys) {

        final NestedIterator<IdTreeMap<V>, V> it = newIteratorByKeys(keys);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<IdTreeMap<V>, T> iteratorByKeys(@Nonnull NestedIterator<V, T> iterator,
            @Nonnull Id<?>[] keys) {

        final NestedIterator<IdTreeMap<V>, T> it = newIteratorByKeys(iterator, keys);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<IdTreeMap<V>, V> iteratorByRange(@Nullable Id<?> fromKey, boolean fromExclusive,
            @Nullable Id<?> toKey, boolean toExclusive, boolean reversed) {

        final NestedIterator<IdTreeMap<V>, V> it =
                newIteratorByRange(fromKey, fromExclusive, toKey, toExclusive, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<IdTreeMap<V>, T> iteratorByRange(@Nonnull NestedIterator<V, T> iterator,
            @Nullable Id<?> fromKey, boolean fromExclusive, @Nullable Id<?> toKey, boolean toExclusive,
            boolean reversed) {

        final NestedIterator<IdTreeMap<V>, T> it =
                newIteratorByRange(iterator, fromKey, fromExclusive, toKey, toExclusive, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    public static <V> NestedIterator<IdTreeMap<V>, V> newIterator(boolean reversed) {
        return reversed ? IdDescendingSimpleIterator.create() : IdAscendingSimpleIterator.create(false);
    }

    @Nonnull
    public static <V, T> NestedIterator<IdTreeMap<V>, T> newIterator(
            @Nonnull NestedIterator<V, T> iterator, boolean reversed) {

        return reversed
                ? IdDescendingChainedIterator.create(iterator)
                : IdAscendingChainedIterator.create(iterator, false);
    }

    @Nonnull
    public static <V> NestedIterator<IdTreeMap<V>, V> newIteratorByKey(@Nullable Id<?> key) {
        return SingletonSimpleIterator.create(key);
    }

    @Nonnull
    public static <V, T> NestedIterator<IdTreeMap<V>, T> newIteratorByKey(
            @Nonnull NestedIterator<V, T> iterator, @Nullable Id<?> key) {
        return SingletonChainedIterator.create(iterator, key);
    }

    @Nonnull
    public static <V> NestedIterator<IdTreeMap<V>, V> newIteratorByKeys(@Nonnull Id<?>[] keys) {
        return ArraySimpleIterator.create(keys);
    }

    @Nonnull
    public static <V, T> NestedIterator<IdTreeMap<V>, T> newIteratorByKeys(
            @Nonnull NestedIterator<V, T> iterator, @Nonnull Id<?>[] keys) {

        return ArrayChainedIterator.create(iterator, keys);
    }

    @Nonnull
    public static <V> NestedIterator<IdTreeMap<V>, V> newIteratorByRange(
            @Nullable Id<?> fromKey, boolean fromExclusive, @Nullable Id<?> toKey, boolean toExclusive,
            boolean reversed) {

        if (reversed) {
            if (toKey == null) {
                if (fromKey == null) {
                    return IdDescendingSimpleIterator.create();
                } else if (fromExclusive) {
                    return ToKeyExclusiveIdDescendingSimpleIterator.create(fromKey.value());
                } else {
                    return ToKeyInclusiveIdDescendingSimpleIterator.create(fromKey.value());
                }
            } else {
                if (fromKey == null) {
                    return IdDescendingSimpleIterator.create(toExclusive, toKey.value());
                } else if (fromExclusive) {
                    return ToKeyExclusiveIdDescendingSimpleIterator.create(toExclusive, toKey.value(), fromKey.value());
                } else {
                    return ToKeyInclusiveIdDescendingSimpleIterator.create(toExclusive, toKey.value(), fromKey.value());
                }
            }
        } else {
            if (fromKey == null) {
                if (toKey == null) {
                    return IdAscendingSimpleIterator.create(false);
                } else if (toExclusive) {
                    return ToKeyExclusiveIdAscendingSimpleIterator.create(false, toKey.value());
                } else {
                    return ToKeyInclusiveIdAscendingSimpleIterator.create(false, toKey.value());
                }
            } else {
                if (toKey == null) {
                    return IdAscendingSimpleIterator.create(fromExclusive, fromKey.value());
                } else if (toExclusive) {
                    return ToKeyExclusiveIdAscendingSimpleIterator
                            .create(fromExclusive, fromKey.value(), toKey.value());
                } else {
                    return ToKeyInclusiveIdAscendingSimpleIterator
                            .create(fromExclusive, fromKey.value(), toKey.value());
                }
            }
        }
    }

    @Nonnull
    public static <V, T> NestedIterator<IdTreeMap<V>, T> newIteratorByRange(
            @Nonnull NestedIterator<V, T> iterator, @Nullable Id<?> fromKey, boolean fromExclusive,
            @Nullable Id<?> toKey, boolean toExclusive, boolean reversed) {

        if (reversed) {
            if (toKey == null) {
                if (fromKey == null) {
                    return IdDescendingChainedIterator.create(iterator);
                } else if (fromExclusive) {
                    return ToKeyExclusiveIdDescendingChainedIterator.create(iterator, fromKey.value());
                } else {
                    return ToKeyInclusiveIdDescendingChainedIterator.create(iterator, fromKey.value());
                }
            } else {
                if (fromKey == null) {
                    return IdDescendingChainedIterator.create(iterator, toExclusive, toKey.value());
                } else if (fromExclusive) {
                    return ToKeyExclusiveIdDescendingChainedIterator.create(iterator, toExclusive, toKey.value(),
                            fromKey.value());
                } else {
                    return ToKeyInclusiveIdDescendingChainedIterator.create(iterator, toExclusive, toKey.value(),
                            fromKey.value());
                }
            }
        } else {
            if (fromKey == null) {
                if (toKey == null) {
                    return IdAscendingChainedIterator.create(iterator, false);
                } else if (toExclusive) {
                    return ToKeyExclusiveIdAscendingChainedIterator.create(iterator, false, toKey.value());
                } else {
                    return ToKeyInclusiveIdAscendingChainedIterator.create(iterator, false, toKey.value());
                }
            } else {
                if (toKey == null) {
                    return IdAscendingChainedIterator.create(iterator, fromExclusive, fromKey.value());
                } else if (toExclusive) {
                    return ToKeyExclusiveIdAscendingChainedIterator.create(iterator, fromExclusive, fromKey.value(),
                            toKey.value());
                } else {
                    return ToKeyInclusiveIdAscendingChainedIterator.create(iterator, fromExclusive, fromKey.value(),
                            toKey.value());
                }
            }
        }
    }
}
