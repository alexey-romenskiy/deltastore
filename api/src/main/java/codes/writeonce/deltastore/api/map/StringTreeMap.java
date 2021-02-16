package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.function.Function;

public final class StringTreeMap<V> extends AbstractTreeMap<String, V, StringTreeMap<V>> {

    @SuppressWarnings("rawtypes")
    private static final StringTreeMap EMPTY_MAP = new StringTreeMap<>(1);

    protected Object[] keys;

    @SuppressWarnings("unchecked")
    public static <V> StringTreeMap<V> empty() {
        return (StringTreeMap<V>) EMPTY_MAP;
    }

    public StringTreeMap() {
        this(1);
    }

    private StringTreeMap(int capacity) {
        super(capacity);
        keys = new Object[capacity];
    }

    private StringTreeMap(int root, int nullKey, int free, int end, int capacity, int size, int modCount,
            Object[] values, int[] flags, Object[] keys) {
        super(root, nullKey, free, end, capacity, size, modCount, values, flags);
        this.keys = keys;
    }

    @SuppressWarnings("MethodDoesntCallSuperMethod")
    @Override
    protected StringTreeMap<V> clone() {
        return new StringTreeMap<>(root, nullKey, free, end, capacity, size, modCount, values.clone(),
                flags.clone(), keys.clone());
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V get(@Nullable String key) {

        if (key == null) {
            if (nullKey == 0) {
                return null;
            } else {
                return (V) values[nullKey];
            }
        } else {
            int p = root;

            while (p != 0) {
                int cmp = key.compareTo((String) keys[p]);
                if (cmp < 0) {
                    p = left(p);
                } else if (cmp > 0) {
                    p = right(p);
                } else {
                    return (V) values[p];
                }
            }

            return null;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V remove(@Nullable String key) {

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
            int p = root;

            while (p != 0) {
                int cmp = key.compareTo((String) keys[p]);
                if (cmp < 0) {
                    p = flags[p * 3];
                } else if (cmp > 0) {
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

                    keys[p] = null;
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
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V put(@Nullable String key, @Nullable V value) {

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

                int cmp;
                int parent;

                do {
                    parent = t;
                    cmp = key.compareTo((String) keys[t]);
                    if (cmp < 0) {
                        t = left(t);
                    } else if (cmp > 0) {
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
                if (cmp < 0) {
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
    }

    @Override
    protected void realloc(int capacity) {
        keys = Arrays.copyOf(keys, capacity);
    }

    @Override
    protected void copyKey(int from, int to) {
        keys[to] = keys[from];
        keys[from] = null;
    }

    @Override
    public boolean containsKey(@Nullable String key) {

        if (key == null) {
            return nullKey != 0;
        } else {
            int p = root;

            while (p != 0) {
                int cmp = key.compareTo((String) keys[p]);
                if (cmp < 0) {
                    p = left(p);
                } else if (cmp > 0) {
                    p = right(p);
                } else {
                    return true;
                }
            }

            return false;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V getOrDefault(@Nullable String key, @Nullable V defaultValue) {

        if (key == null) {
            if (nullKey == 0) {
                return defaultValue;
            } else {
                return (V) values[nullKey];
            }
        } else {
            int p = root;

            while (p != 0) {
                int cmp = key.compareTo((String) keys[p]);
                if (cmp < 0) {
                    p = left(p);
                } else if (cmp > 0) {
                    p = right(p);
                } else {
                    return (V) values[p];
                }
            }

            return defaultValue;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    @Nullable
    public V computeIfAbsent(@Nullable String key, @Nonnull Function<? super String, ? extends V> mappingFunction) {

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
            int p = root;

            while (p != 0) {
                int cmp = key.compareTo((String) keys[p]);
                if (cmp < 0) {
                    p = left(p);
                } else if (cmp > 0) {
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
    }

    protected int getCeilingEntry(@Nonnull String key) {

        int p = root;
        while (p != 0) {
            final int cmp = key.compareTo((String) keys[p]);
            if (cmp < 0) {
                final var left = left(p);
                if (left != 0) {
                    p = left;
                } else {
                    return p;
                }
            } else if (cmp > 0) {
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

    protected int getFloorEntry(@Nonnull String key) {

        int p = root;
        while (p != 0) {
            final int cmp = key.compareTo((String) keys[p]);
            if (cmp > 0) {
                final var right = right(p);
                if (right != 0) {
                    p = right;
                } else {
                    return p;
                }
            } else if (cmp < 0) {
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

    protected int getHigherEntry(@Nonnull String key) {

        int p = root;
        while (p != 0) {
            final int cmp = key.compareTo((String) keys[p]);
            if (cmp < 0) {
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

    protected int getLowerEntry(@Nonnull String key) {

        int p = root;
        while (p != 0) {
            final int cmp = key.compareTo((String) keys[p]);
            if (cmp > 0) {
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
    public NestedIterator<StringTreeMap<V>, V> iterator(boolean reversed) {

        final NestedIterator<StringTreeMap<V>, V> it = newIterator(reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<StringTreeMap<V>, T> iterator(@Nonnull NestedIterator<V, T> iterator,
            boolean reversed) {

        final NestedIterator<StringTreeMap<V>, T> it = newIterator(iterator, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<StringTreeMap<V>, V> iteratorByKey(@Nullable String key) {

        final NestedIterator<StringTreeMap<V>, V> it = newIteratorByKey(key);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<StringTreeMap<V>, T> iteratorByKey(@Nonnull NestedIterator<V, T> iterator,
            @Nullable String key) {

        final NestedIterator<StringTreeMap<V>, T> it = newIteratorByKey(iterator, key);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<StringTreeMap<V>, V> iteratorByKeys(@Nonnull String[] keys) {

        final NestedIterator<StringTreeMap<V>, V> it = newIteratorByKeys(keys);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<StringTreeMap<V>, T> iteratorByKeys(@Nonnull NestedIterator<V, T> iterator,
            @Nonnull String[] keys) {

        final NestedIterator<StringTreeMap<V>, T> it = newIteratorByKeys(iterator, keys);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public NestedIterator<StringTreeMap<V>, V> iteratorByRange(@Nullable String fromKey, boolean fromExclusive,
            @Nullable String toKey, boolean toExclusive, boolean reversed) {

        final NestedIterator<StringTreeMap<V>, V> it =
                newIteratorByRange(fromKey, fromExclusive, toKey, toExclusive, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    @Override
    public <T> NestedIterator<StringTreeMap<V>, T> iteratorByRange(@Nonnull NestedIterator<V, T> iterator,
            @Nullable String fromKey, boolean fromExclusive, @Nullable String toKey, boolean toExclusive,
            boolean reversed) {

        final NestedIterator<StringTreeMap<V>, T> it =
                newIteratorByRange(iterator, fromKey, fromExclusive, toKey, toExclusive, reversed);
        it.reset(this);
        return it;
    }

    @Nonnull
    public static <V> NestedIterator<StringTreeMap<V>, V> newIterator(boolean reversed) {
        return reversed ? StringDescendingSimpleIterator.create() : StringAscendingSimpleIterator.create(false);
    }

    @Nonnull
    public static <V, T> NestedIterator<StringTreeMap<V>, T> newIterator(
            @Nonnull NestedIterator<V, T> iterator, boolean reversed) {

        return reversed
                ? StringDescendingChainedIterator.create(iterator)
                : StringAscendingChainedIterator.create(iterator, false);
    }

    @Nonnull
    public static <V> NestedIterator<StringTreeMap<V>, V> newIteratorByKey(@Nullable String key) {
        return SingletonSimpleIterator.create(key);
    }

    @Nonnull
    public static <V, T> NestedIterator<StringTreeMap<V>, T> newIteratorByKey(
            @Nonnull NestedIterator<V, T> iterator, @Nullable String key) {
        return SingletonChainedIterator.create(iterator, key);
    }

    @Nonnull
    public static <V> NestedIterator<StringTreeMap<V>, V> newIteratorByKeys(@Nonnull String[] keys) {
        return ArraySimpleIterator.create(keys);
    }

    @Nonnull
    public static <V, T> NestedIterator<StringTreeMap<V>, T> newIteratorByKeys(
            @Nonnull NestedIterator<V, T> iterator, @Nonnull String[] keys) {

        return ArrayChainedIterator.create(iterator, keys);
    }

    @Nonnull
    public static <V> NestedIterator<StringTreeMap<V>, V> newIteratorByRange(
            @Nullable String fromKey, boolean fromExclusive, @Nullable String toKey, boolean toExclusive,
            boolean reversed) {

        if (reversed) {
            if (toKey == null) {
                if (fromKey == null) {
                    return StringDescendingSimpleIterator.create();
                } else if (fromExclusive) {
                    return ToKeyExclusiveStringDescendingSimpleIterator.create(fromKey);
                } else {
                    return ToKeyInclusiveStringDescendingSimpleIterator.create(fromKey);
                }
            } else {
                if (fromKey == null) {
                    return StringDescendingSimpleIterator.create(toExclusive, toKey);
                } else if (fromExclusive) {
                    return ToKeyExclusiveStringDescendingSimpleIterator.create(toExclusive, toKey, fromKey);
                } else {
                    return ToKeyInclusiveStringDescendingSimpleIterator.create(toExclusive, toKey, fromKey);
                }
            }
        } else {
            if (fromKey == null) {
                if (toKey == null) {
                    return StringAscendingSimpleIterator.create(false);
                } else if (toExclusive) {
                    return ToKeyExclusiveStringAscendingSimpleIterator.create(false, toKey);
                } else {
                    return ToKeyInclusiveStringAscendingSimpleIterator.create(false, toKey);
                }
            } else {
                if (toKey == null) {
                    return StringAscendingSimpleIterator.create(fromExclusive, fromKey);
                } else if (toExclusive) {
                    return ToKeyExclusiveStringAscendingSimpleIterator.create(fromExclusive, fromKey, toKey);
                } else {
                    return ToKeyInclusiveStringAscendingSimpleIterator.create(fromExclusive, fromKey, toKey);
                }
            }
        }
    }

    @Nonnull
    public static <V, T> NestedIterator<StringTreeMap<V>, T> newIteratorByRange(
            @Nonnull NestedIterator<V, T> iterator, @Nullable String fromKey, boolean fromExclusive,
            @Nullable String toKey, boolean toExclusive, boolean reversed) {

        if (reversed) {
            if (toKey == null) {
                if (fromKey == null) {
                    return StringDescendingChainedIterator.create(iterator);
                } else if (fromExclusive) {
                    return ToKeyExclusiveStringDescendingChainedIterator.create(iterator, fromKey);
                } else {
                    return ToKeyInclusiveStringDescendingChainedIterator.create(iterator, fromKey);
                }
            } else {
                if (fromKey == null) {
                    return StringDescendingChainedIterator.create(iterator, toExclusive, toKey);
                } else if (fromExclusive) {
                    return ToKeyExclusiveStringDescendingChainedIterator.create(iterator, toExclusive, toKey, fromKey);
                } else {
                    return ToKeyInclusiveStringDescendingChainedIterator.create(iterator, toExclusive, toKey, fromKey);
                }
            }
        } else {
            if (fromKey == null) {
                if (toKey == null) {
                    return StringAscendingChainedIterator.create(iterator, false);
                } else if (toExclusive) {
                    return ToKeyExclusiveStringAscendingChainedIterator.create(iterator, false, toKey);
                } else {
                    return ToKeyInclusiveStringAscendingChainedIterator.create(iterator, false, toKey);
                }
            } else {
                if (toKey == null) {
                    return StringAscendingChainedIterator.create(iterator, fromExclusive, fromKey);
                } else if (toExclusive) {
                    return ToKeyExclusiveStringAscendingChainedIterator.create(iterator, fromExclusive, fromKey, toKey);
                } else {
                    return ToKeyInclusiveStringAscendingChainedIterator.create(iterator, fromExclusive, fromKey, toKey);
                }
            }
        }
    }
}
