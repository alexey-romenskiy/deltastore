package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Arrays;
import java.util.function.Function;

public abstract class AbstractTreeMap<K, V, M extends AbstractTreeMap<K, V, M>> implements Cloneable {

    private static final int GROWTH_FACTOR_NUMERATOR =
            Integer.getInteger("codes.writeonce.deltastore.api.map.AbstractTreeMap.GROWTH_FACTOR_NUMERATOR", 21);

    private static final int GROWTH_FACTOR_DENOMINATOR =
            Integer.getInteger("codes.writeonce.deltastore.api.map.AbstractTreeMap.GROWTH_FACTOR_DENOMINATOR", 20);

    private static final int SHRINK_FACTOR_NUMERATOR =
            Integer.getInteger("codes.writeonce.deltastore.api.map.AbstractTreeMap.SHRINK_FACTOR_NUMERATOR", 32);

    private static final int SHRINK_FACTOR_DENOMINATOR =
            Integer.getInteger("codes.writeonce.deltastore.api.map.AbstractTreeMap.SHRINK_FACTOR_DENOMINATOR", 30);

    static {
        if (GROWTH_FACTOR_NUMERATOR < GROWTH_FACTOR_DENOMINATOR) {
            throw new RuntimeException();
        }
        if (SHRINK_FACTOR_NUMERATOR < SHRINK_FACTOR_DENOMINATOR) {
            throw new RuntimeException();
        }
        if (GROWTH_FACTOR_NUMERATOR * SHRINK_FACTOR_DENOMINATOR > SHRINK_FACTOR_NUMERATOR * GROWTH_FACTOR_DENOMINATOR) {
            throw new RuntimeException();
        }
    }

    protected static final int RED = 0x80000000;

    protected int root;

    protected int nullKey;

    protected int capacity;

    protected int size = 0;

    protected int modCount = 0;

    protected Object[] values;

    protected int[] flags; // left, right, parent|red

    public AbstractTreeMap(int capacity) {
        this.capacity = capacity;
        values = new Object[capacity];
        flags = new int[capacity * 3];
    }

    protected AbstractTreeMap(int root, int nullKey, int capacity, int size, int modCount, Object[] values,
            int[] flags) {
        this.root = root;
        this.nullKey = nullKey;
        this.capacity = capacity;
        this.size = size;
        this.modCount = modCount;
        this.values = values;
        this.flags = flags;
    }

    /**
     * From CLR
     */
    protected void fixAfterDeletion(int x) {

        while (x != root && !redOf(x)) {
            if (x == leftOf(parentOf(x))) {
                int sib = rightOf(parentOf(x));

                if (redOf(sib)) {
                    setBlack(sib);
                    setRed(parentOf(x));
                    rotateLeft(parentOf(x));
                    sib = rightOf(parentOf(x));
                }

                if (!redOf(leftOf(sib)) && !redOf(rightOf(sib))) {
                    setRed(sib);
                    x = parentOf(x);
                } else {
                    if (!redOf(rightOf(sib))) {
                        setBlack(leftOf(sib));
                        setRed(sib);
                        rotateRight(sib);
                        sib = rightOf(parentOf(x));
                    }
                    if (redOf(parentOf(x))) {
                        setRed(sib);
                    } else {
                        setBlack(sib);
                    }
                    setBlack(parentOf(x));
                    setBlack(rightOf(sib));
                    rotateLeft(parentOf(x));
                    x = root;
                }
            } else { // symmetric
                int sib = leftOf(parentOf(x));

                if (redOf(sib)) {
                    setBlack(sib);
                    setRed(parentOf(x));
                    rotateRight(parentOf(x));
                    sib = leftOf(parentOf(x));
                }

                if (!redOf(rightOf(sib)) && !redOf(leftOf(sib))) {
                    setRed(sib);
                    x = parentOf(x);
                } else {
                    if (!redOf(leftOf(sib))) {
                        setBlack(rightOf(sib));
                        setRed(sib);
                        rotateLeft(sib);
                        sib = leftOf(parentOf(x));
                    }
                    if (redOf(parentOf(x))) {
                        setRed(sib);
                    } else {
                        setBlack(sib);
                    }
                    setBlack(parentOf(x));
                    setBlack(leftOf(sib));
                    rotateRight(parentOf(x));
                    x = root;
                }
            }
        }

        setBlack(x);
    }

    @SuppressWarnings("SuspiciousNameCombination")
    protected void fixAfterInsertion(int x) {

        flags[x * 3 + 2] |= RED;

        while (x != 0 && x != root && red(parent(x))) {
            if (parentOf(x) == leftOf(parentOf(parentOf(x)))) {
                final int y = rightOf(parentOf(parentOf(x)));
                if (redOf(y)) {
                    setBlack(parentOf(x));
                    setBlack(y);
                    setRed(parentOf(parentOf(x)));
                    x = parentOf(parentOf(x));
                } else {
                    if (x == rightOf(parentOf(x))) {
                        x = parentOf(x);
                        rotateLeft(x);
                    }
                    setBlack(parentOf(x));
                    setRed(parentOf(parentOf(x)));
                    rotateRight(parentOf(parentOf(x)));
                }
            } else {
                final int y = leftOf(parentOf(parentOf(x)));
                if (redOf(y)) {
                    setBlack(parentOf(x));
                    setBlack(y);
                    setRed(parentOf(parentOf(x)));
                    x = parentOf(parentOf(x));
                } else {
                    if (x == leftOf(parentOf(x))) {
                        x = parentOf(x);
                        rotateRight(x);
                    }
                    setBlack(parentOf(x));
                    setRed(parentOf(parentOf(x)));
                    rotateLeft(parentOf(parentOf(x)));
                }
            }
        }

        flags[root * 3 + 2] &= ~RED;
    }

    protected int successor(int t) {

        if (t == 0) {
            return 0;
        } else {
            var p = right(t);
            if (p != 0) {
                while (true) {
                    final var left = left(p);
                    if (left == 0) {
                        break;
                    }
                    p = left;
                }
                return p;
            } else {
                p = parent(t);
                int ch = t;
                while (p != 0 && ch == right(p)) {
                    ch = p;
                    p = parent(p);
                }
                return p;
            }
        }
    }

    protected int predecessor(int t) {

        if (t == 0) {
            return 0;
        } else {
            var p = left(t);
            if (p != 0) {
                while (true) {
                    final var right = right(p);
                    if (right == 0) {
                        break;
                    }
                    p = right;
                }
                return p;
            } else {
                p = parent(t);
                int ch = t;
                while (p != 0 && ch == left(p)) {
                    ch = p;
                    p = parent(p);
                }
                return p;
            }
        }
    }

    /**
     * From CLR
     */
    private void rotateLeft(int p) {

        if (p != 0) {
            final var iRight = p * 3 + 1;
            final var right = flags[iRight];
            final var rightLeft = left(right);
            flags[iRight] = rightLeft;
            if (rightLeft != 0) {
                final var iRightLeftParent = rightLeft * 3 + 2;
                flags[iRightLeftParent] = flags[iRightLeftParent] & RED | p;
            }
            final var iRightParent = right * 3 + 2;
            final var iParent = p * 3 + 2;
            final var parent = flags[iParent] & ~RED;
            flags[iRightParent] = flags[iRightParent] & RED | parent;
            if (parent == 0) {
                root = right;
            } else {
                final var iParentLeft = parent * 3;
                if (flags[iParentLeft] == p) {
                    flags[iParentLeft] = right;
                } else {
                    flags[parent * 3 + 1] = right;
                }
            }
            flags[right * 3] = p;
            flags[iParent] = flags[iParent] & RED | right;
        }
    }

    /**
     * From CLR
     */
    private void rotateRight(int p) {

        if (p != 0) {
            final var iLeft = p * 3;
            final var left = flags[iLeft];
            final var leftRight = right(left);
            flags[iLeft] = leftRight;
            if (leftRight != 0) {
                final var iLeftRightParent = leftRight * 3 + 2;
                flags[iLeftRightParent] = flags[iLeftRightParent] & RED | p;
            }
            final var iLeftParent = left * 3 + 2;
            final var iParent = p * 3 + 2;
            final var parent = flags[iParent] & ~RED;
            flags[iLeftParent] = flags[iLeftParent] & RED | parent;
            if (parent == 0) {
                root = left;
            } else {
                final var iParentRight = parent * 3 + 1;
                if (flags[iParentRight] == p) {
                    flags[iParentRight] = left;
                } else {
                    flags[parent * 3] = left;
                }
            }
            flags[left * 3 + 1] = p;
            flags[iParent] = flags[iParent] & RED | left;
        }
    }

    private boolean redOf(int p) {
        return p != 0 && flags[p * 3 + 2] < 0;
    }

    protected boolean red(int x) {
        return flags[x * 3 + 2] < 0;
    }

    private void setRed(int x) {
        if (x != 0) {
            flags[x * 3 + 2] |= RED;
        }
    }

    private void setBlack(int x) {
        if (x != 0) {
            flags[x * 3 + 2] &= ~RED;
        }
    }

    protected int parent(int x) {
        return flags[x * 3 + 2] & ~RED;
    }

    private int parentOf(int x) {
        return x == 0 ? 0 : flags[x * 3 + 2] & ~RED;
    }

    protected int left(int p) {
        return flags[p * 3];
    }

    private int leftOf(int p) {
        return p == 0 ? 0 : flags[p * 3];
    }

    protected int right(int p) {
        return flags[p * 3 + 1];
    }

    private int rightOf(int p) {
        return p == 0 ? 0 : flags[p * 3 + 1];
    }

    protected int allocate() {

        final var c = size + 1;
        if (c == capacity) {
            grow();
        }
        return c;
    }

    protected void free(int n) {

        final var last = size;

        if (n == last) {
            values[n] = null;
            flags[n * 3] = 0;
            flags[n * 3 + 1] = 0;
            flags[n * 3 + 2] = 0;
        } else {
            modCount++;
            final var parent = flags[last * 3 + 2];
            if ((parent & ~RED) == 0) {
                if (root == last) {
                    root = n;
                    patch(n, last, parent);
                } else if (nullKey == last) {
                    nullKey = n;
                    values[n] = values[last];
                    values[last] = null;
                    flags[n * 3] = 0;
                    flags[n * 3 + 1] = 0;
                    flags[n * 3 + 2] = 0;
                }
            } else {
                flags[last * 3 + 2] = 0;
                patchParent(n, last, parent);
                patch(n, last, parent);
            }
        }

        if (compact(last)) {
            final var shrinkedCapacity = shrinkedCapacity(last);
            capacity = shrinkedCapacity;
            flags = Arrays.copyOf(flags, shrinkedCapacity * 3);
            values = Arrays.copyOf(values, shrinkedCapacity);
            realloc(shrinkedCapacity);
        }
    }

    private void patch(int f, int i, int parent) {

        final var left = flags[i * 3];
        if (left != 0) {
            flags[i * 3] = 0;
            final var x = left * 3 + 2;
            flags[x] = flags[x] & RED | f;
        }

        final var right = flags[i * 3 + 1];
        if (right != 0) {
            flags[i * 3 + 1] = 0;
            final var x = right * 3 + 2;
            flags[x] = flags[x] & RED | f;
        }

        flags[f * 3] = left;
        flags[f * 3 + 1] = right;
        flags[f * 3 + 2] = parent;

        values[f] = values[i];
        values[i] = null;

        copyKey(i, f);
    }

    private void patchParent(int f, int i, int parent) {

        final var p = (parent & ~RED) * 3;
        if (flags[p] == i) {
            flags[p] = f;
        } else {
            flags[p + 1] = f;
        }
    }

    private void grow() {

        final int capacity = addCapacity();
        flags = Arrays.copyOf(flags, capacity * 3);
        values = Arrays.copyOf(values, capacity);
        realloc(capacity);
        this.capacity = capacity;
    }

    protected int addCapacity() {
        return (this.capacity + 1) * GROWTH_FACTOR_NUMERATOR / GROWTH_FACTOR_DENOMINATOR;
    }

    protected int shrinkedCapacity(int used) {
        return used * GROWTH_FACTOR_NUMERATOR / GROWTH_FACTOR_DENOMINATOR;
    }

    protected boolean compact(int used) {
        return capacity > used * SHRINK_FACTOR_NUMERATOR / SHRINK_FACTOR_DENOMINATOR;
    }

    public int size() {
        return size;
    }

    public boolean isEmpty() {
        return size == 0;
    }

    protected int getFirstEntry() {

        int p = root;
        if (p != 0) {
            while (true) {
                final var left = left(p);
                if (left == 0) {
                    break;
                }
                p = left;
            }
        }
        return p;
    }

    protected int getLastEntry() {

        int p = root;
        if (p != 0) {
            while (true) {
                final var right = right(p);
                if (right == 0) {
                    break;
                }
                p = right;
            }
        }
        return p;
    }

    protected abstract void realloc(int capacity);

    protected abstract void copyKey(int from, int to);

    public abstract boolean containsKey(@Nullable K key);

    @Nullable
    public abstract V get(@Nullable K key);

    @Nullable
    public abstract V getOrDefault(@Nullable K key, @Nullable V defaultValue);

    @Nullable
    public abstract V remove(@Nullable K key);

    @Nullable
    public abstract V put(@Nullable K key, @Nullable V value);

    @Nullable
    public abstract V computeIfAbsent(@Nullable K key, @Nonnull Function<? super K, ? extends V> mappingFunction);

    @Nonnull
    public abstract NestedIterator<M, V> iterator(boolean reversed);

    @Nonnull
    public abstract <T> NestedIterator<M, T> iterator(@Nonnull NestedIterator<V, T> iterator,
            boolean reversed);

    @Nonnull
    public abstract NestedIterator<M, V> iteratorByKey(@Nullable K key);

    @Nonnull
    public abstract <T> NestedIterator<M, T> iteratorByKey(@Nonnull NestedIterator<V, T> iterator,
            @Nullable K key);

    @Nonnull
    public abstract NestedIterator<M, V> iteratorByKeys(@Nonnull K[] keys);

    @Nonnull
    public abstract <T> NestedIterator<M, T> iteratorByKeys(@Nonnull NestedIterator<V, T> iterator,
            @Nonnull K[] keys);

    @Nonnull
    public abstract NestedIterator<M, V> iteratorByRange(@Nullable K fromKey, boolean fromExclusive,
            @Nullable K toKey, boolean toExclusive, boolean reversed);

    @Nonnull
    public abstract <T> NestedIterator<M, T> iteratorByRange(@Nonnull NestedIterator<V, T> iterator,
            @Nullable K fromKey, boolean fromExclusive, @Nullable K toKey, boolean toExclusive, boolean reversed);
}
