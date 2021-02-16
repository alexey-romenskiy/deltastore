package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.SmartIterator;

import javax.annotation.Nonnull;
import java.util.Spliterator;
import java.util.function.Consumer;

import static java.util.Objects.requireNonNull;

public abstract class NestedIterator<M, T> implements SmartIterator<T> {

    protected static final int POOL_SIZE = 1024;

    private static final int BATCH_UNIT = 1 << 10;

    private static final int MAX_BATCH = 1 << 25;

    private int batch;

    public abstract void reset(@Nonnull M map);

    public abstract void map(@Nonnull M map);

    @Override
    public Spliterator<T> trySplit() {
        /*
         * Split into arrays of arithmetically increasing batch
         * sizes.  This will only improve parallel performance if
         * per-element Consumer actions are more costly than
         * transferring them into an array.  The use of an
         * arithmetic progression in split sizes provides overhead
         * vs parallelism bounds that do not particularly favor or
         * penalize cases of lightweight vs heavyweight element
         * operations, across combinations of #elements vs #cores,
         * whether or not either are known.  We generate
         * O(sqrt(#elements)) splits, allowing O(sqrt(#cores))
         * potential speedup.
         */
        var next = get();
        if (next != null) {
            int n = batch + BATCH_UNIT;
            if (n > MAX_BATCH) {
                n = MAX_BATCH;
            }
            Object[] a = new Object[n];
            int j = 0;
            do {
                a[j] = next;
            } while (++j < n && (next = get()) != null);
            batch = j;
            return new ArraySpliterator<>(a, 0, j);
        }
        return null;
    }

    @Override
    public boolean tryAdvance(Consumer<? super T> action) {

        requireNonNull(action);
        final var next = get();
        if (next != null) {
            action.accept(next);
            return true;
        }
        return false;
    }

    @Override
    public long estimateSize() {
        return Long.MAX_VALUE;
    }

    @Override
    public int characteristics() {
        return ORDERED | NONNULL;
    }

    /**
     * A Spliterator designed for use by sources that traverse and split
     * elements maintained in an unmodifiable {@code Object[]} array.
     */
    private static final class ArraySpliterator<T> implements Spliterator<T> {

        /**
         * The array, explicitly typed as Object[]. Unlike in some other
         * classes (see for example CR 6260652), we do not need to
         * screen arguments to ensure they are exactly of type Object[]
         * so long as no methods write into the array or serialize it,
         * which we ensure here by defining this class as final.
         */
        private final Object[] array;
        private int index;        // current index, modified on advance/split
        private final int fence;  // one past last index

        /**
         * Creates a spliterator covering the given array and range
         *
         * @param array  the array, assumed to be unmodified during use
         * @param origin the least index (inclusive) to cover
         * @param fence  one past the greatest index to cover
         *               of this spliterator's source or elements beyond {@code SIZED} and
         *               {@code SUBSIZED} which are always reported
         */
        public ArraySpliterator(Object[] array, int origin, int fence) {
            this.array = array;
            this.index = origin;
            this.fence = fence;
        }

        @Override
        public Spliterator<T> trySplit() {
            int lo = index, mid = (lo + fence) >>> 1;
            return (lo >= mid) ? null : new ArraySpliterator<>(array, lo, index = mid);
        }

        @SuppressWarnings("unchecked")
        @Override
        public void forEachRemaining(Consumer<? super T> action) {
            Object[] a;
            int i, hi; // hoist accesses and checks from loop
            if (action == null) {
                throw new NullPointerException();
            }
            if ((a = array).length >= (hi = fence) &&
                (i = index) >= 0 && i < (index = hi)) {
                do {
                    action.accept((T) a[i]);
                } while (++i < hi);
            }
        }

        @Override
        public boolean tryAdvance(Consumer<? super T> action) {
            if (action == null) {
                throw new NullPointerException();
            }
            if (index >= 0 && index < fence) {
                @SuppressWarnings("unchecked") T e = (T) array[index++];
                action.accept(e);
                return true;
            }
            return false;
        }

        @Override
        public long estimateSize() {
            return fence - index;
        }

        @Override
        public int characteristics() {
            return ORDERED | NONNULL | SIZED | SUBSIZED;
        }
    }

    @Override
    public void close() {
        batch = 0;
    }
}
