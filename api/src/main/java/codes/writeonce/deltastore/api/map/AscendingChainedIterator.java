package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

abstract class AscendingChainedIterator<K, V, M extends AbstractTreeMap<K, V, M>, T>
        extends AbstractChainedIterator<K, V, M, T> {

    protected static final int STATE_NULL_INCLUSIVE_RESET = 0;

    /**
     * null entry should be next (but possibly not exist)
     */
    private static final int STATE_NULL_INCLUSIVE = 1;

    /**
     * null entry should be next and exist, index points to null entry, not consumed,
     * expectedModCount is on track
     */
    private static final int STATE_NULL_INCLUSIVE_FETCHED = 2;

    /**
     * null entry consumed, first non-null entry should be next (but possibly not exist)
     */
    protected static final int STATE_NULL_EXCLUSIVE = 3;

    protected static final int STATE_LAST_INCLUSIVE_RESET = 4;

    /**
     * lastKey points to next non-null entry (possibly non-existing)
     */
    private static final int STATE_LAST_INCLUSIVE = 5;

    /**
     * lastKey and index points to next non-null entry, not consumed, expectedModCount is on track
     */
    private static final int STATE_LAST_INCLUSIVE_FETCHED = 6;

    /**
     * lastKey points to previously consumed non-null entry
     */
    protected static final int STATE_LAST_EXCLUSIVE = 7;

    protected static final int STATE_ENDED = 8;

    @Override
    public void reset(@Nonnull M map) {

        this.map = map;
        resetKey();
    }

    @Override
    public void map(@Nonnull M map) {

        if (this.map == map) {
            return;
        }
        this.map = map;
        switch (state) {
            default:
                throw new IllegalStateException();
            case STATE_NULL_INCLUSIVE_FETCHED:
                state = STATE_NULL_INCLUSIVE;
                break;
            case STATE_LAST_INCLUSIVE_FETCHED:
                state = STATE_LAST_INCLUSIVE;
                break;
            case STATE_NULL_INCLUSIVE_RESET:
            case STATE_NULL_INCLUSIVE:
            case STATE_NULL_EXCLUSIVE:
            case STATE_LAST_INCLUSIVE_RESET:
            case STATE_LAST_INCLUSIVE:
            case STATE_LAST_EXCLUSIVE:
            case STATE_ENDED:
                break;
        }
    }

    @Nullable
    @Override
    public T get() {

        switch (state) {
            default:
                throw new IllegalStateException();
            case STATE_ENDED:
                return null;
            case STATE_NULL_INCLUSIVE_RESET:
                if (map.nullKey != 0) {
                    index = map.nullKey;
                    resetIterator();
                    final var next = iterator.get();
                    if (next != null) {
                        expectedModCount = map.modCount;
                        state = STATE_NULL_INCLUSIVE_FETCHED;
                        return next;
                    }
                }
                return fetchFirst();
            case STATE_NULL_INCLUSIVE:
                if (map.nullKey != 0) {
                    index = map.nullKey;
                    mapIterator();
                    final var next = iterator.get();
                    if (next != null) {
                        expectedModCount = map.modCount;
                        state = STATE_NULL_INCLUSIVE_FETCHED;
                        return next;
                    }
                }
                return fetchFirst();
            case STATE_NULL_INCLUSIVE_FETCHED:
                if (expectedModCount == map.modCount) {
                    final var next = iterator.get();
                    if (next != null) {
                        return next;
                    }
                } else if (map.nullKey != 0) {
                    index = map.nullKey;
                    mapIterator();
                    final var next = iterator.get();
                    if (next != null) {
                        expectedModCount = map.modCount;
                        return next;
                    }
                }
                return fetchFirst();
            case STATE_NULL_EXCLUSIVE:
                return fetchFirst();
            case STATE_LAST_INCLUSIVE_RESET: {
                index = ceiling();
                if (index == 0 || ended()) {
                    state = STATE_ENDED;
                    return null;
                }
                resetIterator();
                final var next = iterator.get();
                if (next != null) {
                    updateLastKey();
                    expectedModCount = map.modCount;
                    state = STATE_LAST_INCLUSIVE_FETCHED;
                    return next;
                }
                return fetchNext();
            }
            case STATE_LAST_INCLUSIVE: {
                index = ceiling();
                if (index == 0 || ended()) {
                    state = STATE_ENDED;
                    return null;
                }
                resetOrMapIterator();
                final var next = iterator.get();
                if (next != null) {
                    updateLastKey();
                    expectedModCount = map.modCount;
                    state = STATE_LAST_INCLUSIVE_FETCHED;
                    return next;
                }
                return fetchNext();
            }
            case STATE_LAST_INCLUSIVE_FETCHED:
                if (expectedModCount == map.modCount) {
                    final var next = iterator.get();
                    if (next != null) {
                        return next;
                    }
                } else {
                    index = ceiling();
                    if (index == 0 || ended()) {
                        state = STATE_ENDED;
                        return null;
                    }
                    resetOrMapIterator();
                    final var next = iterator.get();
                    if (next != null) {
                        updateLastKey();
                        expectedModCount = map.modCount;
                        return next;
                    }
                }
                return fetchNext();
            case STATE_LAST_EXCLUSIVE: {
                index = higher();
                return fetchLoop();
            }
        }
    }

    @Override
    public boolean hasNext() {

        switch (state) {
            default:
                throw new IllegalStateException();
            case STATE_ENDED:
                return false;
            case STATE_NULL_INCLUSIVE_RESET:
                if (map.nullKey != 0) {
                    index = map.nullKey;
                    resetIterator();
                    final var next = iterator.hasNext();
                    if (next) {
                        expectedModCount = map.modCount;
                        state = STATE_NULL_INCLUSIVE_FETCHED;
                        return true;
                    }
                }
                return testFirst();
            case STATE_NULL_INCLUSIVE:
                if (map.nullKey != 0) {
                    index = map.nullKey;
                    mapIterator();
                    final var next = iterator.hasNext();
                    if (next) {
                        expectedModCount = map.modCount;
                        state = STATE_NULL_INCLUSIVE_FETCHED;
                        return true;
                    }
                }
                return testFirst();
            case STATE_NULL_INCLUSIVE_FETCHED:
                if (expectedModCount == map.modCount) {
                    final var next = iterator.hasNext();
                    if (next) {
                        return true;
                    }
                } else if (map.nullKey != 0) {
                    index = map.nullKey;
                    mapIterator();
                    final var next = iterator.hasNext();
                    if (next) {
                        expectedModCount = map.modCount;
                        return true;
                    }
                }
                return testFirst();
            case STATE_NULL_EXCLUSIVE:
                return testFirst();
            case STATE_LAST_INCLUSIVE_RESET: {
                index = ceiling();
                if (index == 0 || ended()) {
                    state = STATE_ENDED;
                    return false;
                }
                resetIterator();
                final var next = iterator.hasNext();
                if (next) {
                    updateLastKey();
                    expectedModCount = map.modCount;
                    state = STATE_LAST_INCLUSIVE_FETCHED;
                    return true;
                }
                return testNext();
            }
            case STATE_LAST_INCLUSIVE: {
                index = ceiling();
                if (index == 0 || ended()) {
                    state = STATE_ENDED;
                    return false;
                }
                resetOrMapIterator();
                final var next = iterator.hasNext();
                if (next) {
                    updateLastKey();
                    expectedModCount = map.modCount;
                    state = STATE_LAST_INCLUSIVE_FETCHED;
                    return true;
                }
                return testNext();
            }
            case STATE_LAST_INCLUSIVE_FETCHED:
                if (expectedModCount == map.modCount) {
                    final var next = iterator.hasNext();
                    if (next) {
                        return true;
                    }
                } else {
                    index = ceiling();
                    if (index == 0 || ended()) {
                        state = STATE_ENDED;
                        return false;
                    }
                    resetOrMapIterator();
                    final var next = iterator.hasNext();
                    if (next) {
                        updateLastKey();
                        expectedModCount = map.modCount;
                        return true;
                    }
                }
                return testNext();
            case STATE_LAST_EXCLUSIVE: {
                index = higher();
                return testLoop();
            }
        }
    }

    @Nullable
    private T fetchFirst() {

        index = map.getFirstEntry();
        return fetchLoop();
    }

    @Nullable
    private T fetchNext() {

        index = map.successor(index);
        return fetchLoop();
    }

    @Nullable
    private T fetchLoop() {

        while (true) {
            if (index == 0 || ended()) {
                state = STATE_ENDED;
                return null;
            }
            resetIterator();
            final var next = iterator.get();
            if (next != null) {
                updateLastKey();
                expectedModCount = map.modCount;
                state = STATE_LAST_INCLUSIVE_FETCHED;
                return next;
            }
            index = map.successor(index);
        }
    }

    private boolean testFirst() {

        index = map.getFirstEntry();
        return testLoop();
    }

    private boolean testNext() {

        index = map.successor(index);
        return testLoop();
    }

    private boolean testLoop() {

        while (true) {
            if (index == 0 || ended()) {
                state = STATE_ENDED;
                return false;
            }
            resetIterator();
            final var next = iterator.hasNext();
            if (next) {
                updateLastKey();
                expectedModCount = map.modCount;
                state = STATE_LAST_INCLUSIVE_FETCHED;
                return true;
            }
            index = map.successor(index);
        }
    }
}
