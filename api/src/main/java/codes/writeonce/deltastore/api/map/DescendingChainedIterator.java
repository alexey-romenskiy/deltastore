package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

abstract class DescendingChainedIterator<K, V, M extends AbstractTreeMap<K, V, M>, T>
        extends AbstractChainedIterator<K, V, M, T> {

    protected static final int STATE_INIT = 0;

    protected static final int STATE_LAST_INCLUSIVE_RESET = 1;

    private static final int STATE_LAST_INCLUSIVE = 2;

    private static final int STATE_LAST_INCLUSIVE_FETCHED = 3;

    protected static final int STATE_LAST_EXCLUSIVE = 4;

    protected static final int STATE_NULL_INCLUSIVE_RESET = 5;

    private static final int STATE_NULL_INCLUSIVE = 6;

    private static final int STATE_NULL_INCLUSIVE_FETCHED = 7;

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
            case STATE_LAST_INCLUSIVE_FETCHED:
                state = STATE_LAST_INCLUSIVE;
                break;
            case STATE_NULL_INCLUSIVE_FETCHED:
                state = STATE_NULL_INCLUSIVE;
                break;
            case STATE_INIT:
            case STATE_LAST_INCLUSIVE_RESET:
            case STATE_LAST_INCLUSIVE:
            case STATE_LAST_EXCLUSIVE:
            case STATE_NULL_INCLUSIVE_RESET:
            case STATE_NULL_INCLUSIVE:
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
            case STATE_INIT:
                return fetchFirst();
            case STATE_LAST_INCLUSIVE_RESET: {
                index = ceiling();
                if (index == 0) {
                    return tryFetchNull();
                }
                if (ended()) {
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
                if (index == 0) {
                    return tryFetchNull();
                }
                if (ended()) {
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
                    if (index == 0) {
                        return tryFetchNull();
                    }
                    if (ended()) {
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
            case STATE_LAST_EXCLUSIVE:
                index = higher();
                return fetchLoop();
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
                state = STATE_ENDED;
                return null;
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
                state = STATE_ENDED;
                return null;
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
                state = STATE_ENDED;
                return null;
        }
    }

    @Override
    public boolean hasNext() {

        switch (state) {
            default:
                throw new IllegalStateException();
            case STATE_ENDED:
                return false;
            case STATE_INIT:
                return testFirst();
            case STATE_LAST_INCLUSIVE_RESET: {
                index = ceiling();
                if (index == 0) {
                    return tryTestNull();
                }
                if (ended()) {
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
                if (index == 0) {
                    return tryTestNull();
                }
                if (ended()) {
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
                    if (index == 0) {
                        return tryTestNull();
                    }
                    if (ended()) {
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
            case STATE_LAST_EXCLUSIVE:
                index = higher();
                return testLoop();
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
                state = STATE_ENDED;
                return false;
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
                state = STATE_ENDED;
                return false;
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
                state = STATE_ENDED;
                return false;
        }
    }

    private T fetchFirst() {

        index = map.getLastEntry();
        return fetchLoop();
    }

    private T fetchNext() {

        index = map.predecessor(index);
        return fetchLoop();
    }

    private T fetchLoop() {

        while (true) {
            if (index == 0) {
                return tryFetchNull();
            }
            if (ended()) {
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
            index = map.predecessor(index);
        }
    }

    private T tryFetchNull() {

        if (!nullEnded() && map.nullKey != 0) {
            index = map.nullKey;
            resetIterator();
            final var next = iterator.get();
            if (next != null) {
                expectedModCount = map.modCount;
                state = STATE_NULL_INCLUSIVE_FETCHED;
                return next;
            }
        }
        state = STATE_ENDED;
        return null;
    }

    private boolean testFirst() {

        index = map.getLastEntry();
        return testLoop();
    }

    private boolean testNext() {

        index = map.predecessor(index);
        return testLoop();
    }

    private boolean testLoop() {

        while (true) {
            if (index == 0) {
                return tryTestNull();
            }
            if (ended()) {
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
            index = map.predecessor(index);
        }
    }

    private boolean tryTestNull() {

        if (!nullEnded() && map.nullKey != 0) {
            index = map.nullKey;
            resetIterator();
            final var next = iterator.hasNext();
            if (next) {
                expectedModCount = map.modCount;
                state = STATE_NULL_INCLUSIVE_FETCHED;
                return true;
            }
        }
        state = STATE_ENDED;
        return false;
    }
}
