package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;

abstract class DescendingSimpleIterator<K, V, M extends AbstractTreeMap<K, V, M>>
        extends AbstractSimpleIterator<K, V, M> {

    protected static final int STATE_INIT = 0;

    protected static final int STATE_LAST_INCLUSIVE = 1;

    private static final int STATE_LAST_INCLUSIVE_FETCHED = 2;

    protected static final int STATE_LAST_EXCLUSIVE = 3;

    private static final int STATE_LAST_EXCLUSIVE_FETCHED = 4;

    protected static final int STATE_NULL_INCLUSIVE = 5;

    private static final int STATE_NULL_INCLUSIVE_FETCHED = 6;

    protected static final int STATE_ENDED = 7;

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
            case STATE_LAST_EXCLUSIVE_FETCHED:
                state = STATE_LAST_EXCLUSIVE;
                break;
            case STATE_NULL_INCLUSIVE_FETCHED:
                state = STATE_NULL_INCLUSIVE;
                break;
            case STATE_INIT:
            case STATE_LAST_INCLUSIVE:
            case STATE_LAST_EXCLUSIVE:
            case STATE_NULL_INCLUSIVE:
            case STATE_ENDED:
                break;
        }
    }

    @Override
    public int nextEntry() {

        switch (state) {
            default:
                throw new IllegalStateException();
            case STATE_ENDED:
                return 0;
            case STATE_INIT:
                return fetchFirst();
            case STATE_LAST_INCLUSIVE:
                index = ceiling();
                return fetchLoop();
            case STATE_LAST_INCLUSIVE_FETCHED:
                if (expectedModCount == map.modCount) {
                    state = STATE_LAST_EXCLUSIVE_FETCHED;
                    return index;
                } else {
                    index = ceiling();
                    return fetchLoop();
                }
            case STATE_LAST_EXCLUSIVE:
                index = higher();
                return fetchLoop();
            case STATE_LAST_EXCLUSIVE_FETCHED:
                if (expectedModCount == map.modCount) {
                    return fetchNext();
                } else {
                    index = higher();
                    return fetchLoop();
                }
            case STATE_NULL_INCLUSIVE:
                state = STATE_ENDED;
                return map.nullKey;
            case STATE_NULL_INCLUSIVE_FETCHED:
                state = STATE_ENDED;
                if (expectedModCount == map.modCount) {
                    return index;
                } else {
                    return map.nullKey;
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
            case STATE_INIT:
                return testFirst();
            case STATE_LAST_INCLUSIVE:
                index = ceiling();
                return testLoop();
            case STATE_LAST_INCLUSIVE_FETCHED:
                if (expectedModCount == map.modCount) {
                    return true;
                } else {
                    index = ceiling();
                    return testLoop();
                }
            case STATE_LAST_EXCLUSIVE:
                index = higher();
                return testLoop();
            case STATE_LAST_EXCLUSIVE_FETCHED:
                if (expectedModCount == map.modCount) {
                    return testNext();
                } else {
                    index = higher();
                    return testLoop();
                }
            case STATE_NULL_INCLUSIVE:
                if (map.nullKey != 0) {
                    index = map.nullKey;
                    expectedModCount = map.modCount;
                    state = STATE_NULL_INCLUSIVE_FETCHED;
                    return true;
                } else {
                    state = STATE_ENDED;
                    return false;
                }
            case STATE_NULL_INCLUSIVE_FETCHED:
                if (expectedModCount == map.modCount) {
                    return true;
                } else if (map.nullKey != 0) {
                    index = map.nullKey;
                    expectedModCount = map.modCount;
                    return true;
                } else {
                    state = STATE_ENDED;
                    return false;
                }
        }
    }

    private int fetchFirst() {

        index = map.getLastEntry();
        return fetchLoop();
    }

    private int fetchNext() {

        index = map.predecessor(index);
        return fetchLoop();
    }

    private int fetchLoop() {

        if (index == 0) {
            state = STATE_ENDED;
            if (nullEnded()) {
                return 0;
            }
            return map.nullKey;
        }
        if (ended()) {
            state = STATE_ENDED;
            return 0;
        }
        updateLastKey();
        expectedModCount = map.modCount;
        state = STATE_LAST_EXCLUSIVE_FETCHED;
        return index;
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

        if (index == 0) {
            if (nullEnded() || map.nullKey == 0) {
                state = STATE_ENDED;
                return false;
            } else {
                index = map.nullKey;
                expectedModCount = map.modCount;
                state = STATE_NULL_INCLUSIVE_FETCHED;
                return true;
            }
        }
        if (ended()) {
            state = STATE_ENDED;
            return false;
        }
        updateLastKey();
        expectedModCount = map.modCount;
        state = STATE_LAST_INCLUSIVE_FETCHED;
        return true;
    }
}
