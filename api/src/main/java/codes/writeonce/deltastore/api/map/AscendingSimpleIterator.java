package codes.writeonce.deltastore.api.map;

import javax.annotation.Nonnull;

abstract class AscendingSimpleIterator<K, V, M extends AbstractTreeMap<K, V, M>>
        extends AbstractSimpleIterator<K, V, M> {

    /**
     * null entry should be next (but possibly not exist)
     */
    protected static final int STATE_NULL_INCLUSIVE = 0;

    /**
     * null entry should be next and exist, index points to null entry, not consumed,
     * expectedModCount is on track
     */
    private static final int STATE_NULL_INCLUSIVE_FETCHED = 1;

    /**
     * null entry consumed, first non-null entry should be next (but possibly not exist)
     */
    protected static final int STATE_NULL_EXCLUSIVE = 2;

    /**
     * lastKey points to next non-null entry (possibly non-existing)
     */
    protected static final int STATE_LAST_INCLUSIVE = 3;

    /**
     * lastKey and index points to next non-null entry, not consumed, expectedModCount is on track
     */
    private static final int STATE_LAST_INCLUSIVE_FETCHED = 4;

    /**
     * lastKey points to previously consumed non-null entry
     */
    protected static final int STATE_LAST_EXCLUSIVE = 5;

    /**
     * lastKey and index points to previously consumed non-null entry, expectedModCount is on track
     */
    private static final int STATE_LAST_EXCLUSIVE_FETCHED = 6;

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
            case STATE_NULL_INCLUSIVE_FETCHED:
                state = STATE_NULL_INCLUSIVE;
                break;
            case STATE_LAST_INCLUSIVE_FETCHED:
                state = STATE_LAST_INCLUSIVE;
                break;
            case STATE_LAST_EXCLUSIVE_FETCHED:
                state = STATE_LAST_EXCLUSIVE;
                break;
            case STATE_NULL_INCLUSIVE:
            case STATE_NULL_EXCLUSIVE:
            case STATE_LAST_INCLUSIVE:
            case STATE_LAST_EXCLUSIVE:
            case STATE_ENDED:
                break;
        }
    }

    @Override
    public final int nextEntry() {

        switch (state) {
            default:
                throw new IllegalStateException();
            case STATE_ENDED:
                return 0;
            case STATE_NULL_INCLUSIVE:
                if (map.nullKey != 0) {
                    index = map.nullKey;
                    state = STATE_NULL_EXCLUSIVE;
                    return index;
                }
                return fetchFirst();
            case STATE_NULL_INCLUSIVE_FETCHED:
                if (expectedModCount == map.modCount) {
                    state = STATE_NULL_EXCLUSIVE;
                    return index;
                } else if (map.nullKey != 0) {
                    index = map.nullKey;
                    state = STATE_NULL_EXCLUSIVE;
                    return index;
                }
                return fetchFirst();
            case STATE_NULL_EXCLUSIVE:
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
        }
    }

    @Override
    public boolean hasNext() {

        switch (state) {
            default:
                throw new IllegalStateException();
            case STATE_ENDED:
                return false;
            case STATE_NULL_INCLUSIVE:
                if (map.nullKey != 0) {
                    index = map.nullKey;
                    expectedModCount = map.modCount;
                    state = STATE_NULL_INCLUSIVE_FETCHED;
                    return true;
                }
                return testFirst();
            case STATE_NULL_INCLUSIVE_FETCHED:
                if (expectedModCount == map.modCount) {
                    return true;
                } else if (map.nullKey != 0) {
                    index = map.nullKey;
                    expectedModCount = map.modCount;
                    return true;
                }
                return testFirst();
            case STATE_NULL_EXCLUSIVE:
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
        }
    }

    private int fetchFirst() {

        index = map.getFirstEntry();
        return fetchLoop();
    }

    private int fetchNext() {

        index = map.successor(index);
        return fetchLoop();
    }

    private int fetchLoop() {

        if (index == 0 || ended()) {
            state = STATE_ENDED;
            return 0;
        }
        updateLastKey();
        expectedModCount = map.modCount;
        state = STATE_LAST_EXCLUSIVE_FETCHED;
        return index;
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

        if (index == 0 || ended()) {
            state = STATE_ENDED;
            return false;
        }
        updateLastKey();
        expectedModCount = map.modCount;
        state = STATE_LAST_INCLUSIVE_FETCHED;
        return true;
    }
}
