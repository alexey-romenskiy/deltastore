package codes.writeonce.deltastore.api;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

public class TransformDeltaCommitListener<S extends Store<S>> implements CommitListener<S> {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private final DeltaListener<S> deltaListener;

    public TransformDeltaCommitListener(DeltaListener<S> deltaListener) {
        this.deltaListener = deltaListener;
    }

    @Override
    public void commit(Record<S, ?, ?> tail) {

        DeltaRecord<S, ?, ?> removeListTail = null;
        DeltaRecord<S, ?, ?> updateListTail = null;
        DeltaRecord<S, ?, ?> insertListTail = null;

        int removes = 0;
        int updates = 0;
        int inserts = 0;

        if (tail != null) {
            final Record<S, ?, ?> head = tail.nextInChangeList;
            Record<S, ?, ?> item = head;
            do {
                final DeltaRecord<S, ?, ?> deltaRecord = item.diffStackTop;
                if (item.normalizeChange()) {
                    if ((deltaRecord.diff0 & Record.MASK_EXISTS) == 0) {
                        if ((deltaRecord.mask0 & Record.MASK_EXISTS) == 0) {
                            deltaRecord.release();
                        } else {
                            updateListTail = appendTail(updateListTail, deltaRecord);
                            updates++;
                        }
                    } else {
                        if ((deltaRecord.mask0 & Record.MASK_EXISTS) == 0) {
                            removeListTail = appendTail(removeListTail, deltaRecord);
                            removes++;
                        } else {
                            insertListTail = appendTail(insertListTail, deltaRecord);
                            inserts++;
                        }
                    }
                } else {
                    deltaRecord.release();
                }
                item = item.cleanRecord();
            } while (item != head);
        }

        logger.trace("Transformed removes={} updates={} inserts={}", removes, updates, inserts);

        try {
            deltaListener.acceptDeltas(removeListTail, updateListTail, insertListTail);
        } catch (Exception e) {
            // TODO: 01.06.18 must not happen
            logger.error("Failed to process deltas", e);
        }
    }

    @Nonnull
    private DeltaRecord<S, ?, ?> appendTail(DeltaRecord<S, ?, ?> tail, DeltaRecord<S, ?, ?> item) {
        if (tail == null) {
            item.next = item;
        } else {
            item.next = tail.next;
            tail.next = item;
        }
        return item;
    }
}
