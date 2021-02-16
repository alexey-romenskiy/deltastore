package codes.writeonce.deltastore.api;

public class ReleaseDeltaCommitListener<S extends Store<S>> implements CommitListener<S> {

    @Override
    public void commit(Record<S, ?, ?> tail) {
        if (tail != null) {
            final Record<S, ?, ?> head = tail.nextInChangeList;
            Record<S, ?, ?> item = head;
            do {
                item.diffStackTop.release();
                item = item.cleanRecord();
            } while (item != head);
        }
    }
}
