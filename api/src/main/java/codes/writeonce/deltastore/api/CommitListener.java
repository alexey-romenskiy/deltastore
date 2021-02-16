package codes.writeonce.deltastore.api;

public interface CommitListener<S extends Store<S>> {

    void commit(Record<S, ?, ?> tail);
}
