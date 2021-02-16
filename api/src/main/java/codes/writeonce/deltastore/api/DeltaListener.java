package codes.writeonce.deltastore.api;

public interface DeltaListener<S extends Store<S>> {

    void acceptDeltas(
            DeltaRecord<S, ?, ?> removeListTail,
            DeltaRecord<S, ?, ?> updateListTail,
            DeltaRecord<S, ?, ?> insertListTail
    );
}
