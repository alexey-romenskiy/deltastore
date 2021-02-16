package codes.writeonce.deltastore.api;

public class ReleaseDeltaListener<S extends Store<S>> implements DeltaListener<S> {

    @Override
    public void acceptDeltas(
            DeltaRecord<S, ?, ?> removeListTail,
            DeltaRecord<S, ?, ?> updateListTail,
            DeltaRecord<S, ?, ?> insertListTail
    ) {
        DeltaRecord.iterate(removeListTail, DeltaRecord::release);
        DeltaRecord.iterate(updateListTail, DeltaRecord::release);
        DeltaRecord.iterate(insertListTail, DeltaRecord::release);
    }
}
