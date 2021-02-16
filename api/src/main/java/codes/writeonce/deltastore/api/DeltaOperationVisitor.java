package codes.writeonce.deltastore.api;

public interface DeltaOperationVisitor<U, X extends Throwable> {

    U visitInsert() throws X;

    U visitUpdate() throws X;

    U visitDelete() throws X;
}
