package codes.writeonce.deltastore.api;

public interface Table<E extends Entity<E>> {

    EntityType<E> getType();

    String getName();

    <X extends Throwable> E create(FieldValueSupplierVisitor<X, E> fieldValueSupplierVisitor) throws X;

    <X extends Throwable> E get(FieldValueSupplierVisitor<X, E> fieldValueSupplierVisitor) throws X;
}
