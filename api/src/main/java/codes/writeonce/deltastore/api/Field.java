package codes.writeonce.deltastore.api;

import javax.annotation.Nonnull;

public interface Field<E extends Entity<E>, V> {

    EntityType<E> getEntityType();

    String getName();

    boolean isNullable();

    boolean isMutable();

    V getDefaultValue();

    boolean isDefaultNull(@Nonnull E entity);

    V getValue(@Nonnull E entity);

    void setValue(@Nonnull E entity, V value);

    void clearValue(@Nonnull E entity);

    boolean isChanged(@Nonnull E entity);

    boolean isSet(@Nonnull E entity);

    boolean isNull(@Nonnull E entity);

    void setNull(@Nonnull E entity);

    <U, X extends Throwable> U accept(Visitor<U, X, E> visitor) throws X;

    interface Visitor<U, X extends Throwable, E extends Entity<E>> {

        U visit(BigDecimalField<E> field) throws X;

        U visit(LongField<E> field) throws X;

        U visit(InstantField<E> field) throws X;

        U visit(BooleanField<E> field) throws X;

        <N extends Enum<N>> U visit(EnumField<E, N> field) throws X;

        <I extends Id> U visit(IdField<E, I> field) throws X;

        U visit(StringField<E> field) throws X;

        U visit(IntegerField<E> field) throws X;
    }
}
