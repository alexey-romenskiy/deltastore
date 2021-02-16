package codes.writeonce.deltastore.api;

import javax.annotation.Nonnull;

public interface FieldValueConsumerVisitor<U, X extends Throwable, E extends Entity<E>> {

    <F extends Field<E, V>, V> U visitClear(@Nonnull F field) throws X;

    <F extends Field<E, V>, V> U visitSet(@Nonnull F field, V value) throws X;

    U visitSet(@Nonnull BooleanField<E> field, boolean value) throws X;

    U visitSet(@Nonnull IntegerField<E> field, int value) throws X;

    U visitSet(@Nonnull LongField<E> field, long value) throws X;
}
