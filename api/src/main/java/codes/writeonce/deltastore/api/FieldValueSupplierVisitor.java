package codes.writeonce.deltastore.api;

import javax.annotation.Nonnull;
import java.math.BigDecimal;
import java.time.Instant;

public interface FieldValueSupplierVisitor<X extends Throwable, E extends Entity<E>> {

    BigDecimal visit(BigDecimalField<E> field) throws X;

    Long visit(LongField<E> field) throws X;

    Instant visit(InstantField<E> field) throws X;

    Boolean visit(BooleanField<E> field) throws X;

    <N extends Enum<N>> N visit(EnumField<E, N> field) throws X;

    <I extends Id> I visit(IdField<E, I> field) throws X;

    String visit(StringField<E> field) throws X;

    Integer visit(IntegerField<E> field) throws X;

    boolean visitAsBoolean(@Nonnull BooleanField<E> field) throws X;

    int visitAsInt(@Nonnull IntegerField<E> field) throws X;

    long visitAsLong(@Nonnull LongField<E> field) throws X;
}
