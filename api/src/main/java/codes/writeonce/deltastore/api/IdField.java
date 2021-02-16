package codes.writeonce.deltastore.api;

import javax.annotation.Nonnull;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongFunction;
import java.util.function.Predicate;

public class IdField<E extends Entity<E>, I extends Id> extends AbstractField<E, I> {

    private final LongFunction<I> idFactory;

    private final I defaultValue;

    private final Function<E, I> getter;

    private final BiConsumer<E, I> setter;

    private final Predicate<E> changeTester;

    private final Consumer<E> clearer;

    private final Predicate<E> tester;

    private final Consumer<E> nullSetter;

    private final Predicate<E> nullTester;

    public IdField(
            EntityType<E> entityType,
            String name,
            boolean nullable,
            boolean mutable,
            LongFunction<I> idFactory,
            I defaultValue,
            Function<E, I> getter,
            BiConsumer<E, I> setter,
            Predicate<E> changeTester,
            Predicate<E> tester,
            Consumer<E> clearer,
            Predicate<E> nullTester,
            Consumer<E> nullSetter
    ) {
        super(entityType, name, nullable, mutable);
        this.idFactory = idFactory;
        this.defaultValue = defaultValue;
        this.getter = getter;
        this.setter = setter;
        this.changeTester = changeTester;
        this.clearer = clearer;
        this.tester = tester;
        this.nullSetter = nullSetter;
        this.nullTester = nullTester;
    }

    public I createId(long value) {
        return idFactory.apply(value);
    }

    @Override
    public I getDefaultValue() {
        return defaultValue;
    }

    @Override
    public boolean isDefaultNull(@Nonnull E entity) {
        return defaultValue == null;
    }

    @Override
    public I getValue(@Nonnull E entity) {
        return getter.apply(entity);
    }

    @Override
    public void setValue(@Nonnull E entity, I value) {
        setter.accept(entity, value);
    }

    @Override
    public void clearValue(@Nonnull E entity) {
        clearer.accept(entity);
    }

    @Override
    public boolean isChanged(@Nonnull E entity) {
        return changeTester.test(entity);
    }

    @Override
    public boolean isSet(@Nonnull E entity) {
        return tester.test(entity);
    }

    @Override
    public boolean isNull(@Nonnull E entity) {
        return nullTester.test(entity);
    }

    @Override
    public void setNull(@Nonnull E entity) {
        nullSetter.accept(entity);
    }

    @Override
    public <U, X extends Throwable> U accept(Visitor<U, X, E> visitor) throws X {
        return visitor.visit(this);
    }
}
