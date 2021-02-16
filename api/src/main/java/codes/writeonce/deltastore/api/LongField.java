package codes.writeonce.deltastore.api;

import javax.annotation.Nonnull;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ObjLongConsumer;
import java.util.function.Predicate;
import java.util.function.ToLongFunction;

public class LongField<E extends Entity<E>> extends AbstractField<E, Long> {

    private final Long defaultValue;

    private final ToLongFunction<E> getter;

    private final Predicate<E> changeTester;

    private final Function<E, Long> getterNullable;

    private final ObjLongConsumer<E> setter;

    private final BiConsumer<E, Long> setterNullable;

    private final Consumer<E> clearer;

    private final Predicate<E> tester;

    private final Consumer<E> nullSetter;

    private final Predicate<E> nullTester;

    public LongField(
            EntityType<E> entityType,
            String name,
            boolean nullable,
            boolean mutable,
            Long defaultValue,
            ToLongFunction<E> getter,
            ObjLongConsumer<E> setter,
            Predicate<E> changeTester,
            Predicate<E> tester,
            Consumer<E> clearer,
            Predicate<E> nullTester,
            Consumer<E> nullSetter,
            Function<E, Long> getterNullable,
            BiConsumer<E, Long> setterNullable
    ) {
        super(entityType, name, nullable, mutable);
        this.defaultValue = defaultValue;
        this.getter = getter;
        this.changeTester = changeTester;
        this.getterNullable = getterNullable;
        this.setter = setter;
        this.setterNullable = setterNullable;
        this.clearer = clearer;
        this.tester = tester;
        this.nullSetter = nullSetter;
        this.nullTester = nullTester;
    }

    @Override
    public Long getDefaultValue() {
        return defaultValue;
    }

    @Override
    public boolean isDefaultNull(@Nonnull E entity) {
        return defaultValue == null;
    }

    public long getDefaultValueAsLong() {
        return defaultValue;
    }

    @Override
    public Long getValue(@Nonnull E entity) {
        return getterNullable.apply(entity);
    }

    public long getValueAsLong(@Nonnull E entity) {
        return getter.applyAsLong(entity);
    }

    @Override
    public void setValue(@Nonnull E entity, Long value) {
        setterNullable.accept(entity, value);
    }

    public void setValue(@Nonnull E entity, long value) {
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
