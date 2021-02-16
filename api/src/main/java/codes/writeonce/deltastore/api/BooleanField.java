package codes.writeonce.deltastore.api;

import javax.annotation.Nonnull;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;

public class BooleanField<E extends Entity<E>> extends AbstractField<E, Boolean> {

    private final Boolean defaultValue;

    private final Predicate<E> getter;

    private final BiConsumer<E, Boolean> setter;

    private final Predicate<E> changeTester;

    private final Function<E, Boolean> getterNullable;

    private final BiConsumer<E, Boolean> setterNullable;

    private final Consumer<E> clearer;

    private final Predicate<E> tester;

    private final Consumer<E> nullSetter;

    private final Predicate<E> nullTester;

    public BooleanField(
            EntityType<E> entityType,
            String name,
            boolean nullable,
            boolean mutable,
            Boolean defaultValue,
            Predicate<E> getter,
            BiConsumer<E, Boolean> setter,
            Predicate<E> changeTester,
            Predicate<E> tester,
            Consumer<E> clearer,
            Predicate<E> nullTester,
            Consumer<E> nullSetter,
            Function<E, Boolean> getterNullable,
            BiConsumer<E, Boolean> setterNullable
    ) {
        super(entityType, name, nullable, mutable);
        this.defaultValue = defaultValue;
        this.getter = getter;
        this.setter = setter;
        this.changeTester = changeTester;
        this.getterNullable = getterNullable;
        this.setterNullable = setterNullable;
        this.clearer = clearer;
        this.tester = tester;
        this.nullSetter = nullSetter;
        this.nullTester = nullTester;
    }

    @Override
    public Boolean getDefaultValue() {
        return defaultValue;
    }

    @Override
    public boolean isDefaultNull(@Nonnull E entity) {
        return defaultValue == null;
    }

    public boolean getDefaultValueAsBoolean() {
        return defaultValue;
    }

    @Override
    public Boolean getValue(@Nonnull E entity) {
        return getterNullable.apply(entity);
    }

    public boolean getValueAsBoolean(@Nonnull E entity) {
        return getter.test(entity);
    }

    @Override
    public void setValue(@Nonnull E entity, Boolean value) {
        setterNullable.accept(entity, value);
    }

    public void setValue(@Nonnull E entity, boolean value) {
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
