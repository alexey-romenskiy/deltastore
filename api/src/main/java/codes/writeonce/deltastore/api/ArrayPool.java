package codes.writeonce.deltastore.api;

import javax.annotation.Nonnull;
import java.util.function.Supplier;

public class ArrayPool<T> implements Pool<T> {

    private final Object[] array;

    private final Supplier<T> factory;

    private int index = 0;

    public ArrayPool(int capacity, Supplier<T> factory) {
        array = new Object[capacity];
        this.factory = factory;
    }

    @SuppressWarnings("unchecked")
    @Nonnull
    @Override
    public T get() {

        if (index > 0) {
            final var value = (T) array[--index];
            array[index] = null;
            return value;
        }
        return factory.get();
    }

    @Override
    public void put(@Nonnull T value) {

        if (index < array.length) {
            array[index++] = value;
        }
    }
}
