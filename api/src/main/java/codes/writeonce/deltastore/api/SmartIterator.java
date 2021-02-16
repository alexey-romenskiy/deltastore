package codes.writeonce.deltastore.api;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.Closeable;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static java.util.Objects.requireNonNull;

public interface SmartIterator<T> extends Iterator<T>, Spliterator<T>, Supplier<T>, Closeable {

    default boolean skip() {
        if (hasNext()) {
            next();
            return true;
        } else {
            return false;
        }
    }

    default int skip(int count) {
        int skipped = 0;
        while (skipped < count && skip()) {
            skipped++;
        }
        return skipped;
    }

    @Nonnull
    @Override
    default T next() throws NoSuchElementException {
        final var next = get();
        if (next == null) {
            throw new NoSuchElementException();
        }
        return next;
    }

    @Override
    @Nullable
    T get();

    @Override
    default void forEachRemaining(Consumer<? super T> action) {

        requireNonNull(action);
        while (true) {
            final var next = get();
            if (next == null) {
                break;
            }
            action.accept(next);
        }
    }

    @Override
    default void close() {
        // empty
    }
}
