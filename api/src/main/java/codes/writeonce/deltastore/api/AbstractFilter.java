package codes.writeonce.deltastore.api;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public abstract class AbstractFilter<E extends Entity<?>> implements Index<E> {

    @Override
    public List<E> list() {
        final List<E> list = new ArrayList<>();
        try (var iterator = iterator()) {
            while (true) {
                final var value = iterator.get();
                if (value == null) {
                    break;
                }
                list.add(value);
            }
        }
        return list;
    }

    @Override
    public void list(@Nonnull ArrayList<? super E> list) {
        list.clear();
        try (var iterator = iterator()) {
            while (true) {
                final var value = iterator.get();
                if (value == null) {
                    break;
                }
                list.add(value);
            }
        }
    }

    @Override
    public int size() {
        try (var iterator = iterator()) {
            return iterator.skip(Integer.MAX_VALUE);
        }
    }

    @Override
    public boolean isEmpty() {
        try (var iterator = iterator()) {
            return !iterator.hasNext();
        }
    }

    @Override
    public Stream<E> stream() {
        final var iterator = iterator();
        return StreamSupport.stream(iterator, false).onClose(iterator::close);
    }

    @Nullable
    @Override
    public E first() {
        try (var iterator = iterator()) {
            return iterator.get();
        }
    }

    @Nullable
    @Override
    public E optional() throws NotUniqueException {
        try (var iterator = iterator()) {
            final var value = iterator.get();
            if (value == null) {
                return null;
            }
            if (iterator.hasNext()) {
                throw new NotUniqueException();
            } else {
                return value;
            }
        }
    }

    @Nonnull
    @Override
    public E single() throws NoSuchElementException, NotUniqueException {
        try (var iterator = iterator()) {
            final var value = iterator.next();
            if (iterator.hasNext()) {
                throw new NotUniqueException();
            } else {
                return value;
            }
        }
    }
}
