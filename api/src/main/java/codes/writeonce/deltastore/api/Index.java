package codes.writeonce.deltastore.api;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

public interface Index<E extends Entity<?>> {

    SmartIterator<E> iterator();

    Stream<E> stream();

    List<E> list();

    int size();

    boolean isEmpty();

    @Nullable
    E first();

    @Nullable
    E optional() throws NotUniqueException;

    @Nonnull
    E single() throws NoSuchElementException, NotUniqueException;
}
