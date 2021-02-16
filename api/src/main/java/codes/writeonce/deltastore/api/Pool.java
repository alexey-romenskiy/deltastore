package codes.writeonce.deltastore.api;

import javax.annotation.Nonnull;

public interface Pool<T> {

    @Nonnull
    T get();

    void put(@Nonnull T value);
}
