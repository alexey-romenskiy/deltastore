package codes.writeonce.deltastore.api;

import javax.annotation.Nonnull;

public interface Transactional {

    @Nonnull
    Transaction begin();
}
