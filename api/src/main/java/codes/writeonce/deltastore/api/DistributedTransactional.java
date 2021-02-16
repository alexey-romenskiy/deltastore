package codes.writeonce.deltastore.api;

import javax.annotation.Nonnull;

public interface DistributedTransactional extends Transactional {

    @Nonnull
    @Override
    DistributedTransaction begin();
}
