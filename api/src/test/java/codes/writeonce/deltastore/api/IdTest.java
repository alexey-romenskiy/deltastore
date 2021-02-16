package codes.writeonce.deltastore.api;

import org.junit.Test;

import static org.junit.Assert.assertSame;

public class IdTest {

    @Test
    public void of() {
        assertSame(Id.of(1000), Id.of(1000));
    }
}
