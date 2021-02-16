package codes.writeonce.deltastore;

import codes.writeonce.deltastore.api.ApplyStoreDeltaListener;
import codes.writeonce.deltastore.api.Id;
import codes.writeonce.deltastore.api.NotUniqueException;
import codes.writeonce.deltastore.api.ReleaseDeltaCommitListener;
import codes.writeonce.deltastore.api.Transaction;
import codes.writeonce.deltastore.api.TransformDeltaCommitListener;
import codes.writeonce.deltastore.example.model.common.Order;
import codes.writeonce.deltastore.example.model.order.Order2;
import codes.writeonce.deltastore.example.model.order.OrderStore;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class StoreTest {

    private static final Id<Order> ID1 = Id.of(123);
    private static final Id<Order> ID2 = Id.of(321);

    private static final Id<Order2> ID1B = Id.of(123);
    private static final Id<Order2> ID2B = Id.of(321);

    @Test
    public void commit() {

        final OrderStore store2 = new OrderStore(new ReleaseDeltaCommitListener<>());

        final OrderStore store =
                new OrderStore(new TransformDeltaCommitListener<>(new ApplyStoreDeltaListener<>(store2)));

        try (Transaction t = store.begin()) {
            store.order().create(ID1, 234).setBar("345");
            t.commit();
        }

        try (Transaction ignored = store.begin()) {
            assertEquals(ID1, store.orderFooIndex().list(234).get(0).getId());
            assertEquals(234, store.orderIdIndex().get(ID1).getFoo());
            assertEquals("345", store.orderIdIndex().get(ID1).getBar());
        }

        try (Transaction ignored = store2.begin()) {
            assertEquals(ID1, store2.orderFooIndex().list(234).get(0).getId());
            assertEquals(234, store2.orderIdIndex().get(ID1).getFoo());
            assertEquals("345", store2.orderIdIndex().get(ID1).getBar());
        }
    }

    @Test
    public void rollback1() {

        final OrderStore store2 = new OrderStore(new ReleaseDeltaCommitListener<>());

        final OrderStore store =
                new OrderStore(new TransformDeltaCommitListener<>(new ApplyStoreDeltaListener<>(store2)));

        try (Transaction t = store.begin()) {
            store.order().create(ID1, 234).setBar("345");
            store.order().create(ID2, 432).setBar("543");
            try (Transaction t2 = store.begin()) {
                final Order order = store.orderIdIndex().get(ID1);
                order.setBar(order.getBar() + "x");
            }
            try (Transaction t2 = store.begin()) {
                final Order order = store.orderIdIndex().get(ID2);
                order.setBar(order.getBar() + "y");
                t2.commit();
            }
            t.commit();
        }

        try (Transaction ignored = store.begin()) {
            assertEquals(ID1, store.orderFooIndex().list(234).get(0).getId());
            assertEquals(234, store.orderIdIndex().get(ID1).getFoo());
            assertEquals("345", store.orderIdIndex().get(ID1).getBar());
            assertEquals(ID2, store.orderFooIndex().list(432).get(0).getId());
            assertEquals(432, store.orderIdIndex().get(ID2).getFoo());
            assertEquals("543y", store.orderIdIndex().get(ID2).getBar());
        }

        try (Transaction ignored = store2.begin()) {
            assertEquals(ID1, store2.orderFooIndex().list(234).get(0).getId());
            assertEquals(234, store2.orderIdIndex().get(ID1).getFoo());
            assertEquals("345", store2.orderIdIndex().get(ID1).getBar());
            assertEquals(ID2, store2.orderFooIndex().list(432).get(0).getId());
            assertEquals(432, store2.orderIdIndex().get(ID2).getFoo());
            assertEquals("543y", store2.orderIdIndex().get(ID2).getBar());
        }
    }

    @Test
    public void rollback2() {

        final OrderStore store2 = new OrderStore(new ReleaseDeltaCommitListener<>());

        final OrderStore store =
                new OrderStore(new TransformDeltaCommitListener<>(new ApplyStoreDeltaListener<>(store2)));

        try (Transaction t = store.begin()) {
            store.order().create(ID1, 234).setBar("345");
            store.order().create(ID2, 432).setBar("543");
            try (Transaction t2 = store.begin()) {
                final Order order = store.orderIdIndex().get(ID1);
                order.setBar(order.getBar() + "x");
                t2.commit();
            }
            try (Transaction t2 = store.begin()) {
                final Order order = store.orderIdIndex().get(ID2);
                order.setBar(order.getBar() + "y");
            }
            t.commit();
        }

        try (Transaction ignored = store.begin()) {
            assertEquals(ID1, store.orderFooIndex().list(234).get(0).getId());
            assertEquals(234, store.orderIdIndex().get(ID1).getFoo());
            assertEquals("345x", store.orderIdIndex().get(ID1).getBar());
            assertEquals(ID2, store.orderFooIndex().list(432).get(0).getId());
            assertEquals(432, store.orderIdIndex().get(ID2).getFoo());
            assertEquals("543", store.orderIdIndex().get(ID2).getBar());
        }

        try (Transaction ignored = store2.begin()) {
            assertEquals(ID1, store2.orderFooIndex().list(234).get(0).getId());
            assertEquals(234, store2.orderIdIndex().get(ID1).getFoo());
            assertEquals("345x", store2.orderIdIndex().get(ID1).getBar());
            assertEquals(ID2, store2.orderFooIndex().list(432).get(0).getId());
            assertEquals(432, store2.orderIdIndex().get(ID2).getFoo());
            assertEquals("543", store2.orderIdIndex().get(ID2).getBar());
        }
    }

    @Test
    public void rollback3() {

        final OrderStore store = new OrderStore(new ReleaseDeltaCommitListener<>());

        try (Transaction t = store.begin()) {
            store.order().create(ID1, 234);
            t.commit();
        }

        try (Transaction ignored = store.begin()) {
            store.orderIdIndex().get(ID1).remove();
            store.order().create(ID1, 432);
        }

        try (Transaction ignored = store.begin()) {
            store.orderIdIndex().get(ID1).setBar("asd");
        }
    }

    @Test
    public void rollback4() {

        final OrderStore store = new OrderStore(new ReleaseDeltaCommitListener<>());

        try (Transaction t = store.begin()) {
            store.order2().create(ID1B, 234);
            store.order2().create(ID2B, 432);
            t.commit();
        }

        try (Transaction t = store.begin(true)) {
            store.order2IdIndex().get(ID1B).setFoo(432);
            store.order2IdIndex().get(ID2B).remove();
            t.commit();
        }
    }

    @Test
    public void rollback5() {

        final OrderStore store = new OrderStore(new ReleaseDeltaCommitListener<>());

        try (Transaction t = store.begin()) {
            store.order2().create(ID1B, 234);
            store.order2().create(ID2B, 432);
            t.commit();
        }

        try (Transaction t = store.begin(true)) {
            store.order2IdIndex().get(ID1B).setFoo(432);
            try {
                t.commit();
                fail();
            } catch (NotUniqueException ignored) {
                // expected
            }
        }
    }

    @Test
    public void rollback6() {

        final OrderStore store = new OrderStore(new ReleaseDeltaCommitListener<>());

        try (Transaction t = store.begin()) {
            store.order2().create(ID1B, 234);
            t.commit();
        }

        try (Transaction ignored = store.begin()) {
            assertTrue(store.order2IdIndex().get(ID1B).hasFoo());
            store.order2IdIndex().get(ID1B).clearFoo();
        }

        try (Transaction ignored = store.begin()) {
            assertTrue(store.order2IdIndex().get(ID1B).hasFoo());
        }
    }

    @Test
    public void nestedCommit() {

        final OrderStore store2 = new OrderStore(new ReleaseDeltaCommitListener<>());

        final OrderStore store =
                new OrderStore(new TransformDeltaCommitListener<>(new ApplyStoreDeltaListener<>(store2)));

        try (Transaction t = store.begin()) {
            store.order2().create(ID1B, 234);
            t.commit();
        }

        try (Transaction t = store.begin()) {

            store.order2IdIndex().get(ID1B).setFoo(456);

            try (Transaction t2 = store.begin()) {
                store.order2IdIndex().get(ID1B).setBar("345");
                t2.commit();
            }

            t.commit();
        }

        try (Transaction ignored = store.begin()) {
            assertEquals(ID1B, store.order2IdIndex().get(ID1B).getId());
            assertEquals(456, store.order2IdIndex().get(ID1B).getFoo());
            assertEquals("345", store.order2IdIndex().get(ID1B).getBar());
        }

        try (Transaction ignored = store2.begin()) {
            assertEquals(ID1B, store2.order2IdIndex().get(ID1B).getId());
            assertEquals(456, store2.order2IdIndex().get(ID1B).getFoo());
            assertEquals("345", store2.order2IdIndex().get(ID1B).getBar());
        }
    }

    @Test
    public void nestedCommit2() {

        final OrderStore store2 = new OrderStore(new ReleaseDeltaCommitListener<>());

        final OrderStore store =
                new OrderStore(new TransformDeltaCommitListener<>(new ApplyStoreDeltaListener<>(store2)));

        try (Transaction t = store.begin()) {

            final Order order1 = store.order().create(Id.of(1), 234);

            final Order order2 = store.order().create(Id.of(2), 345);

            try (Transaction t2 = store.begin()) {
                order1.remove();
                store.order().create(Id.of(3), 456);
                order2.remove();
                t2.commit();
            }

            t.commit();
        }

        try (Transaction ignored = store.begin()) {
            assertNull(store.orderIdIndex().get(Id.of(1)));
            assertNull(store.orderIdIndex().get(Id.of(2)));
            assertEquals(456, store.orderIdIndex().get(Id.of(3)).getFoo());
            assertEquals(1, store.orderIdIndex().list().size());
        }

        try (Transaction ignored = store2.begin()) {
            assertNull(store2.orderIdIndex().get(Id.of(1)));
            assertNull(store2.orderIdIndex().get(Id.of(2)));
            assertEquals(456, store2.orderIdIndex().get(Id.of(3)).getFoo());
            assertEquals(1, store2.orderIdIndex().list().size());
        }
    }

    @Test
    public void nestedCommit3() {

        final OrderStore store2 = new OrderStore(new ReleaseDeltaCommitListener<>());

        final OrderStore store =
                new OrderStore(new TransformDeltaCommitListener<>(new ApplyStoreDeltaListener<>(store2)));

        try (Transaction t = store.begin()) {

            final Order order0 = store.order().create(Id.of(4), 567);

            final Order order1 = store.order().create(Id.of(1), 234);

            final Order order2 = store.order().create(Id.of(2), 345);

            try (Transaction t2 = store.begin()) {
                order1.remove();
                store.order().create(Id.of(3), 456);
                order2.remove();
                t2.commit();
            }

            t.commit();
        }

        try (Transaction ignored = store.begin()) {
            assertNull(store.orderIdIndex().get(Id.of(1)));
            assertNull(store.orderIdIndex().get(Id.of(2)));
            assertEquals(567, store.orderIdIndex().get(Id.of(4)).getFoo());
            assertEquals(456, store.orderIdIndex().get(Id.of(3)).getFoo());
            assertEquals(2, store.orderIdIndex().list().size());
        }

        try (Transaction ignored = store2.begin()) {
            assertNull(store2.orderIdIndex().get(Id.of(1)));
            assertNull(store2.orderIdIndex().get(Id.of(2)));
            assertEquals(567, store2.orderIdIndex().get(Id.of(4)).getFoo());
            assertEquals(456, store2.orderIdIndex().get(Id.of(3)).getFoo());
            assertEquals(2, store2.orderIdIndex().list().size());
        }
    }

    @Test
    public void nestedCommit4() {

        final OrderStore store2 = new OrderStore(new ReleaseDeltaCommitListener<>());

        final OrderStore store =
                new OrderStore(new TransformDeltaCommitListener<>(new ApplyStoreDeltaListener<>(store2)));

        try (Transaction t = store.begin()) {
            store.order2().create(ID1B, 234).setBar("567");
            t.commit();
        }

        try (Transaction t = store.begin()) {

            store.order2IdIndex().get(ID1B).setFoo(456);

            try (Transaction t2 = store.begin()) {
                store.order2IdIndex().get(ID1B).setBar("345");
                t2.commit();
            }
        }

        try (Transaction ignored = store.begin()) {
            assertEquals(ID1B, store.order2IdIndex().get(ID1B).getId());
            assertEquals(234, store.order2IdIndex().get(ID1B).getFoo());
            assertEquals("567", store.order2IdIndex().get(ID1B).getBar());
        }

        try (Transaction ignored = store2.begin()) {
            assertEquals(ID1B, store2.order2IdIndex().get(ID1B).getId());
            assertEquals(234, store2.order2IdIndex().get(ID1B).getFoo());
            assertEquals("567", store2.order2IdIndex().get(ID1B).getBar());
        }
    }

    @Test
    public void nestedCommit5() {

        final OrderStore store2 = new OrderStore(new ReleaseDeltaCommitListener<>());

        final OrderStore store =
                new OrderStore(new TransformDeltaCommitListener<>(new ApplyStoreDeltaListener<>(store2)));

        try (Transaction t = store.begin()) {
            store.order2().create(ID1B, 234).setBar("567");
            t.commit();
        }

        try (Transaction t = store.begin()) {

            store.order2IdIndex().get(ID1B).setFoo(456);
            store.order2IdIndex().get(ID1B).setBar("678");

            try (Transaction t2 = store.begin()) {
                store.order2IdIndex().get(ID1B).setBar("345");
                t2.commit();
            }
        }

        try (Transaction ignored = store.begin()) {
            assertEquals(ID1B, store.order2IdIndex().get(ID1B).getId());
            assertEquals(234, store.order2IdIndex().get(ID1B).getFoo());
            assertEquals("567", store.order2IdIndex().get(ID1B).getBar());
        }

        try (Transaction ignored = store2.begin()) {
            assertEquals(ID1B, store2.order2IdIndex().get(ID1B).getId());
            assertEquals(234, store2.order2IdIndex().get(ID1B).getFoo());
            assertEquals("567", store2.order2IdIndex().get(ID1B).getBar());
        }
    }
}
