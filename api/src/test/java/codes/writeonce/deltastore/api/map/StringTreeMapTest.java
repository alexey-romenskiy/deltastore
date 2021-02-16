package codes.writeonce.deltastore.api.map;

import codes.writeonce.deltastore.api.SmartIterator;
import org.junit.Test;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class StringTreeMapTest {

    @Test
    public void test1() {
        assertEquals(emptyList(), collect1(iterate1(fixtute1())));
    }

    @Test
    public void test2() {
        assertEquals(emptyList(), collect2(iterate1(fixtute1())));
    }

    @Test
    public void test3() {
        assertEquals(asList("rty", "rty2"), collect1(iterate1(fixture2())));
    }

    @Test
    public void test4() {
        assertEquals(asList("rty", "rty2"), collect2(iterate1(fixture2())));
    }

    @Test
    public void test5() {
        assertEquals(asList("rty", "rty2"), collect1(iterate1(fixture3())));
    }

    @Test
    public void test6() {
        assertEquals(asList("rty", "rty2"), collect2(iterate1(fixture3())));
    }

    @Test
    public void test7() {
        assertEquals(asList("rty", "rty2"), collect1(iterate1(fixture4())));
    }

    @Test
    public void test8() {
        assertEquals(asList("rty", "rty2"), collect2(iterate1(fixture4())));
    }

    @Test
    public void test9() {
        final var map = fixture3();
        final var iterator = iterate1(map);
        assertTrue(iterator.hasNext());
        requireNonNull(map.get("012")).remove("asd");
        assertTrue(iterator.hasNext());
        assertEquals("rty2", iterator.get());
        assertFalse(iterator.hasNext());
        assertNull(iterator.get());
    }

    @Test
    public void test10() {
        final var map = fixture4();
        final var iterator = iterate1(map);
        assertTrue(iterator.hasNext());
        map.remove("012");
        assertTrue(iterator.hasNext());
        assertEquals("rty2", iterator.get());
        assertFalse(iterator.hasNext());
        assertNull(iterator.get());
    }

    @Nonnull
    private NestedIterator<StringTreeMap<StringTreeMap<StringTreeMap<String>>>, String> iterate1(
            @Nonnull StringTreeMap<StringTreeMap<StringTreeMap<String>>> map) {

        return map.iterator(StringTreeMap.newIterator(StringTreeMap.newIterator(false), false), false);
    }

    @Nonnull
    private StringTreeMap<StringTreeMap<StringTreeMap<String>>> fixtute1() {
        return new StringTreeMap<>();
    }

    @Nonnull
    private StringTreeMap<StringTreeMap<StringTreeMap<String>>> fixture2() {

        final StringTreeMap<StringTreeMap<StringTreeMap<String>>> map1 = new StringTreeMap<>();
        final StringTreeMap<StringTreeMap<String>> map2 = new StringTreeMap<>();
        final StringTreeMap<String> map3 = new StringTreeMap<>();
        map3.put("qwe", "rty");
        map3.put("qwe2", "rty2");
        map2.put("asd", map3);
        map1.put("012", map2);
        return map1;
    }

    @Nonnull
    private StringTreeMap<StringTreeMap<StringTreeMap<String>>> fixture3() {

        final StringTreeMap<StringTreeMap<StringTreeMap<String>>> map1 = new StringTreeMap<>();
        final StringTreeMap<StringTreeMap<String>> map2 = new StringTreeMap<>();
        final StringTreeMap<String> map3 = new StringTreeMap<>();
        final StringTreeMap<String> map4 = new StringTreeMap<>();
        map3.put("qwe", "rty");
        map4.put("qwe", "rty2");
        map2.put("asd", map3);
        map2.put("asd2", map4);
        map1.put("012", map2);
        return map1;
    }

    @Nonnull
    private StringTreeMap<StringTreeMap<StringTreeMap<String>>> fixture4() {

        final StringTreeMap<StringTreeMap<StringTreeMap<String>>> map1 = new StringTreeMap<>();
        final StringTreeMap<StringTreeMap<String>> map2 = new StringTreeMap<>();
        final StringTreeMap<StringTreeMap<String>> map3 = new StringTreeMap<>();
        final StringTreeMap<String> map4 = new StringTreeMap<>();
        final StringTreeMap<String> map5 = new StringTreeMap<>();
        map4.put("qwe", "rty");
        map5.put("qwe", "rty2");
        map2.put("asd", map4);
        map3.put("asd", map5);
        map1.put("012", map2);
        map1.put("123", map3);
        return map1;
    }

    @Nonnull
    private List<String> collect1(SmartIterator<String> iterator) {

        final List<String> list = new ArrayList<>();
        while (true) {
            final String next = iterator.get();
            if (next == null) {
                assertFalse(iterator.hasNext());
                assertNull(iterator.get());
                break;
            }
            list.add(next);
        }
        return list;
    }

    @Nonnull
    private List<String> collect2(SmartIterator<String> iterator) {

        final List<String> list = new ArrayList<>();
        while (true) {
            final boolean hasNext = iterator.hasNext();
            final String next = iterator.get();
            if (next == null) {
                assertFalse(hasNext);
                assertFalse(iterator.hasNext());
                assertNull(iterator.get());
                break;
            }
            assertTrue(hasNext);
            list.add(next);
        }
        return list;
    }
}
