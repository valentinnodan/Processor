import org.junit.Test;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Function;

import static java.lang.Thread.sleep;
import static org.junit.Assert.*;


public class RunnerTest {

    static class SimpleProcessor<T> implements Processor<T> {
        String id;
        List<String> inId;
        Function<List<T>, T> myRun;

        SimpleProcessor(String id, List<String> inId, Function<List<T>, T> fun) {
            this.id = id;
            this.inId = inId;
            this.myRun = fun;
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public List<String> getInputIds() {
            return inId;
        }

        @Override
        public T process(List<T> input) throws ProcessorException {
            return myRun.apply(input);
        }
    }

    static class ThrowProcessor<T> implements Processor<T> {
        String id;
        List<String> inId;

        ThrowProcessor(String id, List<String> inId) {
            this.id = id;
            this.inId = inId;
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public List<String> getInputIds() {
            return inId;
        }

        @Override
        public T process(List<T> input) throws ProcessorException {
            throw new ProcessorException("Hi");
        }
    }

    @Test(timeout = 1_000)
    public void small_graph() {
        Processor<Integer> fst = new SimpleProcessor<>("1", List.of(), (List<Integer> list) -> {
            return 3;
        });
        Processor<Integer> snd = new SimpleProcessor<>("2", List.of(), (List<Integer> list) -> {
            return 2;
        });
        Processor<Integer> trd = new SimpleProcessor<>("3", List.of("1", "2"), (List<Integer> list) -> {
            return list.get(0) + list.get(1);
        });
        Runner<Integer> myRunner = new RunnerImpl<>();
        try {
            Map<String, List<Integer>> expected = new HashMap<>();
            expected.put("1", List.of(3));
            expected.put("2", List.of(2));
            expected.put("3", List.of(5));
            assertEquals(expected, myRunner.runProcessors(Set.of(fst, snd, trd), 1, 1));
        } catch (ProcessorException e) {
            fail();
        }
    }

    @Test(timeout = 100)
    public void fail_graph() {
        Processor<Integer> fst = new SimpleProcessor<>("1", List.of(), (List<Integer> list) -> {
            return 3;
        });
        Processor<Integer> snd = new SimpleProcessor<>("2", List.of("3"), (List<Integer> list) -> {
            return 2;
        });
        Processor<Integer> trd = new SimpleProcessor<>("3", List.of("1", "2"), (List<Integer> list) -> {
            return list.get(0) + list.get(1);
        });
        Runner<Integer> myRunner = new RunnerImpl<>();
        try {
            myRunner.runProcessors(Set.of(fst, snd, trd), 1, 1);
        } catch (ProcessorException e) {
            assertTrue(true);
            return;
        }
        fail();
    }

    @Test(timeout = 1_000)
    public void small_graph_many_iter() {
        Processor<Integer> fst = new SimpleProcessor<>("1", List.of(), (List<Integer> list) -> {
            try {
                sleep(100, 0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return 3;
        });
        Processor<Integer> snd = new SimpleProcessor<>("2", List.of(), (List<Integer> list) -> {
            try {
                sleep(100, 0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return 2;
        });
        Processor<Integer> trd = new SimpleProcessor<>("3", List.of("1", "2"), (List<Integer> list) -> {
            try {
                sleep(100, 0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return list.get(0) + list.get(1);
        });
        Runner<Integer> myRunner = new RunnerImpl<>();
        try {
            Map<String, List<Integer>> expected = new HashMap<>();
            expected.put("1", List.of(3, 3, 3, 3, 3));
            expected.put("2", List.of(2, 2, 2, 2, 2));
            expected.put("3", List.of(5, 5, 5, 5, 5));
            assertEquals(expected, myRunner.runProcessors(Set.of(fst, snd, trd), 3, 5));
        } catch (ProcessorException e) {
            fail();
        }
    }

    @Test(timeout = 1_000)
    public void mutex_test() {
        Lock lock1 = new ReentrantLock();
        Lock lock2 = new ReentrantLock();
        Lock lock3 = new ReentrantLock();
        Processor<Integer> fst = new SimpleProcessor<>("1", List.of(), (List<Integer> list) -> {
            boolean try1 = lock1.tryLock();
            if (!try1) {
                return -3;
            }
            try {
                sleep(100, 0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            lock1.unlock();
            return 3;
        });
        Processor<Integer> snd = new SimpleProcessor<>("2", List.of(), (List<Integer> list) -> {
            boolean try2 = lock2.tryLock();
            if (!try2) {
                return -2;
            }
            try {
                sleep(100, 0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            lock2.unlock();
            return 2;
        });
        Processor<Integer> trd = new SimpleProcessor<>("3", List.of("1", "2"), (List<Integer> list) -> {
            boolean try3 = lock3.tryLock();
            if (!try3) {
                return -(list.get(0) + list.get(1));
            }
            try {
                sleep(100, 0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            lock3.unlock();
            return list.get(0) + list.get(1);
        });
        Runner<Integer> myRunner = new RunnerImpl<>();
        try {
            Map<String, List<Integer>> expected = new HashMap<>();
            expected.put("1", List.of(3, 3, 3, 3, 3));
            expected.put("2", List.of(2, 2, 2, 2, 2));
            expected.put("3", List.of(5, 5, 5, 5, 5));
            assertEquals(expected, myRunner.runProcessors(Set.of(fst, snd, trd), 3, 5));
        } catch (ProcessorException e) {
            fail();
        }
    }


    @Test(timeout = 20_000)
    public void big_test() {
        final Integer[] num = new Integer[1];
        num[0] = 0;
        Processor<Integer> fst = new SimpleProcessor<>("1", List.of(), (List<Integer> list) -> {
            return num[0]++;

        });
        Processor<Integer> snd = new SimpleProcessor<>("2", List.of("1"), (List<Integer> list) -> {
            return 0;
        });
        Processor<Integer> trd = new SimpleProcessor<>("3", List.of("1", "2"), (List<Integer> list) -> {
            return list.get(0) + list.get(1);
        });
        Runner<Integer> myRunner = new RunnerImpl<>();
        try {
            final int iterators = 100000;
            Map<String, List<Integer>> expected = new HashMap<>();
            List<Integer> list1 = new ArrayList<>(iterators);
            List<Integer> list2 = new ArrayList<>(iterators);
            List<Integer> list3 = new ArrayList<>(iterators);
            for (int i = 0; i < iterators; ++i) {
                list1.add(i);
                list2.add(0);
                list3.add(i);
            }
            expected.put("1", list1);
            expected.put("2", list2);
            expected.put("3", list3);
            assertEquals(expected, myRunner.runProcessors(Set.of(fst, snd, trd), 7, iterators));
        } catch (ProcessorException e) {
            fail();
        }
    }

    @Test(timeout = 10_000)
    public void return_null_test1() {
        final Integer[] num = new Integer[1];
        num[0] = 0;
        Processor<Integer> fst = new SimpleProcessor<>("1", List.of(), (List<Integer> list) -> {
            if (num[0] >= 100) {
                return null;
            }
            return num[0]++;

        });
        Processor<Integer> snd = new SimpleProcessor<>("2", List.of("1"), (List<Integer> list) -> {
            return 0;
        });
        Processor<Integer> trd = new SimpleProcessor<>("3", List.of("1", "2"), (List<Integer> list) -> {
            return list.get(0) + list.get(1);
        });
        Runner<Integer> myRunner = new RunnerImpl<>();
        try {
            final int iterators = 10000;
            Map<String, List<Integer>> expected = new HashMap<>();
            List<Integer> list1 = new ArrayList<>(iterators);
            List<Integer> list2 = new ArrayList<>(iterators);
            List<Integer> list3 = new ArrayList<>(iterators);
            for (int i = 0; i < 100; ++i) {
                list1.add(i);
                list2.add(0);
                list3.add(i);
            }
            expected.put("1", list1);
            expected.put("2", list2);
            expected.put("3", list3);
            assertEquals(expected, myRunner.runProcessors(Set.of(fst, snd, trd), 3, iterators));
        } catch (ProcessorException e) {
            fail();
        }
    }

    @Test(timeout = 15_000)
    public void return_null_test2() {
        final Integer[] num = new Integer[1];
        num[0] = 0;
        Processor<Integer> fst = new SimpleProcessor<>("1", List.of(), (List<Integer> list) -> {
            if (num[0] >= 10000) {
                return null;
            }
            return num[0]++;

        });
        Processor<Integer> snd = new SimpleProcessor<>("2", List.of("1"), (List<Integer> list) -> {
            return 0;
        });
        Processor<Integer> trd = new SimpleProcessor<>("3", List.of("1", "2"), (List<Integer> list) -> {
            if (list.get(0) == 7000) {
                return null;
            }
            return list.get(0) + list.get(1);
        });
        Runner<Integer> myRunner = new RunnerImpl<>();
        try {
            final int iterators = 10000;
            Map<String, List<Integer>> expected = new HashMap<>();
            List<Integer> list1 = new ArrayList<>(iterators);
            List<Integer> list2 = new ArrayList<>(iterators);
            List<Integer> list3 = new ArrayList<>(iterators);
            for (int i = 0; i < 7000; ++i) {
                list1.add(i);
                list2.add(0);
                list3.add(i);
            }
            expected.put("1", list1);
            expected.put("2", list2);
            expected.put("3", list3);
            assertEquals(expected, myRunner.runProcessors(Set.of(fst, snd, trd), 3, iterators));
        } catch (ProcessorException e) {
            fail();
        }
    }

    @Test(timeout = 10_000)
    public void exception_test() {
        Processor<Integer> fst = new ThrowProcessor<>("1", List.of());
        Processor<Integer> snd = new SimpleProcessor<>("2", List.of("1"), (List<Integer> list) -> {
            return 0;
        });
        Processor<Integer> trd = new SimpleProcessor<>("3", List.of("1", "2"), (List<Integer> list) -> {
//            System.out.println("3 -> " + list.get(0));
            return list.get(0) + list.get(1);
        });
        Runner<Integer> myRunner = new RunnerImpl<>();
        try {
            final int iterators = 10000;
            myRunner.runProcessors(Set.of(fst, snd, trd), 3, iterators);
        } catch (ProcessorException e) {
            assertEquals("Hi", e.getMessage());
        }
    }

    @Test(timeout = 10_000)
    public void exec_test() {
        final int iterators = 10;
        Integer[] mas = new Integer[1];
        mas[0] = 0;
        Processor<Integer> fst = new SimpleProcessor<>("1", List.of(), (List<Integer> list) -> {
            try {
                sleep(330, 0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return ++mas[0];
        });
        Processor<Integer> snd = new SimpleProcessor<>("2", List.of("1"), (List<Integer> list) -> {
            int num = 0;
            for (int i = 0; i < list.get(0); i++) {
                num += i;
            }
            try {
                sleep(330, 0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return num;
        });
        Processor<Integer> trd = new SimpleProcessor<>("3", List.of("1", "2"), (List<Integer> list) -> {
            try {
                sleep(330, 0);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            return list.get(0) + list.get(1);
        });
        Runner<Integer> myRunner = new RunnerImpl<>();
        try {
            Map<String, List<Integer>> expected = new HashMap<>();
            List<Integer> list1 = new ArrayList<>(iterators);
            List<Integer> list2 = new ArrayList<>(iterators);
            List<Integer> list3 = new ArrayList<>(iterators);
            for (int i = 1; i <= iterators; ++i) {
                list1.add(i);
                list2.add(i * (i - 1) / 2);
                list3.add(i + i * (i - 1) / 2);
            }
            expected.put("1", list1);
            expected.put("2", list2);
            expected.put("3", list3);
            assertEquals(expected, myRunner.runProcessors(Set.of(fst, snd, trd), 3, iterators));
        } catch (ProcessorException e) {
            fail();
        }
    }

}
