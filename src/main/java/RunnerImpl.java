import java.util.*;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

public class RunnerImpl<T> implements Runner<T> {
    private Set<Processor<T>> processors;
    private ArrayList<Node<T>> initials;
    private Map<String, Node<T>> nodes;
    private Queue<Pair<T>> processorsQueue;
    private AtomicInteger iterProcessesCounter;
    private int MAX_PAIRS_COUNT;
    private final AtomicInteger brokenIteration = new AtomicInteger(Integer.MAX_VALUE);
    private final AtomicInteger finishedProc = new AtomicInteger(0);
    private final AtomicReference<ProcessorException> throwed = new AtomicReference<>();

    RunnerImpl() {
    }

    private static class Node<T> {
        Processor<T> processor;
        int color = 0;
        AtomicInteger iterationsCounter = new AtomicInteger(-1);
        List<Node<T>> prev = new ArrayList<>();
        Set<Node<T>> next = new HashSet<>();
        AtomicReferenceArray<T> results;
        AtomicBoolean finished = new AtomicBoolean(false);
        Lock lock = new ReentrantLock();

        Node(Processor<T> processor, int iterationsNumber) {
            this.processor = processor;
            results = new AtomicReferenceArray<>(iterationsNumber);
        }
    }

    private static class Pair<T> {
        int iterationNumber;
        final Node<T> node;

        Pair(int iterationNumber, final Node<T> node) {
            this.iterationNumber = iterationNumber;
            this.node = node;
        }
    }

    private void createGraph(int iterationsNumber) throws ProcessorException {
        for (Processor<T> processor : processors) {
            Node<T> currNode = new Node<>(processor, iterationsNumber);
            if (!nodes.containsKey(processor.getId())) {
                if (processor.getInputIds().isEmpty()) {
                    initials.add(currNode);
                }
            } else {
                throw new ProcessorException("Repeating processors ids");
            }
            nodes.put(processor.getId(), currNode);
        }
        for (Processor<T> processor : processors) {
            Node<T> currNode = nodes.get(processor.getId());
            List<String> currProcPrev = processor.getInputIds();
            for (String s : currProcPrev) {
                if (!nodes.containsKey(s)) {
                    throw new ProcessorException("Not enough processors ids");
                } else {
                    Node<T> prevNode = nodes.get(s);
                    prevNode.next.add(currNode);
                    currNode.prev.add(prevNode);
                }
            }
        }
        for (Node<T> currNode : initials) {
            currNode.color = 1;
            checkCycles(currNode);
            currNode.color = 2;
        }
    }

    private void checkCycles(Node<T> currNode) throws ProcessorException {
        for (Node<T> node : currNode.next) {
            if (node.color == 0) {
                node.color = 1;
                checkCycles(node);
                node.color = 2;
            } else if (node.color == 1) {
                throw new ProcessorException("Cycles in processor's graph");
            }
        }
    }

    private Pair<T> checkNull(Pair<T> currPair) throws InterruptedException {
        while (currPair == null) {

            if (iterProcessesCounter.get() >= MAX_PAIRS_COUNT || finishedProc.get() == processors.size()) {
                return null;
            } else {
                while (processorsQueue.isEmpty() && (iterProcessesCounter.get() != MAX_PAIRS_COUNT || finishedProc.get() == processors.size())) {
                    Thread.sleep(2, 0);
                }
                currPair = processorsQueue.poll();
            }
        }
        if (iterProcessesCounter.get() >= MAX_PAIRS_COUNT || finishedProc.get() == processors.size()) {
            return null;
        }
        return currPair;
    }

    private boolean doJob() throws ProcessorException, InterruptedException {
        Pair<T> currPair = processorsQueue.poll();
        currPair = checkNull(currPair);
        if (currPair == null) {
            return true;
        }

        if (brokenIteration.get() != Integer.MAX_VALUE) {
            if (brokenIteration.get() == 0 || (currPair.node.results.get(brokenIteration.get() - 1) != null)) {
                if (currPair.node.finished.compareAndSet(false, true)) {
                    finishedProc.incrementAndGet();
                }
                return false;
            }
        }
        if (currPair.node.iterationsCounter.get() == currPair.iterationNumber - 1) {
            for (Node<T> pr : currPair.node.prev) {
                if (pr.results.get(currPair.iterationNumber) == null) {
                    processorsQueue.add(currPair);
                    return false;
                }
            }

            boolean state = currPair.node.lock.tryLock(10, TimeUnit.MILLISECONDS);
            if (!state) {
                processorsQueue.add(currPair);
                return false;
            }
            if (currPair.node.iterationsCounter.get() != currPair.iterationNumber - 1) {
                currPair.node.lock.unlock();
                return false;
            }
            currPair.node.iterationsCounter.addAndGet(1);
            List<T> input = new ArrayList<>(currPair.node.prev.size());
            for (Node<T> pr : currPair.node.prev) {
                input.add(pr.results.get(currPair.iterationNumber));
            }
            T result = currPair.node.processor.process(input);
            if (result == null) {
                int prevIter = brokenIteration.get();
                if (currPair.iterationNumber < prevIter) {
                    brokenIteration.compareAndSet(prevIter, currPair.iterationNumber);
                    if (currPair.node.finished.compareAndSet(false, true)) {
                        finishedProc.incrementAndGet();
                    }
                }
                currPair.node.lock.unlock();
                return false;
            }

            currPair.node.results.compareAndSet(currPair.iterationNumber, null, result);
            if (currPair.iterationNumber >= brokenIteration.get() - 1) {
                if (currPair.node.finished.compareAndSet(false, true)) {
                    finishedProc.incrementAndGet();
                }
            }
            for (Node<T> ne : currPair.node.next) {
                processorsQueue.add(new Pair<>(currPair.iterationNumber, ne));
            }
            if (currPair.iterationNumber != MAX_PAIRS_COUNT / processors.size() - 1){
                processorsQueue.add(new Pair<>(currPair.iterationNumber + 1, currPair.node));
            }
            iterProcessesCounter.addAndGet(1);
            currPair.node.lock.unlock();
        } else if (currPair.node.iterationsCounter.get() < currPair.iterationNumber) {
            processorsQueue.add(currPair);
            return false;
        }
        return false;
    }

    /**
     * Runs a set of interdependent processors many times until null is produced by any of them
     *
     * @param maxThreads    - maximum number of threads to be used
     * @param maxIterations - maximum number of iterations to run
     * @param processors    - a set of processors
     * @return a map, where the key is a processor id, and the value is a list of its outputs in the order of iterations
     * @throws ProcessorException if a processor throws an exception, loops detected, or some input ids not found
     */
    @Override
    public Map<String, List<T>> runProcessors(Set<Processor<T>> processors, int maxThreads, int maxIterations) throws ProcessorException {
        if (maxThreads <= 0 || maxIterations <= 0 || processors == null) {
            throw new ProcessorException("Illegal argument");
        }
        initials = new ArrayList<>();
        nodes = new HashMap<>();
        processorsQueue = new ConcurrentLinkedQueue<>();
        iterProcessesCounter = new AtomicInteger(0);
        List<Thread> threads = new ArrayList<>();
        this.processors = processors;
        createGraph(maxIterations);
        MAX_PAIRS_COUNT = processors.size() * maxIterations;
        for (Node<T> initElem : initials) {
            processorsQueue.add(new Pair<>(0, initElem));
        }
        for (int i = 0; i < maxThreads; i++) {
            threads.add(new Thread(() -> {
                try {
                    while (!Thread.interrupted()) {
                        boolean finish = doJob();
                        if (finish) {
                            break;
                        }
                    }
                } catch (ProcessorException e) {
                    throwed.compareAndSet(null, e);
                } catch (InterruptedException ignored) {
                } finally {
                    Thread.currentThread().interrupt();
                }
            }));
            threads.get(i).start();
        }


        while (iterProcessesCounter.get() < MAX_PAIRS_COUNT) {
            if (throwed.get() != null) {
                close(threads);
                throw throwed.get();
            }
            try {
                Thread.sleep(50, 0);
                if (brokenIteration.get() != Integer.MAX_VALUE) {
                    while (finishedProc.get() != processors.size()) {
                        Thread.sleep(1000, 0);
                        for (Processor<T> processor : processors) {
                            if ((brokenIteration.get() == 0) || (nodes.get(processor.getId()).results.get(brokenIteration.get() - 1) != null)) {
                                if (nodes.get(processor.getId()).finished.compareAndSet(false, true)) {
                                    finishedProc.incrementAndGet();
                                }
                            }
                        }
                    }
                    break;
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        close(threads);
        Map<String, List<T>> res = new HashMap<>();
        for (Processor<T> processor : processors) {
            List<T> currRes = new ArrayList<>();
            if (brokenIteration.get() == Integer.MAX_VALUE) {
                for (int i = 0; i < maxIterations; ++i) {
                    T resI = nodes.get(processor.getId()).results.get(i);
                    currRes.add(resI);
                }
            } else {
                for (int i = 0; i < brokenIteration.get(); ++i) {
                    T resI = nodes.get(processor.getId()).results.get(i);
                    currRes.add(resI);
                }
            }
            res.put(processor.getId(), currRes);
        }
        return res;
    }

    private void close(List<Thread> threads) {
        threads.forEach(Thread::interrupt);
        for (Thread thread : threads) {
            try {
                thread.join();
            } catch (InterruptedException ignored) {
            }
        }
    }
}

