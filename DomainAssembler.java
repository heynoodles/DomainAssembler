package org.example.domainassemble;

import java.util.*;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.function.*;
import java.util.stream.Collectors;

public abstract class DomainAssembler<I, S extends DomainAssembler<I, S>> {
    protected final List<I> inputs;
    protected final Map<I, DataContext> contextMap = new IdentityHashMap<>();
    protected Executor executor = ForkJoinPool.commonPool();

    protected DomainAssembler(List<I> inputs) {
        this.inputs = inputs;
        inputs.forEach(i -> {
            DataContext ctx = new DataContext();
            if (i != null) ctx.put(i.getClass(), i);
            contextMap.put(i, ctx);
        });
    }

    public static <I> SingleAssembler<I> on(I input) {
        return new SingleAssembler<>(input);
    }

    public static <I> ListAssembler<I> on(List<I> inputs) {
        return new ListAssembler<>(inputs);
    }

    @SuppressWarnings("unchecked")
    public S withExecutor(Executor executor) {
        this.executor = executor;
        return (S) this;
    }

    public static <I, T_DOMAIN, R> BiFunction<I, DataContext, R> from(Class<T_DOMAIN> clazz, Function<T_DOMAIN, R> mapper) {
        return (input, ctx) -> {
            T_DOMAIN entity = ctx.get(clazz);
            return entity != null ? mapper.apply(entity) : null;
        };
    }

    // ================== 1. One-to-One (fetch) ==================

    public <K, V> S fetchSerial(Class<V> clazz, BiFunction<I, DataContext, K> idG, Function<K, V> f) {
        Set<K> ids = inputs.stream().map(i -> idG.apply(i, contextMap.get(i))).filter(Objects::nonNull).collect(Collectors.toSet());
        Map<K, V> res = new HashMap<>();
        ids.forEach(id -> res.put(id, f.apply(id)));
        return fill(clazz, idG, res);
    }

    public <K, V> S fetchSerial(Class<V> clazz, Function<I, K> idG, Function<K, V> f) {
        return fetchSerial(clazz, (i, ctx) -> idG.apply(i), f);
    }

    public <K, V> S fetchParallel(Class<V> clazz, BiFunction<I, DataContext, K> idG, Function<K, V> f) {
        Set<K> ids = inputs.stream().map(i -> idG.apply(i, contextMap.get(i))).filter(Objects::nonNull).collect(Collectors.toSet());
        Map<K, V> res = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> fs = ids.stream()
                .map(id -> CompletableFuture.runAsync(() -> {
                    V v = f.apply(id);
                    if (v != null) res.put(id, v);
                }, executor))
                .collect(Collectors.toList());
        CompletableFuture.allOf(fs.toArray(new CompletableFuture[0])).join();
        return fill(clazz, idG, res);
    }

    public <K, V> S fetchParallel(Class<V> clazz, Function<I, K> idG, Function<K, V> f) {
        return fetchParallel(clazz, (i, ctx) -> idG.apply(i), f);
    }

    public <K, V> S fetch(Class<V> clazz, BiFunction<I, DataContext, K> idG, Function<Set<K>, Map<K, V>> batchF, int size) {
        Set<K> ids = inputs.stream().map(i -> idG.apply(i, contextMap.get(i))).filter(Objects::nonNull).collect(Collectors.toSet());
        Map<K, V> total = new HashMap<>();
        List<K> list = new ArrayList<>(ids);
        for (int i = 0; i < list.size(); i += size) {
            total.putAll(batchF.apply(new HashSet<>(list.subList(i, Math.min(i + size, list.size())))));
        }
        return fill(clazz, idG, total);
    }

    public <K, V> S fetch(Class<V> clazz, Function<I, K> idG, Function<Set<K>, Map<K, V>> batchF, int size) {
        return fetch(clazz, (i, ctx) -> idG.apply(i), batchF, size);
    }

    // ================== 2. One-to-Many (fetchMany) ==================

    /**
     * 2.1 One-to-Many 单量接口：串行调用 (针对每个 ID 调用 fetcher 并聚合成 List)
     */
    public <K, V> S fetchManySerial(Class<V> clazz, BiFunction<I, DataContext, Collection<K>> idsG, Function<K, V> fetcher) {
        Set<K> allIds = inputs.stream().map(i -> idsG.apply(i, contextMap.get(i))).filter(Objects::nonNull).flatMap(Collection::stream).collect(Collectors.toSet());
        Map<K, V> total = new HashMap<>();
        allIds.forEach(id -> total.put(id, fetcher.apply(id)));
        return fillMany(clazz, idsG, total);
    }

    public <K, V> S fetchManySerial(Class<V> clazz, Function<I, Collection<K>> idsG, Function<K, V> fetcher) {
        return fetchManySerial(clazz, (i, ctx) -> idsG.apply(i), fetcher);
    }

    /**
     * 2.2 One-to-Many 单量接口：并发调用
     */
    public <K, V> S fetchManyParallel(Class<V> clazz, BiFunction<I, DataContext, Collection<K>> idsG, Function<K, V> fetcher) {
        Set<K> allIds = inputs.stream().map(i -> idsG.apply(i, contextMap.get(i))).filter(Objects::nonNull).flatMap(Collection::stream).collect(Collectors.toSet());
        Map<K, V> total = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> fs = allIds.stream()
                .map(id -> CompletableFuture.runAsync(() -> {
                    V v = fetcher.apply(id);
                    if (v != null) total.put(id, v);
                }, executor))
                .collect(Collectors.toList());
        CompletableFuture.allOf(fs.toArray(new CompletableFuture[0])).join();
        return fillMany(clazz, idsG, total);
    }

    public <K, V> S fetchManyParallel(Class<V> clazz, Function<I, Collection<K>> idsG, Function<K, V> fetcher) {
        return fetchManyParallel(clazz, (i, ctx) -> idsG.apply(i), fetcher);
    }

    /**
     * 2.3 One-to-Many 批量接口：分批调用
     */
    public <K, V> S fetchMany(Class<V> clazz, BiFunction<I, DataContext, Collection<K>> idsG, Function<Set<K>, Map<K, V>> batchF, int size) {
        Set<K> allIds = inputs.stream().map(i -> idsG.apply(i, contextMap.get(i))).filter(Objects::nonNull).flatMap(Collection::stream).collect(Collectors.toSet());
        Map<K, V> total = new HashMap<>();
        List<K> list = new ArrayList<>(allIds);
        for (int i = 0; i < list.size(); i += size) {
            total.putAll(batchF.apply(new HashSet<>(list.subList(i, Math.min(i + size, list.size())))));
        }
        return fillMany(clazz, idsG, total);
    }

    public <K, V> S fetchMany(Class<V> clazz, Function<I, Collection<K>> idsG, Function<Set<K>, Map<K, V>> batchF, int size) {
        return fetchMany(clazz, (i, ctx) -> idsG.apply(i), batchF, size);
    }

    @SuppressWarnings("unchecked")
    private <K, V> S fill(Class<V> clz, BiFunction<I, DataContext, K> idG, Map<K, V> data) {
        inputs.forEach(i -> {
            K k = idG.apply(i, contextMap.get(i));
            if (k != null) contextMap.get(i).put(clz, data.get(k));
        });
        return (S) this;
    }

    @SuppressWarnings("unchecked")
    private <K, V> S fillMany(Class<V> clz, BiFunction<I, DataContext, Collection<K>> idsG, Map<K, V> data) {
        inputs.forEach(i -> {
            Collection<K> keys = idsG.apply(i, contextMap.get(i));
            if (keys != null) {
                List<V> vals = keys.stream().map(data::get).filter(Objects::nonNull).collect(Collectors.toList());
                contextMap.get(i).putList(clz, vals);
            }
        });
        return (S) this;
    }

    public static class SingleAssembler<I> extends DomainAssembler<I, SingleAssembler<I>> {
        protected SingleAssembler(I input) {
            super(Collections.singletonList(input));
        }

        public <O> O assemble(BiFunction<I, DataContext, O> ass) {
            return ass.apply(inputs.get(0), contextMap.get(inputs.get(0)));
        }
    }

    public static class ListAssembler<I> extends DomainAssembler<I, ListAssembler<I>> {
        protected ListAssembler(List<I> inputs) {
            super(inputs);
        }

        public <O> List<O> assemble(BiFunction<I, DataContext, O> ass) {
            return inputs.stream().map(i -> ass.apply(i, contextMap.get(i))).collect(Collectors.toList());
        }
    }

    public static class DataContext {
        private final Map<Class<?>, Object> data = new HashMap<>();
        private final Map<Class<?>, List<?>> listData = new HashMap<>();

        public <T> void put(Class<?> k, T v) {
            data.put(k, v);
        }

        public <T> T get(Class<T> k) {
            return k.cast(data.get(k));
        }

        public <T> void putList(Class<?> k, List<T> v) {
            listData.put(k, v);
        }

        @SuppressWarnings("unchecked")
        public <T> List<T> getList(Class<T> k) {
            return (List<T>) listData.getOrDefault(k, Collections.emptyList());
        }
    }
}
