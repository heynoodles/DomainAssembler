package org.example.domainassemble.com.noodles;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.ForkJoinPool;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * A utility class for assembling domain objects by fetching related data in various ways (serial, parallel, batch).
 * It supports both single input and list of inputs.
 *
 * @param <I> The type of the input object(s).
 * @param <S> The type of the concrete DomainAssembler subclass (for method chaining).
 */
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

    /**
     * Creates a SingleAssembler for a single input object.
     *
     * @param input The input object.
     * @param <I>   The type of the input object.
     * @return A SingleAssembler instance.
     */
    public static <I> SingleAssembler<I> on(I input) {
        return new SingleAssembler<>(input);
    }

    /**
     * Creates a ListAssembler for a list of input objects.
     *
     * @param inputs The list of input objects.
     * @param <I>    The type of the input objects.
     * @return A ListAssembler instance.
     */
    public static <I> ListAssembler<I> on(List<I> inputs) {
        return new ListAssembler<>(inputs);
    }

    /**
     * Sets the Executor to be used for parallel operations.
     * Default is ForkJoinPool.commonPool().
     *
     * @param executor The Executor to use.
     * @return This assembler instance.
     */
    @SuppressWarnings("unchecked")
    public S withExecutor(Executor executor) {
        this.executor = executor;
        return (S) this;
    }

    /**
     * Helper method to create a BiFunction that retrieves an entity from the DataContext and applies a mapper function.
     *
     * @param clazz  The class of the entity to retrieve from the context.
     * @param mapper The function to apply to the retrieved entity.
     * @param <I>    The input type.
     * @param <T_DOMAIN> The type of the entity in the context.
     * @param <R>    The result type.
     * @return A BiFunction that takes the input and DataContext, and returns the mapped result.
     */
    public static <I, T_DOMAIN, R> BiFunction<I, DataContext, R> from(Class<T_DOMAIN> clazz, Function<T_DOMAIN, R> mapper) {
        return (input, ctx) -> {
            T_DOMAIN entity = ctx.get(clazz);
            return entity != null ? mapper.apply(entity) : null;
        };
    }

    // ================== 1. One-to-One (fetch) ==================

    /**
     * Fetches data serially (one by one) based on a key derived from the input and context.
     *
     * @param clazz The class of the data to be fetched.
     * @param idG   A BiFunction to generate the key from the input and DataContext.
     * @param f     A Function to fetch the data using the key.
     * @param <K>   The type of the key.
     * @param <V>   The type of the data to fetch.
     * @return This assembler instance.
     */
    public <K, V> S fetchSerial(Class<V> clazz, BiFunction<I, DataContext, K> idG, Function<K, V> f) {
        Set<K> ids = inputs.stream()
                .map(i -> idG.apply(i, contextMap.get(i)))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        Map<K, V> res = new HashMap<>();
        ids.forEach(id -> res.put(id, f.apply(id)));
        return fill(clazz, idG, res);
    }

    /**
     * Fetches data serially (one by one) based on a key derived from the input.
     *
     * @param clazz The class of the data to be fetched.
     * @param idG   A Function to generate the key from the input.
     * @param f     A Function to fetch the data using the key.
     * @param <K>   The type of the key.
     * @param <V>   The type of the data to fetch.
     * @return This assembler instance.
     */
    public <K, V> S fetchSerial(Class<V> clazz, Function<I, K> idG, Function<K, V> f) {
        return fetchSerial(clazz, (i, ctx) -> idG.apply(i), f);
    }

    /**
     * Fetches data in parallel based on a key derived from the input and context.
     *
     * @param clazz The class of the data to be fetched.
     * @param idG   A BiFunction to generate the key from the input and DataContext.
     * @param f     A Function to fetch the data using the key.
     * @param <K>   The type of the key.
     * @param <V>   The type of the data to fetch.
     * @return This assembler instance.
     */
    public <K, V> S fetchParallel(Class<V> clazz, BiFunction<I, DataContext, K> idG, Function<K, V> f) {
        Set<K> ids = inputs.stream()
                .map(i -> idG.apply(i, contextMap.get(i)))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
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

    /**
     * Fetches data in parallel based on a key derived from the input.
     *
     * @param clazz The class of the data to be fetched.
     * @param idG   A Function to generate the key from the input.
     * @param f     A Function to fetch the data using the key.
     * @param <K>   The type of the key.
     * @param <V>   The type of the data to fetch.
     * @return This assembler instance.
     */
    public <K, V> S fetchParallel(Class<V> clazz, Function<I, K> idG, Function<K, V> f) {
        return fetchParallel(clazz, (i, ctx) -> idG.apply(i), f);
    }

    /**
     * Fetches data in batches based on a key derived from the input and context.
     *
     * @param clazz  The class of the data to be fetched.
     * @param idG    A BiFunction to generate the key from the input and DataContext.
     * @param batchF A Function to fetch a map of data given a set of keys.
     * @param size   The batch size.
     * @param <K>    The type of the key.
     * @param <V>    The type of the data to fetch.
     * @return This assembler instance.
     */
    public <K, V> S fetch(Class<V> clazz, BiFunction<I, DataContext, K> idG, Function<Set<K>, Map<K, V>> batchF, int size) {
        Set<K> ids = inputs.stream()
                .map(i -> idG.apply(i, contextMap.get(i)))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        Map<K, V> total = new HashMap<>();
        List<K> list = new ArrayList<>(ids);
        for (int i = 0; i < list.size(); i += size) {
            total.putAll(batchF.apply(new HashSet<>(list.subList(i, Math.min(i + size, list.size())))));
        }
        return fill(clazz, idG, total);
    }

    /**
     * Fetches data in batches based on a key derived from the input.
     *
     * @param clazz  The class of the data to be fetched.
     * @param idG    A Function to generate the key from the input.
     * @param batchF A Function to fetch a map of data given a set of keys.
     * @param size   The batch size.
     * @param <K>    The type of the key.
     * @param <V>    The type of the data to fetch.
     * @return This assembler instance.
     */
    public <K, V> S fetch(Class<V> clazz, Function<I, K> idG, Function<Set<K>, Map<K, V>> batchF, int size) {
        return fetch(clazz, (i, ctx) -> idG.apply(i), batchF, size);
    }

    // ================== 2. One-to-Many (fetchMany) ==================
    // Input 1 Key -> Output N Values (List)

    /**
     * Fetches a collection of data serially (one by one) for a single key derived from the input and context.
     *
     * @param clazz   The class of the data items in the collection.
     * @param idG     A BiFunction to generate the key from the input and DataContext.
     * @param fetcher A Function to fetch the collection of data using the key.
     * @param <K>     The type of the key.
     * @param <V>     The type of the data items.
     * @return This assembler instance.
     */
    public <K, V> S fetchManySerial(Class<V> clazz, BiFunction<I, DataContext, K> idG, Function<K, ? extends Collection<V>> fetcher) {
        Set<K> ids = inputs.stream()
                .map(i -> idG.apply(i, contextMap.get(i)))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        Map<K, Collection<V>> res = new HashMap<>();
        ids.forEach(id -> {
            Collection<V> v = fetcher.apply(id);
            if (v != null) res.put(id, v);
        });
        return fillMany(clazz, idG, res);
    }

    /**
     * Fetches a collection of data serially (one by one) for a single key derived from the input.
     *
     * @param clazz   The class of the data items in the collection.
     * @param idG     A Function to generate the key from the input.
     * @param fetcher A Function to fetch the collection of data using the key.
     * @param <K>     The type of the key.
     * @param <V>     The type of the data items.
     * @return This assembler instance.
     */
    public <K, V> S fetchManySerial(Class<V> clazz, Function<I, K> idG, Function<K, ? extends Collection<V>> fetcher) {
        return fetchManySerial(clazz, (i, ctx) -> idG.apply(i), fetcher);
    }

    /**
     * Fetches a collection of data in parallel for a single key derived from the input and context.
     *
     * @param clazz   The class of the data items in the collection.
     * @param idG     A BiFunction to generate the key from the input and DataContext.
     * @param fetcher A Function to fetch the collection of data using the key.
     * @param <K>     The type of the key.
     * @param <V>     The type of the data items.
     * @return This assembler instance.
     */
    public <K, V> S fetchManyParallel(Class<V> clazz, BiFunction<I, DataContext, K> idG, Function<K, ? extends Collection<V>> fetcher) {
        Set<K> ids = inputs.stream()
                .map(i -> idG.apply(i, contextMap.get(i)))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        Map<K, Collection<V>> res = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> fs = ids.stream()
                .map(id -> CompletableFuture.runAsync(() -> {
                    Collection<V> v = fetcher.apply(id);
                    if (v != null) res.put(id, v);
                }, executor))
                .collect(Collectors.toList());
        CompletableFuture.allOf(fs.toArray(new CompletableFuture[0])).join();
        return fillMany(clazz, idG, res);
    }

    /**
     * Fetches a collection of data in parallel for a single key derived from the input.
     *
     * @param clazz   The class of the data items in the collection.
     * @param idG     A Function to generate the key from the input.
     * @param fetcher A Function to fetch the collection of data using the key.
     * @param <K>     The type of the key.
     * @param <V>     The type of the data items.
     * @return This assembler instance.
     */
    public <K, V> S fetchManyParallel(Class<V> clazz, Function<I, K> idG, Function<K, ? extends Collection<V>> fetcher) {
        return fetchManyParallel(clazz, (i, ctx) -> idG.apply(i), fetcher);
    }

    /**
     * Fetches a collection of data in batches for a single key derived from the input and context.
     *
     * @param clazz  The class of the data items in the collection.
     * @param idG    A BiFunction to generate the key from the input and DataContext.
     * @param batchF A Function to fetch a map of collections given a set of keys.
     * @param size   The batch size.
     * @param <K>    The type of the key.
     * @param <V>    The type of the data items.
     * @return This assembler instance.
     */
    public <K, V> S fetchMany(Class<V> clazz, BiFunction<I, DataContext, K> idG, Function<Set<K>, Map<K, ? extends Collection<V>>> batchF, int size) {
        Set<K> ids = inputs.stream()
                .map(i -> idG.apply(i, contextMap.get(i)))
                .filter(Objects::nonNull)
                .collect(Collectors.toSet());
        Map<K, Collection<V>> total = new HashMap<>();
        List<K> list = new ArrayList<>(ids);
        for (int i = 0; i < list.size(); i += size) {
            Map<K, ? extends Collection<V>> batchRes = batchF.apply(new HashSet<>(list.subList(i, Math.min(i + size, list.size()))));
            if (batchRes != null) {
                total.putAll(batchRes);
            }
        }
        return fillMany(clazz, idG, total);
    }

    /**
     * Fetches a collection of data in batches for a single key derived from the input.
     *
     * @param clazz  The class of the data items in the collection.
     * @param idG    A Function to generate the key from the input.
     * @param batchF A Function to fetch a map of collections given a set of keys.
     * @param size   The batch size.
     * @param <K>    The type of the key.
     * @param <V>    The type of the data items.
     * @return This assembler instance.
     */
    public <K, V> S fetchMany(Class<V> clazz, Function<I, K> idG, Function<Set<K>, Map<K, ? extends Collection<V>>> batchF, int size) {
        return fetchMany(clazz, (i, ctx) -> idG.apply(i), batchF, size);
    }

    // ================== 3. One-to-Many (fetchManyByKeys) ==================
    // Input N Keys -> Output N Values (List)

    /**
     * Fetches data serially (one by one) for multiple keys derived from the input and context.
     *
     * @param clazz   The class of the data to be fetched.
     * @param idsG    A BiFunction to generate a collection of keys from the input and DataContext.
     * @param fetcher A Function to fetch the data using a single key.
     * @param <K>     The type of the key.
     * @param <V>     The type of the data to fetch.
     * @return This assembler instance.
     */
    public <K, V> S fetchManyByKeysSerial(Class<V> clazz, BiFunction<I, DataContext, Collection<K>> idsG, Function<K, V> fetcher) {
        Set<K> allIds = inputs.stream()
                .map(i -> idsG.apply(i, contextMap.get(i)))
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        Map<K, V> total = new HashMap<>();
        allIds.forEach(id -> total.put(id, fetcher.apply(id)));
        return fillManyByKeys(clazz, idsG, total);
    }

    /**
     * Fetches data serially (one by one) for multiple keys derived from the input.
     *
     * @param clazz   The class of the data to be fetched.
     * @param idsG    A Function to generate a collection of keys from the input.
     * @param fetcher A Function to fetch the data using a single key.
     * @param <K>     The type of the key.
     * @param <V>     The type of the data to fetch.
     * @return This assembler instance.
     */
    public <K, V> S fetchManyByKeysSerial(Class<V> clazz, Function<I, Collection<K>> idsG, Function<K, V> fetcher) {
        return fetchManyByKeysSerial(clazz, (i, ctx) -> idsG.apply(i), fetcher);
    }

    /**
     * Fetches data in parallel for multiple keys derived from the input and context.
     *
     * @param clazz   The class of the data to be fetched.
     * @param idsG    A BiFunction to generate a collection of keys from the input and DataContext.
     * @param fetcher A Function to fetch the data using a single key.
     * @param <K>     The type of the key.
     * @param <V>     The type of the data to fetch.
     * @return This assembler instance.
     */
    public <K, V> S fetchManyByKeysParallel(Class<V> clazz, BiFunction<I, DataContext, Collection<K>> idsG, Function<K, V> fetcher) {
        Set<K> allIds = inputs.stream()
                .map(i -> idsG.apply(i, contextMap.get(i)))
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        Map<K, V> total = new ConcurrentHashMap<>();
        List<CompletableFuture<Void>> fs = allIds.stream()
                .map(id -> CompletableFuture.runAsync(() -> {
                    V v = fetcher.apply(id);
                    if (v != null) total.put(id, v);
                }, executor))
                .collect(Collectors.toList());
        CompletableFuture.allOf(fs.toArray(new CompletableFuture[0])).join();
        return fillManyByKeys(clazz, idsG, total);
    }

    /**
     * Fetches data in parallel for multiple keys derived from the input.
     *
     * @param clazz   The class of the data to be fetched.
     * @param idsG    A Function to generate a collection of keys from the input.
     * @param fetcher A Function to fetch the data using a single key.
     * @param <K>     The type of the key.
     * @param <V>     The type of the data to fetch.
     * @return This assembler instance.
     */
    public <K, V> S fetchManyByKeysParallel(Class<V> clazz, Function<I, Collection<K>> idsG, Function<K, V> fetcher) {
        return fetchManyByKeysParallel(clazz, (i, ctx) -> idsG.apply(i), fetcher);
    }

    /**
     * Fetches data in batches for multiple keys derived from the input and context.
     *
     * @param clazz  The class of the data to be fetched.
     * @param idsG   A BiFunction to generate a collection of keys from the input and DataContext.
     * @param batchF A Function to fetch a map of data given a set of keys.
     * @param size   The batch size.
     * @param <K>    The type of the key.
     * @param <V>    The type of the data to fetch.
     * @return This assembler instance.
     */
    public <K, V> S fetchManyByKeys(Class<V> clazz, BiFunction<I, DataContext, Collection<K>> idsG, Function<Set<K>, Map<K, V>> batchF, int size) {
        Set<K> allIds = inputs.stream()
                .map(i -> idsG.apply(i, contextMap.get(i)))
                .filter(Objects::nonNull)
                .flatMap(Collection::stream)
                .collect(Collectors.toSet());
        Map<K, V> total = new HashMap<>();
        List<K> list = new ArrayList<>(allIds);
        for (int i = 0; i < list.size(); i += size) {
            total.putAll(batchF.apply(new HashSet<>(list.subList(i, Math.min(i + size, list.size())))));
        }
        return fillManyByKeys(clazz, idsG, total);
    }

    /**
     * Fetches data in batches for multiple keys derived from the input.
     *
     * @param clazz  The class of the data to be fetched.
     * @param idsG   A Function to generate a collection of keys from the input.
     * @param batchF A Function to fetch a map of data given a set of keys.
     * @param size   The batch size.
     * @param <K>    The type of the key.
     * @param <V>    The type of the data to fetch.
     * @return This assembler instance.
     */
    public <K, V> S fetchManyByKeys(Class<V> clazz, Function<I, Collection<K>> idsG, Function<Set<K>, Map<K, V>> batchF, int size) {
        return fetchManyByKeys(clazz, (i, ctx) -> idsG.apply(i), batchF, size);
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
    private <K, V> S fillMany(Class<V> clz, BiFunction<I, DataContext, K> idG, Map<K, ? extends Collection<V>> data) {
        inputs.forEach(i -> {
            K k = idG.apply(i, contextMap.get(i));
            if (k != null) {
                Collection<V> vals = data.get(k);
                if (vals != null) {
                    contextMap.get(i).putList(clz, new ArrayList<>(vals));
                }
            }
        });
        return (S) this;
    }

    @SuppressWarnings("unchecked")
    private <K, V> S fillManyByKeys(Class<V> clz, BiFunction<I, DataContext, Collection<K>> idsG, Map<K, V> data) {
        inputs.forEach(i -> {
            Collection<K> keys = idsG.apply(i, contextMap.get(i));
            if (keys != null) {
                List<V> vals = keys.stream()
                        .map(data::get)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toList());
                contextMap.get(i).putList(clz, vals);
            }
        });
        return (S) this;
    }

    /**
     * Assembler for a single input object.
     *
     * @param <I> The type of the input object.
     */
    public static class SingleAssembler<I> extends DomainAssembler<I, SingleAssembler<I>> {
        protected SingleAssembler(I input) {
            super(Collections.singletonList(input));
        }

        /**
         * Assembles the final result using the input and the populated DataContext.
         *
         * @param ass A BiFunction to construct the result from the input and DataContext.
         * @param <O> The type of the result.
         * @return The assembled result.
         */
        public <O> O assemble(BiFunction<I, DataContext, O> ass) {
            return ass.apply(inputs.get(0), contextMap.get(inputs.get(0)));
        }
    }

    /**
     * Assembler for a list of input objects.
     *
     * @param <I> The type of the input objects.
     */
    public static class ListAssembler<I> extends DomainAssembler<I, ListAssembler<I>> {
        protected ListAssembler(List<I> inputs) {
            super(inputs);
        }

        /**
         * Assembles the final results using the inputs and their populated DataContexts.
         *
         * @param ass A BiFunction to construct the result from an input and its DataContext.
         * @param <O> The type of the result.
         * @return A list of assembled results.
         */
        public <O> List<O> assemble(BiFunction<I, DataContext, O> ass) {
            return inputs.stream()
                    .map(i -> ass.apply(i, contextMap.get(i)))
                    .collect(Collectors.toList());
        }
    }

    /**
     * Holds the fetched data for a specific input object.
     */
    public static class DataContext {
        private final Map<Class<?>, Object> data = new HashMap<>();
        private final Map<Class<?>, List<?>> listData = new HashMap<>();

        /**
         * Puts a value into the context.
         *
         * @param k   The class key.
         * @param v   The value.
         * @param <T> The type of the value.
         */
        public <T> void put(Class<?> k, T v) {
            data.put(k, v);
        }

        /**
         * Gets a value from the context.
         *
         * @param k   The class key.
         * @param <T> The type of the value.
         * @return The value, or null if not found.
         */
        public <T> T get(Class<T> k) {
            return k.cast(data.get(k));
        }

        /**
         * Puts a list of values into the context.
         *
         * @param k   The class key.
         * @param v   The list of values.
         * @param <T> The type of the values in the list.
         */
        public <T> void putList(Class<?> k, List<T> v) {
            listData.put(k, v);
        }

        /**
         * Gets a list of values from the context.
         *
         * @param k   The class key.
         * @param <T> The type of the values in the list.
         * @return The list of values, or an empty list if not found.
         */
        @SuppressWarnings("unchecked")
        public <T> List<T> getList(Class<T> k) {
            return (List<T>) listData.getOrDefault(k, Collections.emptyList());
        }
    }
}