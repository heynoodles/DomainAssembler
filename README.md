# DomainAssembler

`DomainAssembler` is a powerful and flexible utility class designed to simplify the process of assembling complex domain objects by fetching related data from various sources. It supports serial, parallel, and batch data fetching strategies, making it suitable for high-performance applications.

## Key Features

*   **Fluent API**: Easy-to-read, chainable methods for defining data fetching steps.
*   **Flexible Fetching Strategies**:
    *   **Serial**: Fetches data one by one (useful for simple or dependent fetches).
    *   **Parallel**: Fetches data concurrently using a `ForkJoinPool` or a custom `Executor`.
    *   **Batch**: Fetches data in batches to minimize network round-trips (e.g., database queries).
*   **Context Management**: Maintains a `DataContext` for each input object, allowing intermediate results to be stored and reused.
*   **Support for Single and List Inputs**: seamlessly handles both individual objects and collections of objects.
*   **One-to-One & One-to-Many**: Supports fetching single related entities or collections of entities.

## Usage

### 1. Single Object Assembly

Use `DomainAssembler.on(input)` to start assembling a single object.

```java
User user = ...;

UserDTO userDTO = DomainAssembler.on(user)
    // Fetch UserProfile by userId
    .fetchSerial(UserProfile.class, User::getId, userProfileService::findById)
    // Fetch UserSettings in parallel
    .fetchParallel(UserSettings.class, User::getId, userSettingsService::findByUserId)
    // Assemble the final DTO
    .assemble((u, ctx) -> {
        UserProfile profile = ctx.get(UserProfile.class);
        UserSettings settings = ctx.get(UserSettings.class);
        return new UserDTO(u, profile, settings);
    });
```

### 2. List Assembly

Use `DomainAssembler.on(inputs)` to assemble a list of objects. This is where batch fetching shines.

```java
List<Order> orders = ...;

List<OrderDTO> orderDTOs = DomainAssembler.on(orders)
    // Batch fetch Products for all orders
    .fetch(Product.class, Order::getProductId, productService::findByIds, 100)
    // Batch fetch Customer details
    .fetch(Customer.class, Order::getCustomerId, customerService::findByIds, 50)
    // Assemble the list of DTOs
    .assemble((order, ctx) -> {
        Product product = ctx.get(Product.class);
        Customer customer = ctx.get(Customer.class);
        return new OrderDTO(order, product, customer);
    });
```

### 3. Fetching Strategies

#### One-to-One (fetch)

*   `fetchSerial`: Fetches data one by one.
*   `fetchParallel`: Fetches data concurrently.
*   `fetch`: Fetches data in batches.

#### One-to-Many (fetchMany)

Used when one input key maps to a collection of values (e.g., `User -> List<Role>`).

```java
DomainAssembler.on(users)
    .fetchMany(Role.class, User::getId, roleService::findRolesByUserIds, 100)
    .assemble((user, ctx) -> {
        List<Role> roles = ctx.getList(Role.class);
        return new UserDTO(user, roles);
    });
```

#### One-to-Many by Keys (fetchManyByKeys)

Used when the input contains a collection of keys (e.g., `Order -> List<OrderItemId> -> List<OrderItem>`).

```java
DomainAssembler.on(orders)
    .fetchManyByKeys(OrderItem.class, Order::getOrderItemIds, orderItemService::findByIds, 100)
    .assemble((order, ctx) -> {
        List<OrderItem> items = ctx.getList(OrderItem.class);
        return new OrderDTO(order, items);
    });
```

## DataContext

The `DataContext` holds the fetched data for each input object.

*   `ctx.get(Class<T> clazz)`: Retrieves a single object of the specified type.
*   `ctx.getList(Class<T> clazz)`: Retrieves a list of objects of the specified type.
*   `ctx.put(Class<?> clazz, T value)`: Manually puts data into the context (mostly used internally).

## Custom Executor

You can provide a custom `Executor` for parallel operations:

```java
DomainAssembler.on(inputs)
    .withExecutor(myCustomExecutor)
    .fetchParallel(...)
    .assemble(...);
```
