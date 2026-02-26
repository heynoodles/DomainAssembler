```
  public static void main(String[] args) {

        test1_fetchDetailPage_serial();
        test2_fetchListPage_parallel();
         test3_fetchListPage_batch();
    }

    private static void test3_fetchListPage_batch() {

        OrderService orderService = new OrderServiceImpl();
        UserService userService = new UserServiceImpl();
        ProductService productService = new ProductServiceImpl();

        Map<Long, Order> orders = orderService.getOrdersByIds(Set.of(123L, 456L));

        List<OrderVO> orderVO = DomainAssembler.on(orders.values().stream().toList())
                .fetch(User.class, from(Order.class, Order::getUserId), userService::getUsersByIds, 10)
//                .fetchManySerial(Product.class, from(Order.class, Order::getProductIds), productService::getProductById)
                .fetchMany(Product.class, from(Order.class, Order::getProductIds), productService::getProductsByIds, 10)
                .assemble((order, ctx) -> {
                    OrderVO result = new OrderVO();
                    result.setId(order.getId());
                    result.setAmount(order.getAmount());
                    result.setUser(ctx.get(User.class));
                    result.setProducts(ctx.getList(Product.class));
                    return result;
                });

        System.out.println(orderVO);
    }


    private static void test2_fetchListPage_parallel() {
        OrderService orderService = new OrderServiceImpl();
        UserService userService = new UserServiceImpl();
        ProductService productService = new ProductServiceImpl();

        Map<Long, Order> orders = orderService.getOrdersByIds(Set.of(123L, 456L));

        List<OrderVO> orderVO = DomainAssembler.on(orders.values().stream().toList())
                .fetchParallel(User.class, from(Order.class, Order::getUserId), userService::getUserById)
//                .fetchManySerial(Product.class, from(Order.class, Order::getProductIds), productService::getProductById)
                .fetchManyParallel(Product.class, from(Order.class, Order::getProductIds), productService::getProductById)
                .assemble((order, ctx) -> {
                    OrderVO result = new OrderVO();
                    result.setId(order.getId());
                    result.setAmount(order.getAmount());
                    result.setUser(ctx.get(User.class));
                    result.setProducts(ctx.getList(Product.class));
                    return result;
                });

        System.out.println(orderVO);
    }

    private static void test1_fetchDetailPage_serial() {

        OrderService orderService = new OrderServiceImpl();
        UserService userService = new UserServiceImpl();
        ProductService productService = new ProductServiceImpl();

        OrderVO orderVO = DomainAssembler.on(123L)
                .fetchSerial(Order.class, (id, ctx) -> id, orderService::getOrderById)
                .fetchSerial(User.class, from(Order.class, Order::getUserId), userService::getUserById)
                .fetchManySerial(Product.class, from(Order.class, Order::getProductIds), productService::getProductById)
                .assemble((id, ctx) -> {
                    OrderVO result = new OrderVO();
                    result.setId(id);
                    result.setAmount(ctx.get(Order.class).getAmount());
                    result.setUser(ctx.get(User.class));
                    result.setProducts(ctx.getList(Product.class));
                    return result;
                });

        System.out.println(orderVO);
    }

```
