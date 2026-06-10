# FlashDeal

## 本地 Docker 开发环境

本地开发依赖由 `docker-compose.dev.yml` 统一管理，不会修改或替代生产环境配置。

### 1. 安装并启动 Docker Desktop

确认 Docker Desktop 已运行：

```bash
docker version
docker compose version
```

### 2. 启动本地依赖

```bash
docker compose -f docker-compose.dev.yml up -d
docker ps
```

开发环境服务地址：

| 服务 | 地址 |
| --- | --- |
| MySQL | `localhost:3306`，数据库 `flashdeal`，本地账号 `root / 123456` |
| Redis | `localhost:6379`，无密码 |
| RabbitMQ | `localhost:5672`，本地账号 `guest / guest` |
| RabbitMQ 管理台 | `http://localhost:15672`，账号 `guest / guest` |

MySQL 首次创建 volume 时会自动导入 `src/main/resources/db/flashdeal.sql`。

如果本机 MySQL 服务已经占用 `3306`，请先停止该服务，或同步调整 Compose 端口和 local/test profile；否则 Docker MySQL 无法启动。

### 3. 使用 local profile 启动项目

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=local
```

### 4. 使用 test profile 运行测试

```bash
mvn test -q -Dspring.profiles.active=test
```

测试 profile 指向相同的本地 Docker 依赖，将 local/test 延迟队列 TTL 统一为 30 秒，并关闭后台可靠性任务和 RabbitMQ 监听器自动启动，避免测试期间产生无关重试、对账操作或队列参数冲突。现有 Redis / MySQL 集成测试运行前必须先启动依赖；真实 RabbitMQ 消费链路建议后续增加独立集成测试。

### 5. 重置本地开发环境

如果首次启动 MySQL 初始化失败，或 RabbitMQ 报历史队列参数冲突，可以重建开发环境：

```bash
docker compose -f docker-compose.dev.yml down -v
docker compose -f docker-compose.dev.yml up -d
```

`down -v` 会清空本地开发 MySQL、Redis、RabbitMQ 数据，仅适合开发环境。

项目使用 RabbitMQ 原生 TTL + DLX 实现延迟关单，不依赖 `x-delayed-message` 插件，因此 `rabbitmq:3.13-management` 即可运行。

FlashDeal 是一个基于 Spring Boot、Redis、RabbitMQ、MySQL 的高并发优惠券秒杀与可靠消息实践项目。项目在基础点评业务之上，重点强化了秒杀异步下单链路的最终一致性、消息可靠投递和订单超时关单可靠事件处理。

项目不是单纯堆功能，而是把常见后端高频问题落到代码、配置和脚本中：

- 高并发秒杀如何避免超卖和重复下单；
- Redis 预扣库存后如何异步落库；
- RabbitMQ 消息消费失败如何处理；
- 热点商户详情如何降低 Redis 和 MySQL 压力；
- 多实例本地缓存如何广播失效；
- 订单超时未支付如何自动关闭并回补库存；
- 通用接口限流如何通过注解接入。

## 技术栈

| 分类 | 技术 |
| --- | --- |
| 后端框架 | Spring Boot 2.3.12.RELEASE、Spring MVC |
| ORM | MyBatis-Plus |
| 数据库 | MySQL 8 |
| 缓存 | Redis 7、Caffeine |
| 消息队列 | RabbitMQ management |
| 并发控制 | Redis Lua、Redisson、数据库唯一索引、条件更新 |
| 限流 | Redis ZSet + Lua + AOP + 自定义注解 |
| 本地环境 | Docker Compose |
| 构建 | Maven、JDK 8 |

## 核心功能

### 1. Redis + Lua 秒杀预校验

秒杀入口：

```text
POST /voucher-order/seckill/{voucherId}
```

核心流程：

```text
用户请求秒杀接口
  -> 执行 Redis Lua 脚本
  -> 校验库存
  -> 校验一人一单
  -> Redis 预扣库存
  -> 写入用户下单标记
  -> 写 Redis pending / reservation
  -> Java 写入可靠消息表并发送 RabbitMQ 异步下单消息
```

代码证据：

- `src/main/resources/seckill.lua`
- `src/main/java/com/flashdeal/service/impl/VoucherOrderServiceImpl.java`
- `src/main/java/com/flashdeal/utils/RedisConstants.java`

### 2. RabbitMQ 异步下单

Lua 只负责秒杀资格判断和 Redis 预扣；Java 在 Lua 成功后发送订单消息到 RabbitMQ，消费者异步创建订单并扣减 MySQL 库存。

可靠性边界：

- 消费者手动 ack；
- 消费异常 nack，不重新入队，由死信队列承接；
- 重复消费通过 Redisson 锁、数据库查询和 `uk_user_voucher(user_id, voucher_id)` 唯一索引兜底；
- 消息表写入成功后的 RabbitMQ 发送异常由可靠消息表重试，不直接回滚 Redis；资格回滚统一经过 reservation + orderId CAS。

代码证据：

- `src/main/java/com/flashdeal/mq/VoucherOrderProducer.java`
- `src/main/java/com/flashdeal/mq/VoucherOrderConsumer.java`
- `src/main/java/com/flashdeal/config/RabbitMqConfig.java`
- `src/main/java/com/flashdeal/utils/RabbitConstants.java`
- `src/main/resources/db/flashdeal.sql`

### 3. Caffeine + Redis 二级缓存

商户详情接口：

```text
GET /shop/{id}
```

查询链路：

```text
Caffeine -> Redis -> MySQL
```

实现要点：

- Caffeine 缓存 `Shop` 实体，默认 5 分钟过期；
- Redis 保留原有空值缓存，防止缓存穿透；
- Redis 正常缓存 TTL 增加随机时间，降低缓存雪崩风险；
- 本地缓存开关支持压测对比：

```yaml
local-cache:
  shop:
    enabled: true
```

代码证据：

- `src/main/java/com/flashdeal/config/CaffeineConfig.java`
- `src/main/java/com/flashdeal/utils/LocalCacheConstants.java`
- `src/main/java/com/flashdeal/service/impl/ShopServiceImpl.java`
- `src/main/resources/application-dev.yaml`

### 4. RabbitMQ fanout 广播本地缓存失效

商户更新后，当前实例删除 Redis 和本地 Caffeine，同时通过 RabbitMQ fanout exchange 广播缓存失效消息。各实例使用独立 `AnonymousQueue` 接收消息并删除自己的 Caffeine 本地缓存。

流程：

```text
PUT /shop
  -> 更新 MySQL
  -> 删除 Redis
  -> 删除当前实例 Caffeine
  -> RabbitMQ fanout 广播失效消息
  -> 各实例删除本地 Caffeine
```

代码证据：

- `src/main/java/com/flashdeal/mq/ShopCacheInvalidationProducer.java`
- `src/main/java/com/flashdeal/mq/ShopCacheInvalidationConsumer.java`
- `src/main/java/com/flashdeal/dto/ShopCacheInvalidationMessage.java`
- `src/main/java/com/flashdeal/config/RabbitMqConfig.java`
- `src/main/java/com/flashdeal/service/impl/ShopServiceImpl.java`

### 5. 订单状态流转 + Transactional Outbox 超时关单

订单状态沿用当前表结构：

| 状态码 | 含义 |
| --- | --- |
| 1 | 未支付 |
| 2 | 已支付 |
| 3 | 已核销 |
| 4 | 已取消 |
| 5 | 退款中 |
| 6 | 已退款 |

状态流转：

```text
未支付 -> 已支付
未支付 -> 已取消
已支付 -> 已核销
```

超时关单流程：

```text
订单创建成功
  -> 同事务写入 tb_outbox_event
  -> OrderTimeoutOutboxPublishTask 发布 OrderTimeoutMessage 到 TTL 延迟队列
  -> TTL 到期后进入关闭队列
  -> 消费者检查订单状态
  -> 仍为未支付则取消订单
  -> 回补 MySQL 库存和 Redis 秒杀库存
  -> 不删除 Redis 一人一单标记
```

模拟接口：

```text
POST /voucher-order/pay/{orderId}
POST /voucher-order/use/{orderId}
```

代码证据：

- `src/main/java/com/flashdeal/enums/VoucherOrderStatus.java`
- `src/main/java/com/flashdeal/dto/OrderTimeoutMessage.java`
- `src/main/java/com/flashdeal/entity/OutboxEvent.java`
- `src/main/java/com/flashdeal/mq/OrderTimeoutOutboxPublishTask.java`
- `src/main/java/com/flashdeal/mq/OrderTimeoutProducer.java`
- `src/main/java/com/flashdeal/mq/OrderCloseConsumer.java`
- `src/main/java/com/flashdeal/service/impl/VoucherOrderServiceImpl.java`
- `src/main/resources/application-dev.yaml`

### 6. Redis + AOP + 自定义注解滑动窗口限流

通过 `@RateLimit` 注解接入限流，支持：

- IP 维度；
- USER 维度；
- API 维度。

底层使用 Redis ZSet 记录请求时间戳，通过 Lua 保证删除窗口外请求、统计当前窗口数量、写入本次请求和设置过期时间的原子性。

当前接入：

```java
@RateLimit(key = "seckill:voucher", windowSeconds = 5, maxRequests = 3, dimension = RateLimitDimension.USER)
```

```java
@RateLimit(key = "shop:detail", windowSeconds = 10, maxRequests = 20, dimension = RateLimitDimension.IP)
```

代码证据：

- `src/main/java/com/flashdeal/annotation/RateLimit.java`
- `src/main/java/com/flashdeal/annotation/RateLimitDimension.java`
- `src/main/java/com/flashdeal/aspect/RateLimitAspect.java`
- `src/main/resources/rate_limit.lua`

## 本地启动

### 1. 启动依赖

```bash
docker compose -f docker-compose.dev.yml up -d
```

依赖端口：

| 服务 | 地址 |
| --- | --- |
| MySQL | `localhost:3306` |
| Redis | `127.0.0.1:6379` |
| RabbitMQ | `127.0.0.1:5672` |
| RabbitMQ 管理台 | `http://127.0.0.1:15672` |

RabbitMQ 默认账号：

```text
guest / guest
```

### 2. 导入数据库

```bash
docker exec -i flashdeal-dev-mysql mysql -uroot -p123456 flashdeal < src/main/resources/db/flashdeal.sql
```

如果是已有数据库，检查订单唯一索引和取消时间字段：

```sql
SHOW INDEX FROM tb_voucher_order;
DESC tb_voucher_order;
```

必要时执行：

```sql
ALTER TABLE tb_voucher_order
ADD UNIQUE KEY uk_user_voucher (user_id, voucher_id);

ALTER TABLE tb_voucher_order
ADD COLUMN cancel_time DATETIME NULL COMMENT '取消时间' AFTER use_time;
```

### 3. 启动项目

```bash
mvn spring-boot:run -Dspring-boot.run.profiles=local
```

或：

```bash
mvn spring-boot:run -Dspring-boot.run.arguments="--spring.profiles.active=local"
```

dev 环境配置：

- MySQL：`localhost:3306`
- Redis：`127.0.0.1:6379`
- RabbitMQ：`127.0.0.1:5672`
- 订单超时时间：30 秒

### 4. 编译验证

```bash
mvn -q -DskipTests compile
```

## 常用接口

| 功能 | 方法和路径 |
| --- | --- |
| 发送验证码 | `POST /user/code` |
| 登录 | `POST /user/login` |
| 查询商户详情 | `GET /shop/{id}` |
| 更新商户 | `PUT /shop` |
| 新增秒杀券 | `POST /voucher/seckill` |
| 查询商户券列表 | `GET /voucher/list/{shopId}` |
| 秒杀下单 | `POST /voucher-order/seckill/{voucherId}` |
| 模拟支付 | `POST /voucher-order/pay/{orderId}` |
| 模拟核销 | `POST /voucher-order/use/{orderId}` |

需要登录的接口使用请求头：

```text
authorization: {token}
```

## 当前完成状态

| 能力点 | 状态 |
| --- | --- |
| Redis + Lua 秒杀预校验 | 已实现 |
| RabbitMQ 异步下单 | 已实现并端到端验收 |
| RabbitMQ 手动 ack、失败重试、死信队列 | 已实现 |
| Caffeine + Redis 二级缓存 | 已实现 |
| RabbitMQ fanout 多实例缓存失效 | 已实现并双实例验收 |
| Redis + AOP + 自定义注解限流 | 已实现并编译通过 |
| 订单状态流转 + 超时关单 | 已实现并端到端验收 |
| Docker Compose 本地环境 | 已实现 |
| 压测真实数据 | 未执行，只有方案和模板 |
| 可靠消息表 | 已实现，负责 RabbitMQ 投递可靠性 |
| Redis pending + reservation 对账 | 已实现 |
| 超时关单 Transactional Outbox | 已实现 |

## 简历建议写法

可以写：

```text
基于 Redis + Lua 实现优惠券秒杀库存校验、一人一单校验和库存预扣，使用 RabbitMQ 异步落库，消费者通过手动 ack、失败重试和死信队列保障消费可靠性，并通过唯一索引和条件扣库存防止重复下单与超卖。
```

```text
针对商户详情热点读场景引入 Caffeine + Redis 二级缓存，支持本地缓存开关用于压测对比；商户更新后通过 RabbitMQ fanout 广播缓存失效消息，降低多实例本地缓存不一致风险。
```

```text
设计秒杀订单状态流转，订单创建同事务写入 `tb_outbox_event`，再由 Outbox 发布任务基于 RabbitMQ TTL + DLX 触发超时未支付自动关单，通过条件更新保证支付与关单并发安全，并在取消成功后回补 MySQL 和 Redis 库存。
```

压测数据尚未执行，简历中不要填写具体 QPS、平均响应时间、P95 或提升比例。

## 后续优化

- 执行真实压测并补充性能数据；
- 接入真实告警平台，巡检可靠消息、Redis pending 和 Outbox 的 `NEED_MANUAL`；
- 增加订单查询接口，方便验收和前端展示；
- 限流 IP 获取支持可信代理和 `X-Forwarded-For`；
- 补充运行监控和告警。
## Redis pending 对账边界

秒杀 Lua 成功时会原子写入 `seckill:pending`、pending detail 和 reservation。批量消费者是唯一订单落库入口，先查 MySQL，再 claim reservation，订单落库与 `tb_mq_message=CONSUMED` 在同一事务提交。`SeckillRedisMysqlReconcileTask` 扫描 Redis pending，处理 Lua 成功但消息表尚未写入时宕机的窗口。

可靠消息表负责已落表消息的 RabbitMQ 投递可靠性；订单创建后的超时关单事件已由 Transactional Outbox 保证。

## 可靠消息表职责边界

- Redis pending 对账负责判断 Redis 预扣资格最终有没有形成 MySQL 订单。
- `tb_mq_message` 只负责消息落表后的 RabbitMQ 投递可靠性、重试、NEED_MANUAL 巡检和消费成功状态。
- DLQ 是消费失败异常兜底，Redis 回滚必须经过 reservation + orderId CAS。
- Transactional Outbox 已用于订单创建后的超时关单事件。

## 三轮可靠性边界

- Redis pending 对账：负责 Redis 预扣资格最终有没有形成 MySQL 订单。
- `tb_mq_message`：负责秒杀订单消息落表后的 RabbitMQ 投递可靠性。
- `tb_outbox_event`：负责订单创建成功后的超时关单事件不丢失。

订单与 `ORDER_TIMEOUT` Outbox 事件在同一个 MySQL 事务中提交。发布任务按固定 `expireTime` 计算剩余延迟，发布失败退避重试，耗尽后进入 `NEED_MANUAL`。

## 项目来源

本项目基于公开教学项目进行二次开发和可靠性重构。
