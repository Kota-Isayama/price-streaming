# Request for Quote — Real-Time Pricing Platform

金融商品のRFQ（Request for Quote）を題材に、**リアルタイムPricing基盤の設計・実装を検証するプロジェクト**です。

現在は **JPY Interest Rate Swap（JPY IRS）** を具体的な商品として実装しています。

ただし、このプロジェクトの主眼は「金利スワップ専用システム」を作ることではありません。

商品固有なのは、

```text
PricingRequest
Market Data Dependencies
Pricing Logic
Price Representation
```

などのPricing Domain部分です。

一方、

```text
RFQ Lifecycle
Pricing Runtime
Market Data Distribution
Process Scaling
Failure Recovery
Realtime Distribution
Concurrency Control
```

といった外側の枠組みは、商品ごとに作り直すのではなく、**他の商品にも適用可能な共通Pricing Platform**として設計することを目標としています。

---

# 1. Goal

最終的には、

```text
User / Sales / Trader
        ↓
       RFQ
        ↓
Pricing Runtime
        ↑
   Market Data
        ↓
Realtime Price
        ↓
Sales / Trader UI
        ↓
Quote / Execution
```

というリアルタイムRFQシステムを構築します。

現在はその中でも、

> **RFQがPricing対象になってから、条件変更・Market変動・Pricing終了までを安全に扱う基盤**

に焦点を当てています。

---

# 2. Current Scope

現在実装しているLifecycleは以下です。

```text
Create RFQ
    ↓
Pricing Activated
    ↓
Pricing Session作成

Change RFQ
    ↓
Pricing Changed
    ↓
Sessionを新revisionへ更新

Stop Pricing
    ↓
Pricing Stopped
    ↓
Session削除
```

現在、以下のE2Eは動作しています。

| 機能                        | 状態     |
| ------------------------- | ------ |
| FastAPIによるRFQ作成           | ✅      |
| RFQ変更                     | ✅      |
| Pricing停止                 | ✅      |
| PostgreSQL永続化             | ✅      |
| SQLAlchemy Async          | ✅      |
| Repository / Unit of Work | ✅      |
| Transactional Outbox      | ✅      |
| Outbox Publisher Process  | ✅      |
| Kafka Lifecycle Messaging | ✅      |
| `Activated`受信             | ✅      |
| `Changed`受信               | ✅      |
| `Stopped`受信               | ✅      |
| InMemory Pricing Session  | ✅      |
| PricingRequest revision管理 | ✅      |
| ZeroMQ Market Data受信      | 基本実装あり |
| 本格的なSwap Pricing          | 未実装    |
| Process Restart Recovery  | 未実装    |
| Kafka Rebalance Recovery  | 未実装    |
| Concurrency Control       | 未実装    |
| Conflation / Latest Wins  | 未実装    |
| UI Realtime Distribution  | 未実装    |

---

# 3. Current Architecture

現在採用している方式は **Event-driven / Stateful Pricing Session方式** です。

```text
                        User
                         │
                         │ HTTP
                         ▼
                    ┌─────────┐
                    │ Web API │
                    └────┬────┘
                         │
                 DB Transaction
                         │
                         ▼
                  ┌──────────────┐
                  │ PostgreSQL   │
                  │              │
                  │ RFQ          │
                  │ Pricing Req  │
                  │ Outbox       │
                  └──────┬───────┘
                         │
                         ▼
               ┌──────────────────┐
               │ Outbox Publisher │
               └────────┬─────────┘
                        │
                        ▼
                     Kafka
                        │
                        ▼
                ┌───────────────┐
Market Data ───►│Pricing Process│
   ZeroMQ       │               │
                │ MarketState   │
                │ SessionStore  │
                │ SwapPricer    │
                └───────────────┘
```

---

# 4. Architecture Style

Onion Architectureを採用しています。

```text
presentation
     ↓
application
     ↓
domain
```

InfrastructureはApplicationまたはDomainで定義したPortを実装します。

```text
Application Port
      ▲
      │
Infrastructure Adapter
```

Process BoundaryとLayer Boundaryは別物です。

例えばPricing Processの中にも、

```text
presentation
application
domain
infrastructure
```

が存在します。

---

# 5. Major Processes

## Web API Process

Userから業務操作を受け取ります。

```text
HTTP
 ↓
FastAPI Endpoint
 ↓
Application UseCase
 ↓
Unit of Work
 ↓
Repositories
 ↓
PostgreSQL
```

Web APIはPricing Processへ直接命令しません。

---

## Outbox Publisher Process

PostgreSQLのOutboxをpollingし、未送信イベントをKafkaへpublishします。

```text
Outbox
 ↓
Lifecycle Event
 ↓
Kafka
 ↓
published=true
```

Web APIが直接Kafkaへpublishしないのは、

```text
DB Commit成功
 ↓
Kafka Publish失敗
```

という不整合を避けるためです。

---

## Pricing Process

Market DataとRFQ Lifecycleという複数のInbound Adapterをホストする常駐Processです。

```text
               Pricing Process

 Market Data                  RFQ Lifecycle
     │                              │
   ZeroMQ                         Kafka
     │                              │
     ▼                              ▼
Market Handler              Lifecycle Handler
     │                              │
     ▼                              ▼
Market Update             Activate / Change /
 UseCase                     Stop UseCases
     │                              │
     └───────────┬──────────────────┘
                 ▼
          Runtime State

          ├─ MarketState
          └─ SessionStore
                 │
                 ▼
              Pricer
```

Pricing ProcessはDDDの特別なレイヤーではありません。

---

# 6. RFQ Create / Change / Stop Flow

## Create

```text
POST /rfqs
 ↓
CreateRfqUseCase
 ↓
DB Transaction
 ├─ RFQ INSERT
 ├─ PricingRequest rev1 INSERT
 └─ Outbox Activated(rev1)
 ↓
Outbox Publisher
 ↓
Kafka
 ↓
ActivateRfqPricingUseCase
 ↓
PricingRequest rev1取得
 ↓
RfqPricingSession作成
```

## Change

```text
PATCH /rfqs/{id}
 ↓
ChangeRfqUseCase
 ↓
RFQ revision++
 ↓
DB Transaction
 ├─ RFQ UPDATE
 ├─ PricingRequest revN INSERT
 └─ Outbox Changed(revN)
 ↓
Kafka
 ↓
ChangeRfqPricingUseCase
 ↓
PricingRequest revN取得
 ↓
Session更新
```

## Stop

```text
Stop Pricing
 ↓
Outbox Stopped
 ↓
Kafka
 ↓
StopRfqPricingUseCase
 ↓
SessionStore.remove(rfq_id)
```

---

# 7. PricingRequest Revision

Lifecycle Eventには商品条件全体を載せません。

例えば、

```text
RfqPricingChanged
├─ rfq_id
└─ revision
```

だけを通知します。

Pricing Processは、

```text
rfq_id + revision
       ↓
PricingRequest Repository
       ↓
PricingRequest
```

として実際のPricing条件を取得します。

例えば、

```text
RFQ-123 rev1
RFQ-123 rev2
RFQ-123 rev3
```

というrevision履歴をPostgreSQLへ保持します。

これによりLifecycle EventとPricing条件を明確に対応付けられます。

---

# 8. Transactional Outbox

RFQ状態とLifecycle Messagingの整合性にはTransactional Outboxを利用します。

例えばChangeでは、

```text
Transaction BEGIN

RFQ rev2保存
PricingRequest rev2保存
Outbox Changed(rev2)保存

Transaction COMMIT
```

となります。

その後にOutbox PublisherがKafkaへpublishします。

したがって、

> `Changed(revision=2)` がKafkaへ現れた時点では、PricingRequest revision 2は既にDBから読み取り可能

という契約を成立させられます。

---

# 9. Delivery Semantics

Exactly Onceは前提にしません。

基本戦略は、

```text
At-least-once Delivery
        +
Idempotent Processing
```

です。

例えば、

```text
Kafka publish成功
 ↓
Outboxのpublished更新前にProcess死亡
```

すると同一Eventが再送される可能性があります。

Pricing Process側では、

```text
incoming revision <= current revision
```

であれば無視するなど、duplicate / stale Eventに耐える設計にします。

---

# 10. JPY Interest Rate Swap

現在の具体的なPricing DomainはJPY IRSです。

```text
Swap Economics
      +
Market State
      ↓
SwapPricer
      ↓
SwapPrice
```

Applicationは、

```text
いつ
どのRFQを
どのMarket Snapshotで
Pricingするか
```

を判断します。

Domain Serviceは、

```text
PricingRequest
      +
MarketSnapshot
      ↓
Price
```

という計算自体を担当します。

現在の`SwapPricer`はPricing基盤検証用の簡易実装です。

将来的には、

```text
Curve Construction
Schedule
Cashflow
Discount Factor
Par Rate
NPV
DV01
```

などを実装します。

---

# 11. Product-Agnostic Pricing Framework

現在はJPY IRSのみを対象としています。

ただし、Infrastructure / Runtime設計はJPY IRSへ強く依存させません。

新しい商品を追加する場合には、主に商品固有の、

```text
PricingRequest
Market Dependencies
Pricer
Price Model
```

を追加します。

一方、

```text
RFQ Lifecycle

Outbox

Lifecycle Messaging

Pricing Runtime

Session Management

Process Recovery

Concurrency Control

Realtime Distribution
```

は共通枠組みを利用することを想定しています。

つまり、

```text
                    Common Pricing Runtime

Create / Change / Stop
         │
         ▼
      Session
         │
         ▼
   Pricing Request
         │
         ▼
  Product-specific Pricer
```

という構造です。

商品Domainを無理に共通化することはしません。

**共通化するのはPricingの計算式ではなく、Pricingを実行・管理・配信するRuntimeです。**

---

# 12. Market Data

現在のMarket Data PlaneにはZeroMQ PUB/SUBを利用しています。

```text
Market Data Source
       ↓
     ZeroMQ
       ↓
Pricing Process
       ↓
MarketState
```

Market Dataは、

| 特性      | 内容                |
| ------- | ----------------- |
| 更新頻度    | 高い                |
| 重要な値    | 最新値               |
| 全tick処理 | 必須とは限らない          |
| 遅延      | 小さい方がよい           |
| 配信先     | 複数Pricing Process |

という性質があります。

そのためLifecycle Messagingとは異なるInfrastructureを採用しています。

現在は簡略化して、

```text
JPY-OIS → Decimal
```

ですが、将来はCurve Gridなどの複雑なMarket Stateへ拡張します。

---

# 13. Control Plane vs Data Plane

本プロジェクトではMarket DataとRFQ Lifecycleを別物として扱います。

|            | Market Data Plane   | Pricing Control Plane  |
| ---------- | ------------------- | ---------------------- |
| 例          | Curve, Rate, Fixing | Activate, Change, Stop |
| 更新頻度       | 高い                  | 低い                     |
| 最新値重視      | 強い                  | 弱い                     |
| 1件のloss    | 許容可能な場合あり           | 原則危険                   |
| Ordering   | 値による                | 重要                     |
| Durability | Snapshot併用可能        | 必要                     |
| 現在の実装      | ZeroMQ              | Kafka                  |

すべてを同じMessaging Technologyへ統一することは目標にしていません。

---

# 14. Current Pricing Runtime — Session Model

現在のPricing ProcessはRFQごとの`RfqPricingSession`を保持します。

```text
RFQ-123 Session

├─ PricingRequest
├─ Revision
├─ Market Dependencies
├─ is_pricing
└─ reprice_requested
```

SessionはDomain Aggregateではなく、

> **process-local runtime state**

です。

PricingそのものはSessionに実装せず、Domain ServiceであるPricerが行います。

---

# 15. Alternative Runtime — Polling Pricing API Model

現在のSession Modelとは別に、**Polling型Architectureも有力候補として検討しています。**

この場合、現在のPricing ProcessはRFQ Sessionを所有するProcessではなく、複数台の **Pricing API群** になります。

```text
                Market Data
                    │
                    ▼
          ┌──────────────────┐
          │ Pricing API A    │
          │ Local MarketState│
          └──────────────────┘

          ┌──────────────────┐
          │ Pricing API B    │
          │ Local MarketState│
          └──────────────────┘

          ┌──────────────────┐
          │ Pricing API C    │
          │ Local MarketState│
          └──────────────────┘
```

各Pricing APIは今と同じくMarket Data Feedを購読し、**自分のLocal MarketStateを常に最新化**します。

ただしRFQ Sessionは保持しません。

代わりにWebSocket / Realtime Gateway側が、

```text
自分がUIへ配信しているRFQ
```

を把握し、Pricing APIへ定期的に価格取得を行います。

```text
Browser
   │
   ▼
WebSocket Gateway
   │
   │ RFQ-123を購読中
   │
   ├── periodically GET Price(RFQ-123)
   │
   ├── periodically GET Price(RFQ-456)
   │
   └── periodically GET Price(RFQ-789)
             │
             ▼
        Pricing API Pool
             │
             ▼
        Current Price
             │
             ▼
      WebSocket Gateway
             │
             ▼
           Browser
```

Pricing APIは、

```text
PricingRequest
      +
自分が最新化しているMarketState
      ↓
Price
```

をその場で計算します。

---

# 16. Session Model vs Polling API Model

これは今後の重要なArchitecture Decisionです。

| 観点                        | Stateful Session Model | Polling Pricing API Model  |
| ------------------------- | ---------------------- | -------------------------- |
| RFQ Session               | Pricing Processが保持     | Pricing APIは保持しない          |
| MarketState               | 各Pricing Processが保持    | 各Pricing APIが保持            |
| Repricing契機               | Market Data Event      | Gatewayから定期Request         |
| Price Push速度              | 非常に低遅延にしやすい            | Poll interval分の遅延          |
| RFQ Ownership             | 必要                     | Pricing APIには不要            |
| Kafka Partition Ownership | 重要                     | 大幅に重要度低下                   |
| Worker Scale変更            | Session Recovery必要     | 比較的容易                      |
| Worker死亡                  | Session再構築必要           | 別APIへRequestすればよい          |
| Concurrency制御             | Session単位で必要           | Pricing APIはほぼstateless    |
| HTTP Request量             | 少ない                    | RFQ数 × polling頻度で増加        |
| Pricing計算回数               | Market変化時だけに最適化可能      | Market不変でもpollごとに計算する可能性   |
| Conflation                | Runtime側で必要            | Polling自体が自然なconflationになる |
| UI Gateway責務              | 比較的小さい                 | Poll schedulerとして重くなる      |
| 実装難易度                     | 分散state管理が難しい          | 全体として単純化しやすい               |
| 超低遅延用途                    | 強い                     | Poll周期次第                   |

Pollingモデルにはかなり大きな利点があります。

特に、

```text
Pricing API instance A死亡
      ↓
次のHTTP requestはinstance Bへ
```

とできるため、

> **RFQ Session Ownershipそのものを消せる**

可能性があります。

一方、

```text
10,000 RFQ
×
1秒に1回poll
```

などになればHTTP Request数・Pricing回数が非常に多くなります。

したがって、

```text
Required Latency
RFQ数量
Pricing Cost
Market Update Frequency
UI Subscription Pattern
```

を測定して最終判断します。

---

# 17. Hybrid Model

StatefulとPollingを完全な二者択一にする必要もありません。

例えば、

```text
Pricing API
   ↓
Current MarketState保持
   ↓
price(request)
```

を基本Primitiveとして持ち、

一部の低遅延RFQのみ、

```text
Dedicated Session Runtime
```

を載せることも可能です。

つまり、

```text
                Product Pricer
                      ▲
                      │
        ┌─────────────┴─────────────┐
        │                           │
Polling Pricing API        Stateful Session Runtime
```

という構成も候補です。

---

# 18. Infrastructure Comparison

## Messaging / Streaming

| Technology            | Durability | Replay |    Ordering | Fan-out | Low Latency | Stateful RFQ Routing | 運用負荷 | 主な候補用途                     |
| --------------------- | ---------: | -----: | ----------: | ------: | ----------: | -------------------: | ---: | -------------------------- |
| Kafka                 |          ◎ |      ◎ | Partition内◎ |       ◎ |           ○ |                    ◎ |    高 | Lifecycle / Event Stream   |
| RabbitMQ Quorum Queue |          ◎ |      △ |     Queue内○ |       ○ |           ○ |                    △ |    中 | Commands / Tasks           |
| RabbitMQ Streams      |          ◎ |      ◎ |           ○ |       ◎ |           ○ |                    △ |    中 | Durable Streaming          |
| Azure Event Hubs      |          ◎ |      ◎ | Partition内◎ |       ◎ |           ○ |                    ◎ |    低 | Managed Kafka-like Stream  |
| Azure Service Bus     |          ◎ |      △ | Session利用で◎ |       ◎ |           ○ |                  ○〜◎ |    低 | Commands / Lifecycle       |
| ZeroMQ PUB/SUB        |          × |      × |           △ |       ◎ |           ◎ |                    × |  低〜中 | Market Data                |
| Core NATS             |          × |      × |           ○ |       ◎ |           ◎ |                    △ |    中 | Market Data / Realtime     |
| NATS JetStream        |          ◎ |      ◎ |           ○ |       ◎ |         ○〜◎ |                    ○ |    中 | Durable Messaging          |
| Redis Pub/Sub         |          × |      × |           △ |       ◎ |           ◎ |                    × |    低 | UI Backplane               |
| Redis Streams         |          ○ |      ◎ |           ○ |       ○ |           ○ |                    △ |  低〜中 | Lightweight Durable Stream |
| HTTP                  |      呼出側次第 |      × |   Request単位 |       × |           ○ |              LBだけでは× |    低 | Synchronous Pricing        |

---

# 19. Messaging Technology Detail

## Kafka

**現在のLifecycle実装。**

### Advantages

* Durable Log
* Replay可能
* Partition内Ordering
* `key=rfq_id` が使える
* Consumer Groupによるscale-out
* Partition ownershipをSession ownershipへ利用可能

### Disadvantages

* Broker運用が必要
* Partition設計が重要
* Rebalance処理が必要
* Stateful Runtimeと組み合わせるとRecovery設計が必要

### Suitable for

```text
Pricing Lifecycle
Integration Event
Durable Control Stream
```

---

## RabbitMQ Quorum Queue

### Advantages

* Command Queueとして自然
* Manual ACK
* Retry / DLQ
* Publisher Confirm
* Queue semanticsが理解しやすい

### Disadvantages

* 同一RFQを同じConsumerへrouteするには追加設計が必要
* Kafkaのような自然な`key → partition → owner`ではない

### Suitable for

```text
Command Processing
Task Dispatch
Notification
```

---

## Azure Event Hubs

Kafkaに近いpartition-based managed service候補です。

### Advantages

* Azure PaaS
* Cluster運用負荷が小さい
* Partition / Consumer Group型
* 現在のKafka Architectureを比較的維持しやすい

### Disadvantages

* Apache Kafkaそのものではない
* Azure依存
* Kafka固有機能との差を確認する必要あり

### Suitable for

```text
KafkaをAzure PaaSへ寄せたい場合
```

---

## Azure Service Bus

### Advantages

* Queue / Topic
* DLQ
* Retry
* Managed Service
* Sessionを用いて同一RFQをまとめる設計が可能

### Disadvantages

* Kafkaとは全く異なるモデル
* Event Log / Replay用途ではKafkaほど自然ではない
* Azure lock-inが比較的大きい

### Suitable for

```text
RFQ Command
Lifecycle Control
```

特に、

```text
SessionId = rfq_id
```

という設計は今後比較する価値があります。

---

## ZeroMQ

**現在のMarket Data候補。**

### Advantages

* Broker不要
* 非常に軽量
* Low Latency
* PUB/SUBによるfan-out

### Disadvantages

* Persistenceなし
* Replayなし
* Subscriber停止中のデータを失う
* Snapshot Recoveryが別途必要

### Suitable for

```text
Market Data
Live Feed
```

---

## Core NATS

ZeroMQよりbroker-centricな低遅延Messaging候補です。

### Advantages

* Low Latency
* Subject-based Routing
* PUB/SUB
* Queue Group
* Operational topologyをbrokerへ集約可能

### Disadvantages

* Core NATS単体ではDurabilityなし
* 新しいPlatformの運用が必要

### Suitable for

```text
Market Data
Realtime Distribution
```

---

## NATS JetStream

Core NATSにDurabilityを加えた候補です。

### Advantages

* Persistence
* ACK
* Replay
* Durable Consumer

### Disadvantages

* Kafkaとはownership modelが異なる
* 新たな運用知識が必要

### Suitable for

```text
Lifecycle
Durable Realtime Messaging
```

---

## Redis Pub/Sub

### Advantages

* 非常に簡単
* Low Latency
* WebSocket Gateway間のbackplaneに向く

### Disadvantages

* At-most-once
* Persistenceなし
* Subscriber停止中のmessageを失う

### Suitable for

```text
UI Realtime Backplane
Cache Invalidation
```

---

## Redis Streams

### Advantages

* Persistence
* Consumer Group
* ACK
* Replay

### Disadvantages

* Kafkaほど大規模stream processingを中心に設計されていない
* Stateful RFQ routingは別途考える必要あり

### Suitable for

```text
比較的小規模なDurable Stream
Background Processing
```

---

# 20. Database / State Infrastructure Comparison

| Technology            | Persistence | Transaction | Low-latency State | Multi-process Share | 主な用途                     |
| --------------------- | ----------: | ----------: | ----------------: | ------------------: | ------------------------ |
| PostgreSQL            |           ◎ |           ◎ |                 △ |                   ◎ | Source of Truth / Outbox |
| Process Memory        |           × |           - |                 ◎ |                   × | Hot Pricing Session      |
| Redis                 |           ○ |           △ |                 ◎ |                   ◎ | Shared Cache / Backplane |
| Kafka Log             |           ◎ |           × |                 △ |                   ◎ | Event History            |
| Market Snapshot Store |           ◎ |           - |                 ○ |                   ◎ | Market Recovery          |

現在は、

```text
Business State → PostgreSQL
Runtime Session → Process Memory
Market State → Process Memory
```

としています。

---

# 21. Candidate Infrastructure Combinations

## A. Current Stateful Architecture

| Area            | Technology            |
| --------------- | --------------------- |
| Web API         | FastAPI               |
| Database        | PostgreSQL            |
| ORM             | SQLAlchemy Async      |
| Outbox          | PostgreSQL            |
| Lifecycle       | Kafka                 |
| Market Data     | ZeroMQ                |
| Pricing Session | Process Memory        |
| Pricing         | Python Domain Service |
| UI Distribution | 未実装                   |

### Strong points

```text
Event-driven
Low latency
無駄なPricingを抑えやすい
RFQ単位のOwnershipを明確化可能
```

### Main challenge

```text
Session Recovery
Kafka Rebalance
Concurrency
State Ownership
```

---

## B. Polling Pricing API Architecture

| Area                 | Technology                 |
| -------------------- | -------------------------- |
| Web API              | FastAPI                    |
| Database             | PostgreSQL                 |
| Pricing Runtime      | Pricing API Pool           |
| Market Data          | ZeroMQ / NATS              |
| Market State         | Pricing API Process Memory |
| RFQ Scheduling       | WebSocket Gateway          |
| Price Retrieval      | HTTP                       |
| Browser Distribution | WebSocket / SSE            |

```text
Market Data
   ↓
Pricing API A/B/C
  Local MarketState
       ▲
       │ HTTP Poll
       │
WebSocket Gateway
       │
       ▼
     Browser
```

### Strong points

```text
Pricing APIはRFQに対してほぼstateless
Scale-outが容易
Worker restart recoveryが簡単
Partition ownership不要
```

### Main challenge

```text
HTTP polling量
Pricing計算回数
Polling latency
Gateway側のScheduling責務
```

---

## C. Azure Managed Architecture

| Area             | Candidate                            |
| ---------------- | ------------------------------------ |
| DB               | Azure Database for PostgreSQL        |
| Lifecycle        | Event Hubs / Service Bus             |
| Market Data      | ZeroMQ / NATS / External Feed        |
| Runtime          | AKS                                  |
| UI Backplane     | Azure Managed Redis                  |
| Browser Realtime | WebSocket Gateway / Azure Web PubSub |

目的は現在のDomain / Application設計を維持しつつ、Infrastructure運用をPaaSへ寄せることです。

---

## D. NATS-centric Architecture

| Area         | Candidate      |
| ------------ | -------------- |
| Lifecycle    | NATS JetStream |
| Market Data  | Core NATS      |
| UI Backplane | Core NATS      |
| DB           | PostgreSQL     |

Messaging基盤を統合しやすい構成ですが、Infrastructureを1種類にすること自体は目的ではありません。

---

# 22. Infrastructure Switching

最終的にはProcess起動時の設定だけでInfrastructureを選択できる構成を目指します。

例えば、

```toml
[database]
kind = "postgresql"

[lifecycle]
kind = "kafka"

[market_data]
kind = "zeromq"

[pricing_runtime]
kind = "stateful_session"

[realtime_distribution]
kind = "redis_pubsub"
```

Polling方式なら、

```toml
[pricing_runtime]
kind = "polling_api"
```

Azure環境なら、

```toml
[lifecycle]
kind = "azure_event_hubs"
```

などです。

切り替えはComposition Rootで行います。

```text
Config
 ↓
Composition Root
 ↓
Concrete Adapter
 ↓
Application Port
```

Application内部へ、

```python
if kafka:
    ...
elif rabbitmq:
    ...
```

のようなInfrastructure条件分岐を持ち込まない方針です。

---

# 23. Important Limitation of Abstraction

すべてのInfrastructureを完全互換として扱うことは目標ではありません。

例えば、

```text
Kafka Partition
RabbitMQ Queue
Service Bus Session
HTTP Load Balancer
```

ではownership semanticsが異なります。

したがって、

```text
Messageを受信する
```

というPortは共通化できても、

```text
どのProcessがどのRFQ Sessionを所有するか
```

まで無理に同じInterfaceへ押し込むべきではありません。

Infrastructure固有の強みは残します。

---

# 24. Multi-Process Pricing

Stateful Session方式では複数Pricing Processを利用する予定です。

```text
Pricing Deployment

├─ Process A
├─ Process B
└─ Process C
```

Kafka Lifecycleでは、

```text
rfq_id
  ↓
Kafka Partition
  ↓
Consumer Group Member
  ↓
Pricing Process
```

としてownershipを決めます。

例えば、

```text
Partition 0 → Process A
Partition 1 → Process B
Partition 2 → Process C
Partition 3 → Process A
```

です。

各ProcessのSessionStoreはLocal Memoryです。

---

# 25. Process Restart / Kafka Rebalance

現在まだ未実装の重要領域です。

Stateful Session方式でProcess Aが死亡すると、

```text
Process A
 └─ RFQ-123 Session
       ↓
      LOST
```

します。

KafkaはPartitionを別ProcessへRebalanceできますが、

```text
Partition移動
≠
Session移動
```

です。

そのため、

```text
partition revoked
 ↓
旧ownerのSession停止

partition assigned
 ↓
担当RFQを取得
 ↓
PricingRequest取得
 ↓
Session再構築
```

というRecoveryが必要です。

---

# 26. Why Polling Model Is Interesting Here

Polling Modelでは、

```text
RFQ Session Ownership
```

そのものをPricing APIから取り除ける可能性があります。

Pricing APIは、

```text
Current MarketState
+
Request
→
Price
```

だけを提供します。

つまりScale変更が、

```text
3 Pricing APIs
 ↓
5 Pricing APIs
```

になっても、Load Balancerが振り分ければ済みます。

この差は今後、

> Stateful Runtimeの低遅延・効率性と、Stateless Pricing APIの単純性

を比較する重要なテーマです。

---

# 27. Concurrency

Stateful方式では、

```text
Market Update
Change
Stop
```

が同じSessionを同時に触る可能性があります。

asyncioがsingle threadでも、`await`を跨げばraceは起こります。

例えば、

```text
Market Update
 ↓
rev1取得
 ↓
await

    Change rev2
      ↓
    Session更新

 ↓
古い処理再開
```

という問題があります。

候補は、

| 方式                  | 長所              | 短所           |
| ------------------- | --------------- | ------------ |
| Global Lock         | 簡単              | 全RFQが相互block |
| Per-RFQ Lock        | 比較的簡単           | Lock管理が必要    |
| Event Serialization | 明確              | Queue設計が必要   |
| Actor / Mailbox     | Single ownerで安全 | 実装複雑度増加      |

現在は **Per-RFQ Actor / Mailbox** が有力候補です。

---

# 28. Conflation / Latest Wins

Market Data更新速度がPricing速度より速い場合、

```text
v100 pricing中

v101
v102
v103
v104
```

すべてをPricingする必要はありません。

目標は、

```text
v100 Pricing
 ↓
Update発生を記録
 ↓
v100終了
 ↓
Latest Snapshot = v104
 ↓
v104 Pricing
```

です。

Polling Architectureの場合は、

> 次回poll時に最新MarketStateでPriceを求める

ため、Polling自体が自然なConflationとして働くという違いがあります。

---

# 29. Market Recovery

ZeroMQ PUB/SUBはSubscriber停止中のUpdateを保持しません。

したがってProcess restart時には、

```text
Market Snapshot
      +
Live Updates
```

が必要です。

さらに、

```text
Snapshot sequence = 100
 ↓
Live Update 101
 ↓
Live Update 102
```

のようにSnapshotとLive Feedの間をsequenceで接続することも検討します。

---

# 30. Observability

今後必要になる主要Metricsです。

| Metric               | Purpose        |
| -------------------- | -------------- |
| Kafka Consumer Lag   | Lifecycle処理遅延  |
| Outbox Backlog       | Event配送遅延      |
| Active Session Count | Pricing負荷      |
| Pricing Latency      | 計算性能           |
| Pricing Throughput   | 処理能力           |
| Market Data Lag      | Market鮮度       |
| Conflation Count     | Update圧縮効果     |
| Rebalance Count      | Runtime安定性     |
| Error Count          | 障害検出           |
| HTTP Poll Rate       | Polling方式の負荷測定 |

特にStateful方式とPolling方式を比較するには、

```text
Pricing Latency
CPU Usage
Pricing Count
Network Request Count
RFQ Count
```

を測定する必要があります。

---

# 31. Priority Improvements

| Priority | Item                             | Reason                     |
| -------- | -------------------------------- | -------------------------- |
| P0       | Pricing Process restart recovery | Stateful方式のCorrectness     |
| P0       | Kafka Rebalance Recovery         | Multi-process必須            |
| P0       | Active RFQのauthoritative state決定 | Recovery source            |
| P0       | Duplicate Event Idempotency      | At-least-once対応            |
| P1       | Session Concurrency Control      | Race防止                     |
| P1       | Conflation / Latest Wins         | Backpressure               |
| P1       | Market Snapshot Recovery         | ZeroMQ復旧                   |
| P1       | Multi-process scale test         | Ownership検証                |
| P1       | Polling Architecture PoC         | Stateful方式との比較             |
| P1       | Observability                    | Architecture評価             |
| P2       | Config-driven Adapter switching  | Infrastructure portability |
| P2       | Message Schema Versioning        | Evolution                  |
| P2       | UI Realtime Distribution         | End-user delivery          |
| P2       | Production Swap Pricer           | Domain精緻化                  |

---

# 32. Immediate Roadmap

現在、

```text
Create
 ↓
Activated

Change
 ↓
Changed

Stop
 ↓
Stopped
```

までは動作しています。

次に検証する項目は、

```text
1. Pricing Process restart recovery

2. Kafka partition reassignment時のSession recovery

3. Stateful Session concurrency

4. latest-wins / conflation

5. Market Snapshot recovery

6. 複数Pricing Processでscale-in / scale-out

7. Polling Pricing API方式のPoC

8. Stateful方式 vs Polling方式のperformance比較

9. Configuration-driven Infrastructure switching

10. Pricing Result realtime distribution

11. Production-grade JPY IRS Pricing
```

です。

---

# 33. Current Technology Stack

| Category                | Current Technology    |
| ----------------------- | --------------------- |
| Language                | Python                |
| Web API                 | FastAPI               |
| Async Runtime           | asyncio               |
| Database                | PostgreSQL            |
| ORM                     | SQLAlchemy Async      |
| Business Transaction    | Unit of Work          |
| Integration Reliability | Transactional Outbox  |
| Lifecycle Messaging     | Kafka                 |
| Kafka Client            | aiokafka              |
| Market Data             | ZeroMQ                |
| Runtime Session         | InMemory              |
| Pricing                 | Python Domain Service |
| Deployment Target       | Containers / AKS想定    |

---

# 34. Design Principles

## Product-specific Domain, Product-independent Runtime

Pricing計算自体を無理に共通化しません。

共通化するのは、

```text
Lifecycle
Runtime
Distribution
Recovery
Concurrency
Infrastructure Boundary
```

です。

---

## Runtime State Is Disposable

```text
RfqPricingSession
MarketState
```

は消えても構いません。

ただし再構築可能でなければなりません。

---

## Durable Business State, Ephemeral Hot State

```text
RFQ / PricingRequest
    → PostgreSQL

Session / MarketState
    → Process Memory
```

という役割分担を基本とします。

---

## At-least-once + Idempotency

Distributed Exactly Onceではなく、

```text
Reliable Delivery
+
Revision Checking
+
Idempotent Processing
```

を基本戦略とします。

---

## Choose Infrastructure by Data Semantics

```text
Lifecycle
Market Data
UI Fan-out
Business State
```

の性質は異なります。

すべてを1種類のMiddlewareへ統一することは目的としません。

---

## Infrastructure Is Selected at the Composition Root

Application LogicはKafkaやZeroMQを知りません。

Process起動時の設定とDIによってConcrete Adapterを選択します。

---

## Do Not Commit Too Early to Stateful Runtime

現在はStateful Pricing Session方式を実装しています。

しかし、

```text
Polling Pricing API
Hybrid Runtime
```

も有力候補です。

実装の美しさだけで決定せず、

```text
Latency
Throughput
RFQ Count
Pricing Cost
Operational Complexity
Failure Recovery
```

を測定してArchitectureを選択します。

---

# 35. Target

このプロジェクトの最終目標は、

> **特定の商品や特定のMessaging Technologyに依存せず、リアルタイム金融Pricingを安全に実行・配信できるRuntimeの設計を確立すること**

です。

現在はJPY IRSを題材として、

```text
FastAPI
PostgreSQL
SQLAlchemy
Transactional Outbox
Kafka
ZeroMQ
asyncio
InMemory Pricing Session
```

を使ったStateful方式を実装しています。

今後はPolling方式も実装し、

```text
Stateful Event-driven Pricing
vs
Stateless-ish Polling Pricing API
```

を実際の性能・運用性・障害復旧の観点から比較します。

その上で、

```text
Recovery
Concurrency
Backpressure
Scaling
Infrastructure Portability
Realtime Distribution
```

を補強し、実運用可能なPricing Platformへ発展させます。
