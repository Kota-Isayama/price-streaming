# Request for Quote

金融商品のRFQ（Request for Quote）を題材に、リアルタイムPricing基盤を設計・実装するためのプロジェクトです。

現在は主に **JPY Interest Rate Swap** を対象として、Market Dataの更新に応じてRFQを継続的に再Pricingする仕組みを実装しています。

最終的には、債券や金利スワップなど複数商品について、

```text
Market Data
    ↓
Pricing
    ↓
RFQ
    ↓
Realtime Distribution
    ↓
Sales / Trader UI
```

というリアルタイムRFQシステムを構築することを目標としています。

---

# 現在のスコープ

現時点では、RFQ全体の業務フローを実装しているわけではありません。

まず以下の部分に焦点を当てています。

```text
RFQ becomes pricing-active
        ↓
Pricing Session作成
        ↓
Market Data受信
        ↓
対象RFQを再Pricing
        ↓
RFQ変更時にSession更新
        ↓
RFQ終了時にSession削除
```

Quote登録、Execution、Booking、UIへのリアルタイム配信などは今後追加していきます。

---

# 対象商品

## JPY Interest Rate Swap

Swapでは、外部から完成済みのRFQ Priceを受け取るのではなく、Market Stateと契約条件から自サービス内でPriceを計算します。

概念的には、

```text
Market State
    +
Swap Economics
    ↓
SwapPricer
    ↓
SwapPrice
```

です。

現在の `SwapPricer` はPricing基盤の動作確認を目的としたMock実装であり、本格的な金融計算はまだ行っていません。

将来的にはCurve Construction、Schedule、Cashflow、Par Rate、NPV、DV01などを実装する予定です。

## Bond

Bondについては、Swapとは異なり、外部からすでに計算済みのBond Priceが配信されることを想定しています。

```text
Bond Price Stream
        ↓
Bond IDでselect
        ↓
RFQ Reference Price
```

したがって、

```text
Bond
= Selection

Swap
= Derivation / Calculation
```

という違いがあります。

この違いを無理に共通化せず、

```text
RFQに対してPricing Updateが継続的に発生する
```

という境界を共通化していく方針です。

---

# Architecture

Onion Architectureを採用しています。

基本レイヤーは以下です。

```text
domain
application
infrastructure
presentation
```

依存方向は内側へ向かいます。

```text
presentation
      ↓
application
      ↓
domain

infrastructure
      ↑
application port
```

Infrastructureの具体実装はApplication側で定義されたPortを実装します。

---

# Pricing Process

Pricing処理は将来的にAPI Processとは別の常駐Processとして動かすことを想定しています。

このプロセス自体をDDDの特別な要素として扱うのではなく、

**複数のInbound Adapterを同時にホストするProcess**

として扱います。

現在想定している構造は以下です。

```text
                     Pricing Process

       Market Data                    RFQ Lifecycle
           │                               │
        ZeroMQ                           Kafka
           │                               │
           ▼                               ▼
 Market Data Handler             RFQ Lifecycle Handler
           │                               │
           ▼                               ▼
 HandleMarketData          Activate / Change / Stop
  UpdateUseCase                    UseCases
           │                               │
           └───────────────┬───────────────┘
                           ▼
                  Shared Runtime State
                  ├─ MarketState
                  └─ SessionStore
                           │
                           ▼
                       SwapPricer
```

普通のWeb APIにおける、

```text
HTTP Request
    ↓
Endpoint
    ↓
UseCase
```

に対して、このProcessでは、

```text
ZeroMQ Message
    ↓
Handler
    ↓
UseCase
```

または、

```text
Kafka Message
    ↓
Handler
    ↓
UseCase
```

という形になります。

---

# Market Data

Market Dataのリアルタイム配信には **ZeroMQ PUB/SUB** を使用します。

Market Dataは、

* 更新頻度が高い
* 最新値が重要
* 古い更新をすべて処理する必要がない場合がある
* 複数のPricing Processへ同じデータをfan-outしたい

という性質を持つためです。

現在は簡略化して、

```text
JPY-OIS → Decimal
```

というMarket Dataを使用しています。

将来的には、

```text
Curve Grid
Bond Price
Fixing
FX Spot
Volatility Surface
```

など、それぞれ異なるデータ構造へ拡張します。

ZeroMQから受け取ったデータは、

```text
ZeroMQ
    ↓
ZeroMqMarketDataSubscriber
    ↓
MarketDataUpdate
    ↓
HandleMarketDataUpdateUseCase
    ↓
MarketState
```

という経路で処理されます。

---

# RFQ Pricing Lifecycle

RFQ Pricing Sessionの作成・変更・終了はMarket Dataとは性質が異なります。

例えば、

```text
RfqPricingActivated
RfqPricingChanged
RfqPricingStopped
```

は、一件のロストによる影響が大きいため、ZeroMQ PUB/SUBではなく **durable messaging** を利用する方針です。

現在はKafkaを第一候補としています。

```text
topic = rfq-pricing-lifecycle
key   = rfq_id
```

同じRFQ IDをKafka message keyとして使用することで、同一RFQのlifecycle eventが同じpartitionに配置されることを利用します。

Pricing Process群は同じConsumer Groupとして動作する想定です。

```text
Kafka

Partition 0 ─── Pricing Pod A
Partition 1 ─── Pricing Pod B
Partition 2 ─── Pricing Pod A
Partition 3 ─── Pricing Pod B
```

これを将来的な、

```text
Kafka Partition Ownership
        ≒
RFQ Pricing Session Ownership
```

の基礎とします。

---

# Pricing Request

Lifecycle Eventには、Swapの契約条件全体を含めません。

例えば、

```text
RfqPricingActivated
    rfq_id
    revision
```

程度の情報だけを持たせます。

Pricingに必要な具体的な経済条件は `RfqPricingRequestLoader` から取得します。

```text
RfqPricingActivated
    rfq_id = RFQ-123
    revision = 4
          ↓
ActivateRfqPricingUseCase
          ↓
RfqPricingRequestLoader
          ↓
SwapPricingRequest
          ↓
RfqPricingSession
```

Application側では取得経路を知りません。

```python
class RfqPricingRequestLoader(Protocol):
    async def load(
        self,
        rfq_id: RfqId,
        revision: int,
    ) -> LoadedPricingRequest:
        ...
```

Infrastructure側で、

```text
InMemoryRfqPricingRequestLoader
HttpRfqPricingRequestLoader
PostgresRfqPricingRequestLoader
```

などへ差し替え可能な設計とします。

現在はInMemory実装を使用しています。

---

# RfqPricingSession

`RfqPricingSession` はDomain Aggregateではありません。

1つのRFQについての **process-local runtime state** を表します。

概念的には、

```text
RFQ-123 Pricing Session
├─ PricingRequest
├─ Market Dependencies
├─ revision
├─ pricing中か
└─ reprice要求があるか
```

などを保持します。

現在は最小実装として、

```python
@dataclass
class RfqPricingSession:
    request: SwapPricingRequest
    dependencies: set[MarketDataId]

    is_pricing: bool = False
    reprice_requested: bool = False
```

程度です。

Session自身にはPricing計算を実行させません。

Pricingを実行するタイミングはApplication UseCaseが判断し、実際の価格計算はDomain Serviceである `SwapPricer` が担当します。

---

# Session Store

複数のUseCaseが同じPricing Sessionを操作するため、Session群は共有runtime stateとしてStoreへ切り出しています。

Application側ではPortを定義します。

```python
class RfqPricingSessionStore(Protocol):
    def get(
        self,
        rfq_id: RfqId,
    ) -> RfqPricingSession | None:
        ...

    def save(
        self,
        session: RfqPricingSession,
    ) -> None:
        ...

    def remove(
        self,
        rfq_id: RfqId,
    ) -> None:
        ...

    def list_all(
        self,
    ) -> list[RfqPricingSession]:
        ...
```

現在は、

```text
InMemoryRfqPricingSessionStore
```

を使用しています。

これはテスト用Fakeというだけではなく、本番でもPricing Process内のruntime stateを保持する方式として利用することを想定しています。

---

# Runtime Stateと永続化

重要な設計方針として、

**Pricing Process内のInMemory StateをSource of Truthにはしません。**

Pricing Processが突然終了すると、

```text
MarketState
RfqPricingSessionStore
```

は失われます。

これは許容し、外部のauthoritative stateから再構築できる設計を目指します。

## Pricing Session

Sessionは、永続化されたRFQ状態から再構築します。

```text
Persistent RFQ State
        ↓
currently pricing-active RFQ
        ↓
PricingRequest
        ↓
RfqPricingSession
```

## Market State

Market Stateは、

```text
Market Snapshot
        +
ZeroMQ Live Updates
```

から復元することを想定しています。

ZeroMQ PUB/SUBだけではSubscriber停止中の更新を復元できないため、Process restart時のMarket Snapshot取得は今後実装します。

---

# Application UseCases

Pricing Lifecycleについて、現在はUseCase単位にApplication Serviceを分けています。

```text
ActivateRfqPricingUseCase
ChangeRfqPricingUseCase
StopRfqPricingUseCase
HandleMarketDataUpdateUseCase
```

例えばActivateは、

```text
RfqPricingActivated
        ↓
PricingRequestをload
        ↓
Sessionを作成
        ↓
SessionStoreへ保存
        ↓
必要なMarket Dataが揃っている
        ↓
Initial Pricing
```

を担当します。

Market Updateは、

```text
MarketDataUpdate
        ↓
MarketState更新
        ↓
Active Sessionを確認
        ↓
更新されたMarket Dataに依存するSessionを特定
        ↓
Pricing
```

を担当します。

Stopは、

```text
RfqPricingStopped
        ↓
SessionStoreから削除
```

を担当します。

---

# Domain Pricing

価格計算そのものはDomain Serviceへ分離します。

例えば現在のMock `SwapPricer` は、

```python
class SwapPricer:
    def price(
        self,
        request: SwapPricingRequest,
        market: MarketSnapshot,
    ) -> SwapPrice:
        ...
```

という形です。

Application層は、

```text
どのRFQを
いつ
どのMarket Snapshotで
Pricingするか
```

を判断します。

Domain側は、

```text
PricingRequest
+
MarketSnapshot
↓
Price
```

という計算そのものを担当します。

Application UseCase内には金融計算式を書かない方針です。

---

# Current Runtime Model

現時点では、1 Pricing Processの中で以下を共有します。

```text
Pricing Process
│
├─ MarketState
│
└─ InMemoryRfqPricingSessionStore
```

複数UseCaseは同じインスタンスを参照します。

```text
                       ActivateUseCase
                           │
SessionStore ──────────────┼── ChangeUseCase
                           │
                           ├── StopUseCase
                           │
                           └── MarketUpdateUseCase


MarketState ───────────────┬── ActivateUseCase
                           │
                           ├── ChangeUseCase
                           │
                           └── MarketUpdateUseCase
```

---

# Concurrency

現在、Market Data ConsumerとRFQ Lifecycle Consumerは別々のasync Taskとして動作する想定です。

そのため、単一threadのasyncioであっても、

```text
MarketDataUpdateUseCase
        │
        ├─ Session取得
        │
        ├─ await ...
        │
        │
        │   ← この間に
        │      ChangeRfqPricingUseCase
        │      StopRfqPricingUseCase
        │
        └─ 処理再開
```

というTask間のrace conditionが発生する可能性があります。

これは現在の未解決課題です。

今後、

```text
asyncio.Lock
Session単位のLock
Actor / Mailbox
Single-owner Task
Event serialization
```

などを比較し、Session単位の安全な並行性制御を導入します。

特に、今後実装予定のlatest-wins / conflationとの相性から、Sessionを1 Taskが所有するActor的な設計も候補としています。

---

# Conflation / Latest Wins

Market Dataでは、全更新に対して必ずPricingを行う必要はないと考えています。

例えば、

```text
Curve v100でpricing中

v101
v102
v103
v104

が到着
```

した場合、

```text
v100
v101
v102
v103
v104
```

を全部計算するのではなく、

```text
v100 pricing
      ↓
更新が来たことだけ記録
      ↓
v100終了
      ↓
latest snapshot = v104
      ↓
v104で再pricing
```

するlatest-wins方式を検討しています。

`RfqPricingSession` に存在する、

```python
is_pricing
reprice_requested
```

は、この制御を実装するためのruntime stateとして利用する予定です。

---

# Multi-Pod

Pricing Processは複数Podで動作することを前提に設計します。

```text
Pricing Deployment
├─ Pod A
├─ Pod B
└─ Pod C
```

Market DataはすべてのPricing Podへ配信します。

```text
ZeroMQ Market Data
      │
      ├── Pod A
      ├── Pod B
      └── Pod C
```

一方、RFQ lifecycleは同じRFQを1つのPodが担当するようにKafka Consumer Groupを利用します。

```text
Kafka
  key = rfq_id
      ↓
partition
      ↓
Pricing Pod ownership
```

各PodのSession Storeはlocal memoryです。

```text
Pod A
└─ SessionStore
   ├─ RFQ-101
   └─ RFQ-105

Pod B
└─ SessionStore
   ├─ RFQ-102
   └─ RFQ-103
```

---

# Kafka Failure Recovery

複数Pod構成では、Pod停止時にKafka partitionが別Podへreassignされます。

例えば、

```text
Pod A
  Partition 3
  RFQ-123 Session

      ↓ Pod A死亡

Partition 3
      ↓
Pod Bへassignment
```

となった場合、Pod BにはRFQ-123のSessionが存在しません。

そのため今後、

```text
on_partitions_revoked
    ↓
そのpartitionに属するSessionを停止・破棄

on_partitions_assigned
    ↓
そのpartitionで現在activeなRFQを取得
    ↓
Session再構築
```

という仕組みを実装する予定です。

現在はこのstateful failoverまでは未実装です。

---

# Messaging Responsibilities

通信方式を統一することは目的としていません。

それぞれの要件に応じて使い分けます。

```text
Market Data
    → ZeroMQ PUB/SUB

RFQ Pricing Lifecycle
    → Kafka

Pricing Request取得
    → Application Port経由
       InMemory / HTTP / DB等

将来のUI Pricing Distribution
    → Redis / NATS / WebSocket等を検討
```

Market DataとRFQ Lifecycleは性質が異なるため、同じMessaging Technologyへ無理に統一しない方針です。

---

# Planned Repository Structure

現在は概ね以下の構造を想定しています。

```text
src/request_for_quote/
├── domain/
│   ├── common.py
│   ├── market/
│   ├── swap/
│   └── pricing/
│
├── application/
│   └── pricing/
│       ├── activate.py
│       ├── change.py
│       ├── stop.py
│       ├── handle_market_data_update.py
│       ├── session.py
│       └── ports/
│           ├── market_data_subscriber.py
│           ├── pricing_request_loader.py
│           ├── pricing_session_store.py
│           └── rfq_pricing_lifecycle_subscriber.py
│
├── infrastructure/
│   ├── inmemory/
│   ├── zeromq/
│   └── kafka/
│
└── presentation/
    └── pricing/
        ├── handlers.py
        └── main.py
```

実装を進めながら必要に応じて変更します。

---

# Current Development Goal

現在の最小E2E目標は以下です。

```text
1. Pricing Process起動

2. ZeroMQからMarket Data受信

3. KafkaからRfqPricingActivated受信

4. PricingRequestをロード

5. RfqPricingSession生成

6. Market Dataが揃っていればinitial pricing

7. ZeroMQ Market Data更新

8. Active RFQを自動reprice

9. RfqPricingChanged受信

10. PricingRequestを新revisionへ更新

11. 即reprice

12. RfqPricingStopped受信

13. Session削除

14. Market Dataが更新されてもそのRFQはpriceされない
```

---

# Next Steps

直近では以下を順番に進めます。

1. KafkaによるRFQ Pricing lifecycleのE2Eを完成させる
2. 複数async TaskからSessionを操作する際のrace conditionを整理する
3. Session単位のconcurrency controlを導入する
4. latest-wins / conflationを実装する
5. Kafka partition rebalanceとSession recoveryを実装する
6. Pricing Process restart時のMarket Snapshot recoveryを実装する
7. Bond Pricingを追加する
8. Pricing結果のRealtime Distributionを追加する
9. WebSocket経由でSales / Trader UIへ配信する
10. 本格的なSwap Pricingへ置き換える

---

# Design Principles

このプロジェクトでは以下を重視します。

**抽象化を急がない**

実際にBondとSwapを実装し、違いと共通点が確認できてから共通化します。

**Domain LogicとOrchestrationを分ける**

Application層は「いつ、何を、どの状態で実行するか」を担当し、金融計算そのものはDomainへ置きます。

**Runtime StateをSource of Truthにしない**

Pricing Process内のstateは失われても外部状態から再構築可能にします。

**Messaging Technologyを要件に合わせて選ぶ**

ZeroMQ、Kafka、HTTPなどを無理に統一せず、それぞれのデータの性質に合わせて選択します。

**Process BoundaryとArchitecture Layerを混同しない**

Pricing ProcessはDDDのレイヤーではありません。

1つのProcessの中で、

```text
Presentation
Application
Domain
Infrastructure
```

を組み合わせて動作させます。

**まず動かしてから高度化する**

最初から完全な分散・障害復旧・最適化を実装せず、最小E2Eを確認しながら一段ずつ高度化します。
