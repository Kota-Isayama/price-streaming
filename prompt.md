添付のREADMEを読み込み、このプロジェクトについて**技術者向けのアーキテクチャ説明・比較検討用PowerPoint資料**を作成してください。

この資料の目的は、単なる実装状況の報告ではありません。

初めてこのプロジェクトを見るエンジニア・アーキテクト・技術マネージャーが、

* 何を実現したいシステムなのか
* なぜ現在の構成になっているのか
* 現在どこまで実装できているのか
* Stateful Pricing Worker方式とPolling Pricing API方式の違い
* Kafka、RabbitMQ、ZeroMQ、NATS、Redis、Azure系サービスなどをどこに使えるのか
* 今後Production化するために何が不足しているのか
* どのArchitectureを最終採用するか判断するために何を検証すべきか

を視覚的に理解できるようにすることが目的です。

READMEに書かれている内容を一次情報として扱ってください。

READMEに書かれている「現在実装済みの内容」「検討中の内容」「将来案」を混同しないでください。

特に資料中では、

**Current / Implemented**
**Candidate / Alternative**
**Future / Proposed**

を明確に区別してください。

READMEに存在しない事実を、現在実装済みであるかのように補完しないでください。

一方で、「今後さらにあった方がよい機能・Infrastructure・運用機能」については、一般的な分散システム・リアルタイムPricing Platformの観点から積極的に提案してください。その場合は必ず「提案」「Future Proposal」などと明示してください。

---

## 想定読者

主な読者は以下です。

* Backend Engineer
* Application Architect
* Platform / Infrastructure Engineer
* Quant / Pricing Engineer
* Technical Manager

金融商品Pricingそのものに詳しくない人でも理解できるようにしてください。

現在の商品例は **JPY Interest Rate Swap** に限定してください。

ただし、

「JPY IRS専用システムを作っているのではなく、商品固有なのはPricing Domainであり、Lifecycle / Runtime / Recovery / Distributionなどの外側のFrameworkは他商品にも適用できる」

という設計思想を明確にしてください。

Bondの具体例は今回の資料では不要です。

---

## 資料全体のストーリー

以下の順番で、ストーリーとして理解できる資料にしてください。

### 1. What are we building?

最初に、実現したい業務・機能を一枚で示してください。

例えば、

Client / User
→ RFQ登録
→ Pricing開始
→ Market Data更新
→ Repricing
→ RFQ条件変更
→ Repricing
→ Pricing停止

という流れです。

将来的には、

Pricing Result
→ Realtime Distribution
→ Sales / Trader UI
→ Quote / Execution

へ発展することも示してください。

このスライドでは実装技術よりも、「システムが何をしたいのか」を中心にしてください。

---

### 2. Design Requirements

このArchitectureに必要な非機能要件を整理してください。

例：

* Market Dataは高頻度・低遅延
* 最新Marketを使ってPricingしたい
* RFQのActivate / Change / Stopはロストさせたくない
* 同一RFQの変更順序を守りたい
* Pricing Processを複数台へScale Outしたい
* Processが落ちても復旧できるようにしたい
* Product固有PricingとRuntime Infrastructureを分離したい
* Infrastructureを将来的に差し替えられるようにしたい

これらのRequirementが後続のArchitecture選択につながるようにしてください。

---

### 3. Current Architecture — Stateful Pricing Worker Model

現在実装しているArchitectureを大きな構成図で示してください。

必ず以下を図に含めてください。

User
→ FastAPI Web API
→ PostgreSQL
→ Transactional Outbox
→ Outbox Publisher Process
→ Kafka
→ Pricing Process

Pricing Processには、

* Kafka Lifecycle Consumer
* ZeroMQ Market Data Consumer
* MarketState
* InMemory RfqPricingSessionStore
* SwapPricer

が存在します。

ZeroMQ Market Data SourceからPricing Processへの別経路も図示してください。

「Business StateはPostgreSQL」
「Lifecycle ControlはKafka」
「Live Market DataはZeroMQ」
「Hot Runtime StateはProcess Memory」

という役割分担が視覚的に分かるようにしてください。

---

### 4. RFQ Lifecycle Sequence

Create / Change / StopについてSequence Diagram風のスライドを作成してください。

特にChangeでは、

User
→ PATCH RFQ
→ RFQ revision更新
→ PricingRequest新revision保存
→ Outbox Changed
→ Kafka
→ Pricing Process
→ PricingRequest revision取得
→ Pricing Session更新

という流れを示してください。

「KafkaにChanged(revision=N)が現れた時点では、PricingRequest revision=NがDBから取得可能」

というTransactional Outboxの保証も強調してください。

---

### 5. Why Transactional Outbox?

DB更新後にWeb APIからKafkaへ直接publishする方式と、Transactional Outbox方式を比較してください。

Bad Pattern:

DB Commit
→ Kafka Publish
→ Kafka Publish失敗

Current Pattern:

DB Transaction
→ RFQ + PricingRequest + Outbox
→ Commit
→ Outbox Publisher
→ Kafka

という比較図を作り、なぜOutboxを採用しているのか視覚化してください。

---

### 6. Stateful Pricing Runtime

RfqPricingSessionの役割を説明してください。

SessionはDomain Aggregateではなく、Process Local Runtime Stateです。

Sessionが保持するものの例：

* PricingRequest
* revision
* Market dependencies
* pricing中か
* reprice要求があるか

Session自身はPricing Calculationを行わず、

Application
→ いつPricingするか決定

Domain Pricer
→ Price Calculation

という責務分離を図示してください。

---

### 7. Alternative Architecture — Polling Pricing API Model

Stateful Worker方式とは別に検討しているPolling方式を大きなArchitecture Diagramで示してください。

Polling方式ではPricing Workerを、

**複数のPricing API Process**

へ置き換えます。

各Pricing API ProcessはZeroMQ / NATS等のMarket Data Feedを購読して、

Local MarketState

を常に最新化します。

ただし、RFQごとのPricing Sessionは持ちません。

代わりに、

Browser
→ WebSocket / Realtime Gateway
→ 自分が担当しているRFQを管理
→ Pricing API Poolへ定期的にHTTP GET
→ 最新MarketStateでPricing
→ WebSocket経由でBrowserへPriceを配信

という構成にします。

Pricing APIは、

PricingRequest
+
Current Local MarketState
→ Price

という比較的StatelessなServiceとして動作する想定です。

---

### 8. Stateful vs Polling

この比較は資料の中心の一つにしてください。

表・評価マトリクスを使って、少なくとも以下を比較してください。

* RFQ Session Ownership
* Process Scale Out
* Process Failure Recovery
* Kafka Partition依存
* Concurrency Control
* Repricing Latency
* HTTP Request数
* Pricing Calculation数
* Conflation
* Implementation Complexity
* Operational Complexity
* Suitable RFQ Count
* Suitable Pricing Cost
* 超低遅延Pricingへの適性

「Stateful方式が正解」「Polling方式が正解」と決めつけないでください。

最終判断には、

RFQ数
Market Update Frequency
Required Latency
Pricing Calculation Cost
CPU Usage
Network Traffic

などを実測する必要があることを示してください。

Hybrid方式についても、小さく候補として示してください。

例：

Stateless Pricing APIを基本Primitiveとして持ち、
一部の超低遅延RFQのみStateful Session Runtimeへ載せる。

---

### 9. Messaging / Infrastructure Comparison

ここは文章ではなく、**比較表・ヒートマップ・Pros/Consカード**を中心にしてください。

候補として少なくとも以下を含めてください。

* Kafka
* RabbitMQ Quorum Queue
* RabbitMQ Streams
* ZeroMQ
* Core NATS
* NATS JetStream
* Redis Pub/Sub
* Redis Streams
* Azure Event Hubs
* Azure Service Bus
* HTTP / REST

評価軸の例：

* Durability
* Replay
* Ordering
* Fan-out
* Low Latency
* Consumer Group / Work Distribution
* Stateful RFQ Routingとの相性
* Operation Complexity
* Vendor Lock-in
* Azure PaaS化のしやすさ

そして、

**Lifecycle Control**
**Market Data**
**Realtime UI Distribution**
**Background Task**
**Synchronous Pricing**

のどこに適しているかを示してください。

Technologyそのもののランキングではなく、「用途との相性」として評価してください。

---

### 10. Infrastructure Combination Candidates

具体的なArchitecture Stackを3〜4案並べてください。

例：

#### Current OSS-oriented

PostgreSQL

* Kafka
* ZeroMQ
* Process Local Memory
* 将来 Redis / WebSocket

#### Azure Managed Balanced

Azure Database for PostgreSQL

* Azure Event Hubs
* AKS Pricing Process
* ZeroMQ / NATS Market Data
* Azure Managed Redis

#### Azure Native

Azure Database for PostgreSQL

* Azure Service Bus Sessions
* AKS
* Azure Web PubSub

#### NATS-centric

PostgreSQL

* NATS JetStream
* Core NATS
* WebSocket Gateway

各案について、

* メリット
* デメリット
* Vendor Lock-in
* Operational burden
* Stateful Pricingとの相性
* Polling Pricingとの相性

を視覚的に比較してください。

---

### 11. What Is Missing for Production?

READMEに記載されている未実装部分に加え、リアルタイムPricing Platformとして必要と思われる機能を提案してください。

少なくとも以下を検討してください。

#### Pricing Process Recovery

Pricing Processが死亡すると、

MarketState
PricingSessionStore

が消えます。

Persistent StateからSessionを復元する必要があります。

#### Kafka Rebalance Recovery

Partitionのownerが変わった場合、

新ownerが対象RFQのSessionを再構築する必要があります。

#### Market Snapshot

ZeroMQ等のLive FeedだけではProcess停止中のMarket Updateを失います。

そのため、

Market Snapshot
+
Live Updates

で復旧する必要があります。

さらに、

Snapshot sequence
+
Live stream sequence

によってSnapshotとLive Data間のgapを防ぐ案を提案してください。

Market Snapshotの保存候補として、

* PostgreSQL
* Redis
* Dedicated Market Data Cache
* Object Storage
* Kafka compacted topic
* External Market Data Service

などを比較しても構いません。

それぞれについて、

* Snapshot size
* Update frequency
* Recovery latency
* Persistence requirement

から向き不向きを説明してください。

#### Concurrency Control

Market Update
Change
Stop

が同じSessionへ同時に作用するrace conditionがあります。

候補：

* Global Lock
* Per-RFQ Lock
* Event Serialization
* Actor / Mailbox

特にPer-RFQ Actorを有力候補として説明してください。

#### Conflation / Latest Wins

Pricing中にMarket Updateが複数届いた場合、

全UpdateをPricingせず最新Marketだけを再Pricingする方式を図示してください。

#### Backpressure

Pricing速度よりMarket Update速度が速い場合の制御を提案してください。

#### Observability

候補Metrics：

* Kafka Consumer Lag
* Outbox Backlog
* Active Pricing Sessions
* Pricing Latency
* Pricing Throughput
* Market Data Lag
* Repricing Count
* Conflation Count
* Polling Request Rate
* Error Rate
* Rebalance Count

#### Health Check

Web API
Outbox Publisher
Pricing Process

それぞれでLiveness / Readinessが何を意味するか整理してください。

#### Graceful Shutdown

Kafka offset
In-flight Pricing
Session ownership
Outbox publish

を安全に終了する設計を提案してください。

#### Message Schema

将来的なEvent Envelopeとして、

event_id
event_type
schema_version
occurred_at
rfq_id
revision
correlation_id

などを候補として示してください。

JSON / Avro / Protobufなどの比較も必要なら行ってください。

---

### 12. Future Realtime Distribution

将来的なUI Pricing配信方式を提案してください。

候補Architecture：

Pricing Runtime
→ Realtime Backplane
→ WebSocket / SSE Gateway
→ Browser

Backplane候補：

* Redis Pub/Sub
* Core NATS
* Kafka

Browser Transport：

* WebSocket
* SSE
* Azure Web PubSub

一覧画面で複数RFQを購読し、詳細画面では特定RFQを購読することも想定してください。

Polling Pricing Architectureの場合は、WebSocket Gateway自体がPricing Poll Schedulerになる可能性も示してください。

---

### 13. Recommended Evaluation Plan

Architectureを議論だけで決めず、PoCで比較する計画を提案してください。

例えば、

Stateful Worker方式
vs
Polling Pricing API方式

について、

* 100 RFQ
* 1,000 RFQ
* 10,000 RFQ
* Market Update 1秒
* Market Update 100ms
* 軽いPricing
* 重いPricing

など条件を変え、

* End-to-end latency
* CPU
* Memory
* Pricing count
* HTTP request count
* Recovery time
* Scale-out time

を比較するPerformance Test案を作ってください。

---

## スライド構成

PowerPointは **15〜20枚程度** を目安にしてください。

推奨構成：

1. Title / Project Overview
2. Business Goal
3. Functional & Non-functional Requirements
4. Current System Overview
5. Create / Change / Stop Flow
6. Transactional Outbox
7. Stateful Pricing Runtime
8. Stateful Multi-Process Architecture
9. Polling Pricing API Architecture
10. Stateful vs Polling Comparison
11. Control Plane vs Market Data Plane
12. Messaging Technology Comparison
13. Infrastructure Combination Options
14. Failure / Recovery Scenarios
15. Market Snapshot & Recovery Proposal
16. Concurrency / Conflation / Backpressure
17. Realtime UI Distribution
18. Observability / Operations
19. Evaluation Plan
20. Roadmap / Decision Points

必要であれば枚数は調整して構いません。

---

## Visual Design Requirements

文章を大量に並べる資料にはしないでください。

優先順位は、

1. Architecture Diagram
2. Sequence Diagram
3. Comparison Table / Matrix
4. Decision Tree
5. Failure Scenario Diagram
6. Short explanatory text

としてください。

1スライド1メッセージを基本としてください。

Architecture Diagramでは、

* Web API
* PostgreSQL
* Kafka
* ZeroMQ
* Outbox Publisher
* Pricing Process
* Pricing API
* WebSocket Gateway
* Browser

などを色や形でカテゴリ分けしてください。

例えば、

* Application / Process
* Persistent Store
* Messaging
* Runtime Memory
* External / UI

を視覚的に区別してください。

「Current」「Alternative」「Future Proposal」も色またはラベルで区別してください。

比較表では、長文を書かず、

◎ / ○ / △ / ×

や短いPros / Consを利用してください。

---

## 特に強調するメッセージ

資料全体を通じて、以下を強調してください。

**1. 商品固有なのはPricing Domainであり、Runtime Architectureは他商品にも利用できる。**

**2. Business State / Lifecycle / Market Data / Hot Runtime Stateではデータの性質が違うため、Infrastructureを無理に統一しない。**

**3. 現在はStateful Pricing Session方式を実装しているが、Polling Pricing API方式も有力候補であり、まだArchitectureを固定していない。**

**4. Stateful方式の最大課題はSession Ownership / Recovery / Concurrency。**

**5. Polling方式の最大課題はPolling Load / Pricing Count / Latency。**

**6. 最終Architectureは机上の比較だけではなく、実際のLoad / Latency / Recovery Testによって決定する。**

**7. Infrastructureの具体実装はComposition Rootで差し替えられる構造を目指す。**

---

## 出力品質

単なるREADMEの章立てをPowerPointへ転記するのではなく、

**「要件 → Architecture → Alternative → Trade-off → Missing Pieces → Evaluation Plan」**

という技術意思決定のストーリーに変換してください。

資料を見ただけで、

> 「なぜこのArchitectureなのか」
> 「他にどんな選択肢があるのか」
> 「どこがまだ危ないのか」
> 「次に何を検証すればArchitectureを決められるのか」

が分かることを最重要視してください。
