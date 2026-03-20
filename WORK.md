# RM 写操作上收 PD 统一方案（替代 PR #10185）+ PR 拆分执行计划

## Summary
- 目标：`resource manager` 的 **metadata 写操作统一在 PD server 完成**，覆盖经典模式与微服务模式。
- 兼容约束：`redirector.NewHandler` **必须保留**，不能删除；路径入口保持兼容。
- 微服务模式：RM 侧通过 watcher 获取最新 metadata，读请求可读本地 cache（或按设计转发），写请求不上 RM。
- 拆分策略：先做“默认行为不变”的基础重构，再切流 HTTP/gRPC/client，最后补 watcher 一致性与故障矩阵。
- PR 粒度：每个 PR 控制在 `<= 500` 行（目标 `200~480`），超预算继续拆。
- 当前进展（2026-03-20）：
  - PR1 已完成合并：<https://github.com/tikv/pd/pull/10227>
  - PR2 已完成合并：<https://github.com/tikv/pd/pull/10234>
  - PR3（含 former PR4）已完成合并：<https://github.com/tikv/pd/pull/10246>
  - PR5（former PR6，client 拆分）已完成合并：<https://github.com/tikv/pd/pull/10255>
  - PR6（watcher v1 scaffold）已完成合并：<https://github.com/tikv/pd/pull/10259>
  - PR4（metadata writes -> PD）当前是 draft PR，待在 `master` 上 rebase 并清理冲突：<https://github.com/tikv/pd/pull/10260>
  - `POST /resource-manager/api/v1/config/controller` 的 all-or-nothing follow-up 已从 PR3 review 拆出单独 issue：<https://github.com/tikv/pd/issues/10335>

---

## 一、设计目标与边界

### 1.1 In Scope
- Resource Group settings 的增删改（含 keyspace 维度）。
- controller config 写。
- service-limit metadata 写。
- 微服务模式下 RM metadata 同步机制（watcher + 启动一致性）。
- gRPC/HTTP/client 路由收敛到“metadata 写由 PD 生效”。

### 1.2 Out of Scope
- token 运行态逻辑重写（AcquireTokenBuckets 主流程保持）。
- 非 RM 组件的大规模接口改造。
- 一次性大迁移（采用渐进切流）。

### 1.3 强约束（必须满足）
- 不能出现路由重复注册冲突（`/resource-manager/api/v1/` 单路径注册）。
- 不能在 PD 场景复用依赖 `*rmserver.Server` 强转的旧 API handler（避免 panic）。
- 仅上收“写操作”；Get/List 读能力保持兼容（RM 可读 watcher cache 或转发）。
- `TokenOnly/MetaOnly` 模式下写盘行为要有显式 gate 且有测试断言。
- 不再依赖“tokens=0 + PatchSettings”这类脆弱语义，必须有 settings-only apply 路径。
- 明确 legacy/keyspace 双路径 precedence 与回滚策略。

---

## 二、关键技术决策

### 2.1 写角色模型
- 引入写角色：
  - `LegacyAll`
  - `PDMetaOnly`
  - `RMTokenOnly`
- 默认仍为 `LegacyAll`（先不改线上行为），逐 PR 切流。

### 2.2 Handler 策略
- 新增 PD 专用 metadata handler（独立于 RM server 类型）。
- `redirector.NewHandler` 保留为兼容入口，在内部路由到新 handler/转发逻辑。

### 2.3 gRPC 与 client 路由
- gRPC：Add/Modify/Delete 写在 PD proxy 本地处理；RM 写接口返回 `FailedPrecondition`。
- gRPC 读：Get/List 保留。
- client：写 RPC（Add/Modify/Delete）固定 PD；读/Token（Get/List/AcquireTokenBuckets）优先 RM discovery，缺失时回退 PD。

### 2.4 watcher 一致性
- 启动阶段要求“同 revision 载入 + 追平后再服务”。
- 逐步覆盖 settings/states，再补 controller/service-limit。
- 覆盖 compaction、leader/primary 切换等恢复路径。

---

## 三、每个 PR 的标准流程（必须执行）

每个 PR 都严格走三阶段：**提出 PR → fix comments → merge**。

### 3.1 提出 PR（Draft）
- 分支命名：`codex/rm-prX-...`
- PR 描述必须包含：
  - In scope / Out of scope
  - 风险与回滚方案
  - 测试清单（本 PR 最小验证）
  - 行数预算（目标值 + 实际值）

### 3.2 Fix Comments（最多两轮）
- Round 1：先修 correctness（崩溃、兼容、错误码、语义回归）。
- Round 2：再修 maintainability（命名、结构、复用、测试可读性）。
- 规则：只处理本 PR 范围，避免“顺手改大面”。

### 3.3 Merge Gate
- 通过本 PR 对应最窄单测/集成测试。
- 明确“无回归点”逐条勾选。
- 行数仍需 `< 500`；超出必须拆分后再合并。

---

## 四、PR 拆分（执行版）

## PR0（条件执行，<=120）
**Title**: `revert: rollback PR #10185 commits if present`
- 提出：检查 `0323b183 / df3874e9 / ae27572d / 3d28a5d6` 是否存在于当前历史，存在则回滚，不存在则 no-op。
- fix comments：仅处理“是否误回滚/漏回滚”。
- merge：工作树与 master 基线一致，行为不偏移。

## PR1（<=350）
**Title**: `rm: introduce manager write roles and gates`
- 状态：Merged（2026-02-12，<https://github.com/tikv/pd/pull/10227>）
- 提出：引入 `LegacyAll / PDMetaOnly / RMTokenOnly` gate，默认 `LegacyAll`。
- fix comments：重点验证 mode gate 与默认行为不变。
- merge：gate 单测通过，默认路径无回归。

## PR2（<=300）
**Title**: `rm: add settings-only apply path`
- 状态：Merged（2026-02-13，<https://github.com/tikv/pd/pull/10234>，独立于 PR1）
- 提出：新增 settings-only apply，绕开 token delta patch 语义。
- fix comments：重点验证 metadata 更新不改 tokens。
- merge：`Modify metadata -> tokens unchanged` 强断言通过。

## PR3（<=450）
**Title**: `rm: add PD metadata handler with redirector local routing`
- 状态：Merged（2026-03-16，<https://github.com/tikv/pd/pull/10246>）
- 提出：新增 PD 专用 handler，并在 redirector 内本地处理 metadata config 路由。
- fix comments：重点验证 keyspace 场景、错误码、路径兼容与 panic 风险。
- merge：metadata `/config/**` 路由端到端可测，非 metadata 路由保持转发语义。
- follow-up：controller config API 的 all-or-nothing 语义问题拆分为独立 issue：<https://github.com/tikv/pd/issues/10335>

## PR4（<=420）
**Title**: `rm: move gRPC writes to PD proxy, keep RM reads/tokens`
- 状态：Draft（<https://github.com/tikv/pd/pull/10260>，当前 `DIRTY`，待基于 `master`/PR6 rebase）
- 提出：PD proxy 处理 Add/Modify/Delete；RM 写返回 `FailedPrecondition`；保留 Get/List。
- fix comments：重点验证“只禁写，不禁读”。
- merge：写路径全走 PD；RM Get/List 与现有集成行为一致。

## PR5（<=420）
**Title**: `client: split metadata and token connections`
- 状态：Merged（2026-02-28，<https://github.com/tikv/pd/pull/10255>）
- 提出：client 的写 RPC（Add/Modify/Delete）固定走 PD；读/Token（Get/List/AcquireTokenBuckets）优先走 RM discovery。
- fix comments：重点验证“写走 PD，读+token 走 RM discovery”的分流语义。
- merge：client 路由测试通过，token path 无回归，读路径保持 RM 运行态可见性。

## PR6（<=480）
**Title**: `rm: add metadata watcher v1 with revision-safe startup`
- 状态：Merged（2026-03-19，<https://github.com/tikv/pd/pull/10259>）
- 提出：settings + states watcher，保证同 revision 载入并追平后再服务。
- fix comments：重点修复 WaitLoad 窗口不一致风险。
- merge：启动一致性与最终一致性测试通过。

## PR7（<=480）
**Title**: `rm: watcher v2 for controller/service-limit + precedence + failures`
- 提出：补 controller/service-limit watch、legacy/keyspace precedence、故障矩阵。
- fix comments：重点验证 compaction 与 leader/primary 切换恢复。
- merge：四类故障场景全部通过并稳定。

---

## 五、接口/行为变更基线

### 5.1 保持不变
- `redirector.NewHandler` 签名与兼容入口路径。

### 5.2 渐进行为变更
- 微服务模式下 RM 写 RPC 返回 `FailedPrecondition`。
- metadata 写统一由 PD 生效。
- client 写 RPC 固定 PD；读+token 仍走 RM discovery（无 RM 时回退 PD）。

---

## 六、测试与验收矩阵

### 6.1 单测优先
- 写角色 gate。
- settings-only apply。
- PD metadata handler。
- watcher state machine 与 revision-safe startup。

### 6.2 集成测试分配
- PR4/PR5：gRPC 路由切流与 client 路由验证。
- PR6/PR7：watcher 一致性与故障恢复验证。

### 6.3 必测故障场景（PR7 收口）
- PD leader 切换期间写入语义稳定。
- RM primary 切换期间 watcher 连续。
- etcd compaction 后 watcher 重建恢复。
- RM 不可用时 PD metadata 写行为与错误语义稳定。

### 6.4 当前收尾项（2026-03-20）
- `#10260`：在 `#10259` 已合并后，重新基于 `master` 整理 stacked history，只保留 “route metadata writes via PD” 这一层行为变更。
- `#10335`：单独修复 controller config 更新的 API 级 all-or-nothing 语义，不阻塞 `#10260` 的 rebase。

---

## 七、Assumptions / Defaults
- “写操作”定义：resource group settings、controller config、service-limit metadata。
- token 运行态 state 不上收 PD，仍由 RM token path 管理。
- 若任一 PR 超过 500 行，优先拆测试或拆下一 PR，不压缩关键 correctness 逻辑。
