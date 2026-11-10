# FTDI-Core v0.4.3 使用手册

> 本地运行的**行情数据可信性基础设施**（研究原型）：接收历史行情、按事件持久化、跨进程投递给独立的消费端，
> 并能逐条事件审计"有没有丢、有没有重复、有没有被改"。
> **不是交易程序**：没有下单、账户、私钥功能；不联网（只用本机 127.0.0.1）；**未经生产认证**。

---

## 1. 环境与安装

- Windows 10/11 或 Linux（已在 Ubuntu 24.04 上验证），Python **3.10+**（只用标准库，无需安装任何依赖包）。
- **把运行数据放在固态硬盘上**。机械硬盘会让投递慢约 6 倍（见 `docs/PERF_WINDOWS_DELIVERY.md`）。
- 解压发布包 `FTDI-Core-v0.4.3-windows.zip`（Linux 同样适用），在解压出的 `FTDI-Core-v0.4.3` 目录里打开 PowerShell。

一键自检（全部测试 + 命令行冒烟，约 1 分钟）：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\scripts\Verify-FTDI.ps1
python scripts\Check-Release-Manifest.py      # 校验发布包每个文件的 SHA-256
python -m ftdi_core --version                  # ftdi-core 0.4.3
```

## 2. 一条完整的数据链路

```text
原始 ZIP + 校验文件 ─prepare─> 规范化 JSONL ─replay─> 生产端（逐条日志 + 有序 outbox）
     ─lab-e2e─> 独立消费端进程（本机 HTTP，先落库再确认） ─audit-e2e-archive─> 逐条对账结论
     ─archive-producer─> 生产端历史归档      ─consumer-retention─> 消费端历史压缩（两者先后不限）
```

### 2.1 准备与回放

```powershell
python -m ftdi_core prepare --archive D:\data\BTCUSDT-aggTrades-2024-06-01.zip `
    --checksum D:\data\BTCUSDT-aggTrades-2024-06-01.zip.CHECKSUM --outdir C:\ftdi\prepared --limit 0
python -m ftdi_core replay --lane "BTC=C:\ftdi\prepared\BTCUSDT-aggTrades-full-core-v1.jsonl" `
    --outdir C:\ftdi\run --batch 1024 --max-pending 4096 --max-backlog 8192
python -m ftdi_core audit --outdir C:\ftdi\run
```

- `prepare` 会核对 ZIP 的 SHA-256 与配套校验文件（Binance 官方命名为 `.zip.CHECKSUM`，内容是 `哈希  文件名`；文件名不同也可以，例如您现有的 `BTCUSDT-aggTrades-2024-06-01.zip.zip`），保留原始 CSV 的全部 8 列；原始 ZIP **只读、永不修改**。
- `--limit 0` 表示全量；不加时默认只取前 50,000 条。
- 有缺口、冲突或格式错误时，回放会**停下并报告**，绝不跳过。

### 2.2 投递给独立消费端并对账

```powershell
python -m ftdi_core lab-e2e --outdir C:\ftdi\run --consumer-db C:\ftdi\consumer\effects.sqlite3 --batch 1024
python -m ftdi_core audit-e2e-archive --outdir C:\ftdi\run --consumer-db C:\ftdi\consumer\effects.sqlite3 `
    --report C:\ftdi\reports\audit.json
```

- `lab-e2e` 会启动一个只监听 127.0.0.1 的消费端进程。消费端**先提交、后确认**；中断后用同样的路径重跑即可续传，不会重复记账。
- **推荐始终使用 `audit-e2e-archive`**：只读，覆盖归档和压缩后的数据，逐条核对事件身份、内容哈希和源文件每一行。
  `audit-e2e`（v0.2 版）只看驻留数据，遇到归档会按设计直接拒绝。

## 3. 审计结论与退出码

| 状态 | 含义 | 退出码 |
|---|---|---:|
| `COMPLETE` | 每一条事件：源文件 → 生产端 → 消费端，身份与内容全部一致；审计期间证据未被改动 | 0 |
| `PENDING_DELIVERY` | 证据一致，只是还有事件未投递 → 再跑 `lab-e2e` | 2 |
| `RETRY_REQUIRED` | 消费端已落库、生产端确认丢失 → 再跑 `lab-e2e`（安全重发，不会重复记账） | 2 |
| `INCONSISTENT` | 证据缺失、冲突、被篡改、序列有缺口或审计期间被改动 → 看报告里的 `issue_counts` | 2 |
| 前置条件失败 | 文件不存在、路径非法等，输出 `FTDI_CORE_FAIL_CLOSED: ...`，**不会生成报告** | 3 |

报告里每个问题都有代码（例如 `ARCHIVE_SHA256_MISMATCH`、`CONSUMER_DIGEST_MISSING`），完整含义见
`docs/ARCHIVE_E2E_AUDIT.md` 和 `docs/CONSUMER_RETENTION_V2.md`。标为 NOTICE 的只是提示，不影响结论。

## 4. 历史归档与压缩（节省空间）

**所有会改动数据的命令，不加 `--apply` 都只演练、什么都不改。** 执行前请停掉投递（维护窗口），执行后跑一次审计。

### 4.1 生产端归档

```powershell
python -m ftdi_core archive-producer --outdir C:\ftdi\run --lane BTC --consumer-db C:\ftdi\consumer\effects.sqlite3 --limit 500000           # 演练
python -m ftdi_core archive-producer --outdir C:\ftdi\run --lane BTC --consumer-db C:\ftdi\consumer\effects.sqlite3 --limit 500000 --apply   # 执行
```

- **每次演练和执行前都会先自动跑一次完整审计**（`audit-e2e-archive`）。只要整个运行目录里任何一个通道有致命问题
  （状态为 `INCONSISTENT`，例如篡改、ACK 账不一致、未解决的序列缺口、源文件被改），就**拒绝归档**并列出问题代码，
  因为归档会删除这些问题的证据。审计结果为 `COMPLETE`、`PENDING_DELIVERY` 或 `RETRY_REQUIRED` 时才继续。
  全天数据的这一步约 1.5 分钟。
- 只归档**连续的、已确认的、并且消费端有独立回执**的前缀。回执可以是消费端的驻留行，也可以是压缩后的指纹；
  按设计被判为重复或隔离、从未投递的原始记录不需要回执。消费端库必须是独立的数据库，不能是生产端的库。
  每段最多 20,000 条：先写成 gzip 段、落盘并校验 SHA-256，再在同一个事务里删除库内对应的行，并核对删除的行数；
  生产端库里如果有触发器（删除可能带出别的行）一律拒绝。
- 每条记录在写入前都用审计自己的解析器检查一遍：**审计读不回来的记录绝不写进归档**（直接拒绝，什么都不改）。
- 演练不修改任何数据或证据文件（SQLite 的共享内存协调文件 `-shm` 除外），并预测目标段文件是否冲突、
  是否有范围重叠的遗留段文件、临时文件位置是否被占用。
  在维护窗口内（没有程序在写），演练结果里的 `would_archive` 就是执行时会归档的条数；`receipts` 显示回执来源。
  如果演练期间（或执行前的审计期间）有程序改动了数据库，会报错，不给结论、不执行。
- `stopped` 说明停在哪里：`LIMIT_REACHED`（达到上限）、`NOT_ACKNOWLEDGED`（后面的事件还没投递确认）、
  `END_OF_JOURNAL`（已全部归档）等。
- 执行中途如果被拒绝或出错（退出码 3），报错信息会写明本次**实际已提交**的事件数和段数（从库里读回）。
  已提交的段都是完整、可审计的，修好问题后再次执行会从断点继续。
- `--chunk` 很小时速度会明显变慢（每段都要扫描一次相关表）；保持默认 20000 即可。

### 4.2 消费端压缩（Task 002）

```powershell
python -m ftdi_core plan-consumer-retention --consumer-db C:\ftdi\consumer\effects.sqlite3 --producer-root C:\ftdi\run
python -m ftdi_core consumer-retention migrate --consumer-db C:\ftdi\consumer\effects.sqlite3 --apply
python -m ftdi_core consumer-retention compact --consumer-db C:\ftdi\consumer\effects.sqlite3 --lane BTC `
    --producer-root C:\ftdi\run --watermark <规划给出的值或更小>            # 演练
python -m ftdi_core consumer-retention compact ... --apply                  # 执行
python -m ftdi_core consumer-retention rehydrate --consumer-db ... --lane BTC --producer-root C:\ftdi\run --apply   # 需要时恢复
```

- 老数据的完整内容移入 `effects.sqlite3.archive\` 下的校验归档，库里只保留每条事件的指纹，用来识别重复和冲突。
  全天实测：397 MiB → VACUUM 后 61 MiB，归档 51 MiB。
- 压缩后，即使生产端从头重发全部事件，也**不会重复记账**；旧版程序往压缩区间写入会被数据库直接拒绝。
- `rehydrate` 能把数据逐行原样恢复；**归档文件永远不会被删除**。数据库文件要执行 VACUUM 才会真正变小（本工具不自动执行）。
- 生产端归档与消费端压缩的先后顺序不限。

### 4.3 回收磁盘空间（`vacuum-db`，v0.3.1）

归档/压缩只是把行删掉，SQLite 文件本身不会变小。`vacuum-db` 重建一个数据库文件来回收空间：

```powershell
python -m ftdi_core vacuum-db --db C:\ftdi\consumer\effects.sqlite3            # 演练：可回收多少、需要多少空闲空间
python -m ftdi_core vacuum-db --db C:\ftdi\consumer\effects.sqlite3 --apply    # 执行
python -m ftdi_core vacuum-db --db C:\ftdi\run\lanes\BTC\state.sqlite3 --apply # 生产端库同理
```

- 全程独占该数据库：如果有程序正在读写它（或在 WAL 模式下读过它），就拒绝执行；执行期间别的程序无法读写它。
  执行前请先停掉所有使用该库的 FTDI 程序（维护窗口）。
- 先把原库**逐字节复制**成 `effects.sqlite3.pre-vacuum-<时间>`，并核对副本与原库逻辑内容完全相同，**然后**才让 SQLite
  在原文件上执行它自带的、带事务保护的 `VACUUM`，结束后再逐表逐行核对内容未变。数据库文件始终在原位，不会出现"文件不见了"的状态。
- **这份副本不会被删除**。确认无误（例如跑一次 `audit-e2e-archive`）后，由您自己决定是否删除它。
- 库文件是链接/硬链接、`-wal`/`-shm` 不是这个库自己的 SQLite 文件、库里有 FTDI 不认识的对象（视图、全文索引、
  额外的表或生成列等）一律拒绝，不做任何写入。
- 需要的空闲空间见演练结果里的 `space_needed_bytes`（一份副本加上整理用的临时空间）。全天实测：压缩后的消费端库约 400 MiB → 约 61 MiB。

## 5. 一键全天验收（推荐每次升级后执行）

```powershell
python scripts\acceptance_full_day.py --archive D:\data\BTCUSDT-aggTrades-2024-06-01.zip `
    --checksum D:\data\BTCUSDT-aggTrades-2024-06-01.zip.CHECKSUM --workdir C:\ftdi_acceptance
```

在一个**新目录**里跑完整链路，包括：两种顺序的归档与压缩、生产端重启重发、篡改检测、旧程序写入拦截、恢复一致性。
最后打印 `ACCEPTANCE PASS`，证据保存在 `C:\ftdi_acceptance\acceptance_evidence.json`。
全天数据在固态硬盘上约 25 分钟，占用约 4 GB；输入的 ZIP 只读，跑前跑后会核对哈希。跑完后可以删除 workdir。

## 6. 实时只读采集（Task 004，v0.4.0 起；v0.4.1 修正时钟证据）

把交易所公开的实时成交（aggTrade）采成一个个**封存的时段**，再交给上面同一条链路回放、投递、审计。
**不需要也不接受任何密钥，只读，不下单。**

```powershell
# 1) 采集一个时段（默认只允许本机地址；连接真实交易所必须显式加 --public-network）
python -m ftdi_core live-capture --symbol BTCUSDT --capture-dir C:\ftdi\live --public-network --max-seconds 600
# 2) 验证（只读）：每条事件都能从原始帧/REST 响应重新推导，序号连续，没有冲突
python -m ftdi_core verify-session --session C:\ftdi\live\BTCUSDT\session-<时间>
# 3) 接着采下一个时段，从上一段的最后序号 +1 无缝衔接
python -m ftdi_core live-capture --symbol BTCUSDT --capture-dir C:\ftdi\live --public-network --previous-session <上一段目录>
# 4) 把一条时段链拼成一个不可变文件，然后照常 replay / lab-e2e / audit-e2e-archive
python -m ftdi_core compose-sessions --session <段1> --session <段2> --output C:\ftdi\live_chain1
python -m ftdi_core replay --lane LIVE=C:\ftdi\live_chain1\events.jsonl --outdir C:\ftdi\live_run1
# 5) 时延证据（Task 003 规则；没有时钟模型就不给数字）
python -m ftdi_core analyze-clock-evidence --session <段目录>
```

- 每个时段保存：原始 WebSocket 消息与 REST 响应（收到即落盘）、规范化事件、时钟样本、清单。
- 缺口用公开 REST 按序号补齐；补不上就封存缺口之前的部分并停止（`GAP_BLOCKED`），**绝不跳过**。
  同一序号收到不同内容 → 停止（`HALTED_CONFLICT`），并把有歧义的部分截掉。
- 进程被杀：`python -m ftdi_core seal-session --session <目录> --apply` 从完整行恢复，原文件保留。
- 链变长后：从链头重新拼接到一个**新**目录、回放到一个**新**运行目录，投递给同一个消费端——已有事件只记重复。
- 退出码：0 正常结束（达到条数/时长/Ctrl-C），2 因缺口、冲突、格式错误、限流等提前结束（已封存），3 前置条件失败。
- **连接公网是您的决定**：本程序只访问 `data-stream.binance.vision` 与 `data-api.binance.vision` 两个公开行情地址。
- 时延报告的状态：`ESTIMATED`（有可信样本，给出带不确定度的估计）、`NO_TRUSTED_SAMPLES`（能校正但没有一个样本可信，
  例如时钟样本往返太慢）、`NO_CLOCK_MODEL`（没有时钟样本）、`INCONCLUSIVE`（负值过多，时钟模型不对）。
  想得到可信的时延数字，采集前先同步 Windows 时钟（管理员 PowerShell：`w32tm /resync`，再用
  `w32tm /query /status` 确认不是"Leap 指示器: 3"）。
- 2026-11-08 的真实网络试采（v0.4.0）：采集、验证、拼接、投递、审计全部通过（2,480 条，审计 COMPLETE），
  但没有时延数字；原因和 v0.4.1 的修正见 `docs/REAL_LIVE_PILOT_RESULTS.md`。
- 2026-11-09 第二次试采（v0.4.1）：5,560 条，审计 COMPLETE 且无任何提示，时钟样本往返 43–81 ms，修正全部生效；
  仍无可信时延——到交易所的往返至少 43 ms，不确定度至少 22.5 ms，超过 20 ms 上限。见 `docs/REAL_LIVE_PILOT2_RESULTS.md`。
- 第三次采集（每 5 秒一个时钟样本，121 个）：最快往返仍是 42.7 ms，说明这是本机到交易所的网络下限，多取样也无法
  达到 20 ms 的标准。仅作参考的推演：若把上限放宽到 25/30 ms，P50 约 40 ms（误差 ±25/±30 ms，未认证）。
- 在您的 Linux 云服务器上采集（每 5 秒一个时钟样本）：121 个样本中有 16 个往返低于 38 ms，得到**第一个可信的时延估计**：
  状态 ESTIMATED，1,147 条可信读数，P50 18.5 ms、P95 19.7 ms、P99 20.0 ms，每个值带最多 ±20 ms 的不确定度；
  用已同步时钟独立核对得 P50 19.5 ms。未经认证。见 `docs/REAL_LIVE_REMOTE_RESULTS.md`。
- 同一服务器晚上连续采 1 小时（54,438 条，审计 COMPLETE）：P50 35.4 ms（独立核对 36.56 ms），
  整小时都很稳定。比下午慢约 17 ms 的原因是每条连接走的网络路径不同（实测有约 35/51/67 ms 三档），不是测量误差。
- 时钟样本间隔（v0.4.2 起）：`--clock-interval-s 5` 表示每 5 秒取一个样本（默认 60；连公网时至少 1）。
  样本越多，越可能碰到一次很快的往返，不确定度就越小。
- 选路（v0.4.3 起，默认关闭）：`--path-probes 16 --path-probe-s 8` 会在当前连接旁边再开试探连接，比较同一时段的
  送达延迟，留下更快的那条。两条连接同时在收，不会产生缺口，也不需要 REST 补数。服务器上实测：每条连接要么约 19 ms、
  要么约 36 ms，开启后两次从 35 ms 切到了 19 ms；但如果试探的连接里没有快的，也只能保持原速。

## 7. 时钟证据分析（Task 003）

```powershell
python -m ftdi_core analyze-clock-evidence --pilot-db <实时试采的 capture.sqlite3>
```

只读分析实时采集数据库里的时间戳。在没有时钟偏差模型时，**不输出任何延迟或 P99**。
之前那次 200 条试采中，199 个负值是本机时钟比交易所慢约 0.6 秒造成的，不是"负延迟"。

## 8. 安全规则（产品内置，不要绕过）

- 只读行情：不下单、不需要也不接受任何 API Key。
- 只用本机 127.0.0.1 HTTP，不要暴露到局域网或公网。
- 原始 ZIP、源 JSONL、归档文件是证据：程序只读不改，归档永不自动删除。
- 缺口、冲突、篡改、容量耗尽一律**失败闭锁**，宁可停下也不报假"完整"。
- 审计期间任何证据文件发生变化，结论一律不是 `COMPLETE`。

## 9. 常见问题

| 现象 | 处理 |
|---|---|
| 投递很慢 | 把 `runs\`、消费端数据库放到固态硬盘；用 `--batch 1024` |
| `SNAPSHOT_UNVERIFIABLE`（WAL 存在但缺 `-shm`） | 复制数据库时要连同 `-wal`、`-shm` 一起复制；或先用写入它的程序正常打开并关闭一次，再审计 |
| `CONCURRENT_MODIFICATION_DETECTED` | 审计期间有程序在写数据；停掉投递/归档后重跑审计 |
| `ARCHIVE_PATH_RELOCATED` | 运行目录被整体搬动过；确认无误后加 `--allow-relocated-archives`（只读新位置，永不读旧路径） |
| 归档程序中途崩溃后再执行时报 `SNAPSHOT_UNVERIFIABLE` | 先运行一次 `python -m ftdi_core audit --outdir <运行目录>`（它会正常打开并收尾 WAL），再重新演练、执行 |
| `vacuum-db` 报 `database in use` | 还有程序打开着这个库（投递、审计、另一个窗口）；全部关闭后重试 |
| 归档被拒绝：`overlaps the range` | 之前一次用不同 `--chunk` 中断的运行留下了段文件；用当时的 `--chunk` 重试，或检查后把该文件移出 `archives\` |
| 压缩被拒绝 | 按提示处理：有缺口、生产端尚未确认、段过大（调小 `--segment-size`）、目录是链接等 |

## 10. 不能宣称的事实

- 不是生产系统；没有 24 小时在线稳定性、真实网络 P99、断电灾备认证。
- `COMPLETE` 只证明所提供区间内的本地证据链一致，不证明首个序号之前的数据完整，也不是交易所的完整性认证。
- 无法防御能同时改写源文件、生产端、归档和消费端全部证据的人（没有签名或只读存储）。
- 实时采集只证明时段内事件来自交易所且连续无冲突；不证明时段之外的行情，也不是 24 小时在线认证。
  时延数字（若有）只是带不确定度的估计，从未经过认证。

## 11. 文档索引

| 文档 | 内容 |
|---|---|
| `RELEASE_NOTES_v0.4.3.md`、`RELEASE_NOTES_v0.4.2.md`、`RELEASE_NOTES_v0.4.1.md`、`RELEASE_NOTES_v0.4.0.md`、`RELEASE_NOTES_v0.3.1.md`、`RELEASE_NOTES_v0.3.0.md` | 版本变化 |
| `docs/RELEASE_GATES_V04.md`、`docs/RELEASE_GATES_V03.md` | 发布门禁与证据 |
| `docs/LIVE_CAPTURE.md` / `docs/REAL_LIVE_PILOT_RESULTS.md`、`docs/REAL_LIVE_PILOT2_RESULTS.md`、`docs/REAL_LIVE_REMOTE_RESULTS.md` | 实时采集契约 / 真实网络试采结果 |
| `docs/ARCHITECTURE_V1_2.md` | v0.3 架构 |
| `docs/ARCHIVE_E2E_AUDIT.md` / `docs/CONSUMER_RETENTION_V2.md` | 审计 / 压缩的精确契约 |
| `docs/TASK003_TIME_PROVENANCE_DESIGN.md` | 时间溯源规则 |
| `docs/REAL_ARCHIVE_AUDIT_RESULTS.md` / `docs/REAL_CONSUMER_RETENTION_RESULTS.md` | 全天真实数据证据 |
| `docs/STORAGE_MAINTENANCE.md` | `vacuum-db` 的精确契约 |
| `docs/LIVE_CAPTURE.md` | 实时采集、时段验证、链拼接的精确契约 |
| `docs/ADRS.md` | 架构决策记录（ADR-001…011） |
