# NetFlow v9 / IPFIX 模板缓存工作原理

> goflow2 基于 FlowStore 的模板缓存机制:模板集从解码到写入、查询、TTL 过期、JSON 持久化的完整生命周期。

## 1. 总体设计

模板缓存在逻辑上分为四层:

```
网络设备 UDP 报文
      │  (utils/pipe.go NetFlowPipe.DecodeFlow)
      ▼
解码器层  DecodeMessageVersion → DecodeMessageNetFlow / DecodeMessageIPFIX
      │  遍历 FlowSet,识别模板集/数据集
      ▼
存储接口层 netflow.TemplateStore (AddTemplate / GetTemplate)
      │  按 路由器/版本/观测域/模板ID 定位
      ▼
实现层  TemplateFlowStore → flowstore.Store (泛型 KV,带 TTL 与钩子)
      │
      ├─ 持久化: JSON 落盘 (Manager + filePersistence,去抖)
      ├─ 观测:   Prometheus hooks (metrics.TemplateStoreHooks)
      └─ HTTP:   /store 端点查询全部模板
```

核心思想:**模板元数据按键存储,数据流按模板键查询解码**;生命周期事件(新增/访问/删除)通过钩子接到持久化与指标,完全不侵入解码器。

## 2. 存储键

### 2.1 解码器侧键

模板按四元组定位(`decoders/netflow/template_store.go`):

- `FlowContext{RouterKey}`:路由上下文,`RouterKey` 即报文源地址(`pkt.Src.String()`);
- `version`:协议版本(9/10);
- `obsDomainId`:观测域 ID(IPFIX),v9 固定为 0;
- `templateId`:模板 FlowSet ID(数据集 ID ≥ 256)。

### 2.2 打包键

- 内部 Map 键(`decoders/netflow/template_types.go:20`):

  ```
  templateKey = (uint64(version) << 48) | (uint64(obsDomainId) << 16) | uint64(templateId)
  ```

  用于快照与方法内索引,低位 16 bit 即模板 ID。

- 持久化 JSON 键(`utils/store/templates/persistence.go`):格式化为字符串 `"version/obs-domain/template-id"`(如 `"10/1234/256"`),重启后按 `/` 解析回三元组。

### 2.3 FlowStore 键

`TemplateFlowStore` 使用具名结构体键(`utils/store/templates/store.go:13`):

```go
type flowStoreTemplateKey struct {
    RouterKey   string
    Version     uint16
    ObsDomainID uint32
    TemplateID  uint16
}
```

## 3. 写入:模板集 → AddTemplate

### 3.1 解码路径

`decoders/netflow/netflow.go DecodeMessageVersion` 按版本分发:

- v9 → `DecodeMessageNetFlow`(读 header:版本/计数/系统Uptime/Unix秒/序列号/源ID);
- v10 → `DecodeMessageIPFIX`(读 header:版本/长度/导出时间/序列号/ObservationDomainId)。

两者进入 `DecodeMessageCommon`,逐 FlowSet 处理(`DecodeMessageCommonFlowSet`,netflow.go:318),流程:

1. 读 `FlowSetHeader{Id, Length}`,`Id` 决定分类;
2. **模板集**(v9 Id=0 / v10 Id=2)→ `DecodeTemplateSet`;v9 Id=1 / v10 Id=3 → Options 模板集(`DecodeNFv9OptionsTemplateSet` / `DecodeIPFIXOptionsTemplateSet`);
3. 对每条 `TemplateRecord`(或 Options 模板记录)逐个 `store.AddTemplate(ctx, version, obsDomainId, TemplateId, record)`。

### 3.2 存储行为

`TemplateFlowStore.AddTemplate`(`utils/store/templates/store.go:164`):

1. 构造 `flowStoreTemplateKey`;
2. `store.GetQuiet` 检查是否已存在(静默读取,不触发 hooks);
3. `store.Set` 写入;
4. 返回状态:`TemplateAdded`(新)或 `TemplateUpdated`(覆写),否则 `TemplateUnchanged`。

### 3.3 底层 Set 语义

`flowstore.Store.Set`(`pkg/flowstore/store.go:231`):

- 已存在:先判过期(过期且不可续期则先删),更新值;默认 `refreshTTLOnWrite` 开启 → 写入即续期 TTL;
- 新建:插入,计算 `expiresAt`,`ttl>0` 时才设置默认 TTL;
- 变更后触发 hooks(`OnSet` → 模板钩子 `OnAdd`)。

## 4. 查询:数据集 → GetTemplate

解码到 **数据集**(FlowSet Id ≥ 256)时(`DecodeMessageCommonFlowSet` 的 `Id >= 256` 分支):

1. `store.GetTemplate(ctx, version, obsDomainId, fsheader.Id)`;
2. 命中则按模板**具体类型**分发解码:
   - `TemplateRecord` → `DecodeDataSet` 按 `Fields` 解出 `DataFlowSet`;
   - `IPFIXOptionsTemplateRecord` / `NFv9OptionsTemplateRecord` → `DecodeOptionsDataSet` 按 `Scopes`/`Options` 解出 `OptionsDataFlowSet`;
3. 未命中返回 `ErrorTemplateNotFound`,该数据集无法解码(collector 层聚合错误并按限频静默告警)。

`GetTemplate` 走 `Get`(带访问钩子),因此可利用 `OnAccess` 做续期与访问指标;而查询是否命中模板的存在性检查用 `GetQuiet`,避免误触发。

## 5. TTL 与过期清扫

`flowstore.Store` 内置 TTL:

- 每个 entry 有 `expiresAt`,`Get`/`Set` 时惰性判过(`expiredLocked`);
- 默认值来自 `defaultTTL`(仅当 > 0 时启用);
- `refreshTTLOnRead`(即 `-templates.ttl.extend-on-access`):读取命中时重置 TTL(写入刷新默认开启,读取刷新默认关闭);
- 后台清扫:`StartSweeper(interval)`(`pkg/flowstore/store.go:421`)用 `time.Ticker` 周期调 `ExpireStale()` 批量清理过期项;`interval<=0` 则不启动。

`TemplateFlowStore.Start()` → `store.Start(sweepInterval)`,由 app 装配的默认值驱动:

| 配置 | 默认值 | 含义 |
| --- | --- | --- |
| `-templates.ttl` | `0` | TTL,0 = 永不过期 |
| `-templates.sweep-interval` | `1m` | 清扫周期 |
| `-templates.ttl.extend-on-access` | `false` | 读取时续期 |

TTL=`0` 时不设置 `defaultTTL`,条目永不过期,清扫仍是空转,开销可忽略。

## 6. 持久化(重启恢复)

### 6.1 触发

`utils/store/persistence/store_persistence.go Manager.NewTemplateStore`:

1. `ensureFilePersistence()` 复用共享落盘器;
2. 挂持久化钩子:`templates.PersistenceHooks(notifyChange)`——**只监听 OnAdd 与 OnRemove**(`utils/store/templates/persistence.go:15`),即仅"变更"触发通知,读取不触发;
3. 挂关闭钩子,`Close` 时落最终快照。

### 6.2 落盘

`notifyChange` → 去抖调度 → `flush`:

- 将 `MarshalJSONSnapshot`(经 `GetAll` 快照,键转 `"v/obs/tid"` 字符串)写入指定文件;
- 原子写策略(tmp + rename),避免崩溃损坏;
- `-store.json.interval`(默认 10s)控制合并写入频率,`-store.json.path` 指定文件路径。

Notes:模板与采样率(`samplingrates`)共用同一持久化文件,各自按 key 段存储(`documentTemplatesKey` / `documentSampleRatesKey`)。

### 6.3 预加载

启动时 `preload(documentTemplatesKey, LoadJSON)`:

- 读文件,按版本/字段解回正确的模板类型(`TemplateRecord` / Options 模板);
- 逐条 `AddTemplate` 重建缓存,实现**重启不需重等模板集到达**;
- 文件不存在则不阻塞启动。

## 7. 观测(metrics)与 HTTP

### 7.1 Prometheus hooks(`metrics/store_hooks.go:13`)

```
OnAdd    → goflow2_netflow_templates{router,version,obs_domain_id,template_id,type} 计数; Added/Updated 时间戳
OnAccess → 访问时间戳(供"模板活跃度"监控)
OnRemove → 系列注销
```

`type` 标签区分普通 `template` 与 `options_template`。

### 7.2 HTTP /store

HTTPServer 的 `StoreHandler` 枚举 `GetAll()`:`router → { 打包键: 模板 }`,用于在线查看各路由器下全部模板及其缓存状态。

## 8. 关键代码位置

| 用途 | 位置 |
| --- | --- |
| 接口 FlowContext / TemplateStore / ManagedTemplateStore | decoders/netflow/template_store.go |
| 模板状态与打包键 | decoders/netflow/template_types.go |
| FlowSet 分类与模板读写调用 | decoders/netflow/netflow.go(DecodeMessageCommonFlowSet:318) |
| 版本分发 | decoders/netflow/netflow.go(DecodeMessageVersion:517) |
| 管道装配 ctx + templateStore | utils/pipe.go(NetFlowPipe.DecodeFlow:156) |
| 模板流程存储实现 | utils/store/templates/store.go |
| TTL 泛型存储 | pkg/flowstore/store.go(Set:231 / GetQuiet:298 / StartSweeper:421) |
| JSON 快照读/写 | utils/store/templates/persistence.go |
| 落盘管理器 | utils/store/persistence/store_persistence.go |
| Prometheus hooks | metrics/store_hooks.go |
| app 装配/默认值 | pkg/goflow2/app/app.go、pkg/goflow2/config/config.go |
| 采集与错误静默 | pkg/goflow2/collector/collector.go |
