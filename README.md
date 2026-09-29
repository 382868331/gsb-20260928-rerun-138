# 多来源数据连接框架（Go）

这是一个零外部账号、零网络依赖的基础框架。首轮提供两个虚构本地来源，并将分页读取统一为批次输出：

- `local-users`：可变键值表，`Begin` 时生成并持久化不可变快照，后续增删不影响当前扫描。
- `local-events`：事件日志，快照固定高水位，新增事件属于下一轮；删除事件不影响已持久化的快照。

## 核心边界

框架层只协调三件事：连接器、进度、数据身份。

- `Connector.Begin`：由来源创建明确的 `snapshot_id`。
- `Connector.Resume`：由来源解释自己的不透明 `cursor` 并恢复到同一快照。
- `Checkpoint.Cursor`：对框架是字符串，不假定为普通页码。
- `Identity`：固定为 `source/entity/id`，不包含页码、偏移或快照。
- `Runner`：先持久化输出批次，再原子推进 checkpoint。

因此，崩溃在“输出已保存、进度未保存”之间时，恢复会重新输出上一批；重复记录拥有相同稳定身份，下游可幂等去重。框架不会把变化数据上的普通页码伪装成可靠语义。

## 输出与状态

默认演示目录由 `--state-dir` 指定：

- `output/<source>.jsonl`：统一 JSON Lines 批次输出，允许崩溃恢复产生重复行。
- `progress/<source>.checkpoint.json`：原子替换的来源进度。
- `snapshots/<source>/<snapshot_id>.json`：本地模拟来源的可恢复快照。

## 本地演示

第一轮在第 2 个批次后注入崩溃（该批次输出已落盘，checkpoint 仍停在上一批）：

```bash
go run ./cmd/datapipe-demo --state-dir ./tmp/demo --crash-after-batch 2
```

恢复时修改虚构用户表。恢复仍读取旧快照，新增和删除不会导致漏读：

```bash
go run ./cmd/datapipe-demo --state-dir ./tmp/demo --mutate-on-resume
```

用户源会出现 7 条原始输出：5 个唯一身份 + 崩溃窗口重放的 2 条重复身份。

## 测试

```bash
go test ./...
```

测试覆盖：

- 翻页期间新增、删除记录时，恢复仍绑定原快照。
- 事件日志高水位游标忽略恢复时的追加和截断。
- 输出成功但进度未推进时，产生可按身份识别的重复。
- 完成后的 checkpoint 不再回放。
- 快照文件支持“新连接器实例”恢复，等价于进程重启。
- 游标由来源编码，框架不把它解释为页码。

## 新增来源

新增来源时实现 `internal/datapipe` 中的两个接口：

1. `Begin(ctx) (Scan, error)`：创建或固定来源快照，返回 snapshot id。
2. `Resume(ctx, checkpoint) (Scan, error)`：校验 snapshot id 并解析来源自有游标。
3. `Scan.Next(ctx)`：返回一批 `Record` 和下一个不透明游标；空游标表示完成。
4. 保证每个记录的 `Identity` 在跨页、重试和快照恢复后稳定。

无需修改 `Runner`、进度文件格式或输出协调逻辑。若真实来源不能恢复旧快照，应在 `Resume` 明确返回“快照过期/需要新快照”，由上层决定重新全量或改用来源支持的键集/变更游标。

