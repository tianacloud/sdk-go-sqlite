# Tiana SQLite Go SDK

[English](README.md) | **简体中文**

基于 Tiana SQLite 的原生 Go `database/sql` 驱动。
模块路径为 `github.com/tianacloud/sdk-go-sqlite`，包名 `tianasqlite`，
驱动名 `tiana-sqlite`。要求 Go 1.25+，建议使用受支持且已修补安全漏洞的工具链。
客户端不依赖 CGO。

驱动通过 [sdk-go](https://github.com/tianacloud/sdk-go) 建立经过证书验证的
TLS 1.3 / HTTP2 CONNECT 隧道，使用 `hrana-http` profile，再通过
`/v3/pipeline` 发送 Hrana 3 JSON。SQL 和参数不会拼接进 URL。

## 安装与发布状态

版本标签为 `v1.0.0`。配置仓库访问权限后安装：

```sh
go get github.com/tianacloud/sdk-go-sqlite@v1.0.0
```

sdk-go 依赖固定为 `v1.0.0`（提交
`ad9dfa0b943a7ba2f15984fa88d07c797b3239d3`）。独立构建使用远端模块及
校验和，不依赖本地 replace、workspace 或相邻源码。

```sh
GOWORK=off go mod download
```

目前仓库需要经过认证的 GitHub 访问。配置 Git 凭据或 SSH 路由，并将
GOPRIVATE 限定为两个 SDK 的模块路径。公共依赖继续使用正常校验服务，
go.sum 记录下载的 SDK 校验和。匿名下载和托管 CI 需先解决仓库访问权限。

构建始终使用 `GOWORK=off`，按锁定版本获取远端 SDK，不选择相邻源码副本。

## 通过 database/sql 连接

```go
package main

import (
    "context"
    "database/sql"
    "log"
    "os"
    "time"

    tiana "github.com/tianacloud/sdk-go"
    tianasqlite "github.com/tianacloud/sdk-go-sqlite"
)

func main() {
    token, err := tiana.NewToken(os.Getenv("TIANA_TOKEN"))
    if err != nil { log.Fatal(err) }
    connector, err := tianasqlite.NewConnector(tianasqlite.Config{
        Gateway: tiana.Config{
            Endpoint: os.Getenv("TIANA_ENDPOINT"),
            Token: token,
        },
        RequestTimeout: 30 * time.Second,
    })
    if err != nil { log.Fatal(err) }
    db := sql.OpenDB(connector)
    defer db.Close()
    db.SetMaxOpenConns(4)
    db.SetMaxIdleConns(4)
    db.SetConnMaxIdleTime(30 * time.Second)

    ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
    defer cancel()
    var answer int64
    if err := db.QueryRowContext(ctx, "SELECT ?", int64(42)).Scan(&answer); err != nil {
        log.Fatal(err)
    }
    log.Print(answer)
}
```

Endpoint 必须是部署提供的完整主机名，例如
`ep-01j5c9m7q2v8x4k6n3r0t1w2yz.db.example.test`（仅作示例），
满足 sdk-go 的 Endpoint ID 与 DNS 格式要求。可通过 Gateway.RootCAs、
Gateway.DialAddress 配置测试证书或自定义路由，证书校验保持开启。
连接池中的每个连接拥有独立的 SDK Client、隧道和 Hrana 会话。

驱动不会隐式登录、读取环境变量或本地凭据文件。示例显式读取环境变量。
应用可在完成实例授权后，通过 sdk-go/auth 获取 InstanceToken；
账户 access token、refresh token 不能用于 Gateway 隧道。

对于允许匿名访问的 Endpoint，可省略 Token，或使用：

```go
db, err := sql.Open("tiana-sqlite", "tiana://ep-01j5c9m7q2v8x4k6n3r0t1w2yz.db.example.test")
```

DSN 只接受 Endpoint，不允许凭据、查询参数、路径或端口。
凭据和 TLS 配置通过 NewConnector 传入。sql.Open/OpenDB 延迟建立连接；
可使用 PingContext 验证到 SQLite App 的连通性。

## 查询、参数与预备语句

支持 ExecContext、QueryContext、QueryRowContext、PrepareContext 及对应的
标准 database/sql API。预备语句在本地保存 SQL，每次执行时发送；
首版不创建服务端预备语句缓存。

支持位置参数 `?`，或 `:name` 配合 `sql.Named("name", value)` 的命名参数。
两种绑定方式不可混用。App 验证单条 SQL 及完整的参数绑定；
首版不提供 SQL 脚本或多语句执行接口。

| Go 输入 | Hrana / SQLite 值 |
| --- | --- |
| nil、值为 nil 的 []byte | NULL |
| int64 及可转换的整数类型 | 有符号 64 位整数，不经浮点转换 |
| 有限 float64 | 浮点数 |
| bool | 整数 0 或 1 |
| 合法 UTF-8 string | 文本 |
| 非 nil 的 []byte，包括空切片 | Blob |
| time.Time | RFC3339Nano 文本 |

NaN、无穷大、整数溢出和非法 UTF-8 会在发送 SQL 前被拒绝。
结果值为 nil、int64、float64、string 或 []byte；时间字符串不会自动解析成 time.Time。
提供列名和声明类型，不为 SQLite 动态类型虚构可空性或扫描类型元数据。
支持 RowsAffected；App 未提供插入行 ID 时，LastInsertId 返回错误。

## 事务与连接池

使用 BeginTx、Commit、Rollback；事务始终绑定同一个 Hrana 会话。
每次 SQL 执行后，在同一 pipeline 请求中通过 get_autocommit 校验事务状态。
支持默认和 serializable 隔离级别；只读事务及其他隔离级别返回明确的不支持错误。

请使用 sql.Tx 管理事务。原始 BEGIN、COMMIT、ROLLBACK、SAVEPOINT、RELEASE、
END 会被拒绝。服务端事务状态意外变化时，该连接会被废弃，避免污染连接池。
Commit 失败不会自动重试。修改连接级 PRAGMA 或使用临时对象时，
应持有专用 sql.Conn；连接池不保证每次查询都获得全新的 SQLite 会话。

根据部署设置池容量。空闲 Hrana baton 会按 App 的 stream TTL 过期；
ConnMaxIdleTime 应小于该 TTL。示例中的 30 秒假设服务端 TTL 更大，例如 60 秒。
驱动不发送后台保活请求。

## Shell 专用会话

`NewSession(Config)` 提供独占会话，支持原始 BEGIN IMMEDIATE/EXCLUSIVE、
SAVEPOINT、ROLLBACK TO、RELEASE 和 COMMIT。配置检查不联网，首次 Execute
才连接；调用串行执行，不连接池化、不重连、不重试。database/sql 接口仍要求
使用 sql.Tx 管理事务，原有保护不变。

```go
session, err := tianasqlite.NewSession(config) // 与 NewConnector 使用相同配置
if err != nil { return err }
defer session.Close()
result, err := session.Execute(ctx, "SELECT 42")
if err != nil { return err }
_ = result // 有序 Columns、带类型 Rows、Affected、LastInsertRowID
```

Execute 接受单条原始 SQL，不提供参数绑定；应用的绑定查询请使用 database/sql。
Result 保留 Hrana JSON 的整数文本、base64 BLOB 和 NULL，不经过浮点数转换。
Autocommit 返回最近确认的状态和 known 标志；新会话处于已知自动提交状态，
失败或关闭后状态未知。Shell 退出时可对已知活动事务显式执行 ROLLBACK，再 Close。
ExecuteAndClose 将 execute/get_autocommit/close 放在同一 pipeline 中。
确认成功的 Close 会回滚未提交事务；失败会话的 Close 仅释放本地资源，不能证明
远端已清理。必须检查执行错误，后续 Close 不能消除响应丢失带来的不确定性。
CloseContext 允许回滚与关闭共享同一清理预算；ExecuteAndClose 即使收到已取消
的 context 也会释放本地资源，补充关闭失败会报告结果未知。
调用通过会话锁串行化，请求超时从获得锁后开始计时。
脚本解析、历史、格式化与账号登录由调用方负责。

## 取消、资源限制与错误

RequestTimeout 默认 30 秒；调用方 deadline 可以进一步缩短时间。
取消连接建立或请求 context 会中断 I/O。建立连接时使用的 context
不会控制该连接后续的空闲生命周期。

HTTP 响应头上限为 32 KiB，编码后的请求和响应正文上限为 8 MiB。
结果在限制范围内完整缓冲，首版不是流式游标接口；
JSON 解码及 Go 对象还会占用额外内存，正文上限不等于总内存上限。
当前 baton 已知时，Close 会尝试在最多三秒内关闭 Hrana 会话，
随后释放 SDK 隧道和 Client。

完整 HTTP 拒绝响应中的 `BATON_INVALID` 表示当前操作未执行
（`OutcomeUnknown=false`），但原会话仍不可用。其他结果确定的 SQL 错误可能
保留部分写入，不能仅凭该布尔值决定重试。交互客户端可结合执行前已确认的事务
状态决定是否新建 Session 并重试被拒绝的操作；新会话不会恢复连接级状态。

请求可能已经发出后，传输或协议失败会返回 OutcomeUnknown=true 的
`*tianasqlite.Error`，**不会返回 driver.ErrBadConn**，防止 database/sql
自动重放可能已经提交的写入。无效连接会被丢弃，后续独立操作可建立新连接。
是否重试应由应用根据已确认状态判断；可通过 errors.Is 获取 context 原因。

OutcomeUnknown=false 不保证“没有副作用”，也不代表可以安全重试。
SQLite INSERT OR FAIL 可能保留语句的一部分修改，同时返回约束错误。
需要应用级原子性时，应使用显式事务并回滚。

错误只暴露受限的已知代码，不回显任意服务端错误消息、SQL、参数或凭据；
未知代码归为通用错误。驱动自身不输出日志。
HTTP 重定向和 Hrana base_url 重定向均被拒绝。

响应丢失后，新的 baton 可能未知。关闭传输连接不能证明回滚或提交结果；
App 可能保留会话及写锁，直到 TTL 清理。
Commit 成功沿用 App 的持久化语义，SDK 不额外保证数据已同步到对象存储。

## 验证与首版范围

```sh
go test -race -timeout 2m ./...
go vet ./...
python3 scripts/check-public-source.py
# 可选：真实 App 库和本地 SQLite，经原生 SDK TLS/H2 CONNECT 通信。
bash scripts/test-app.sh /path/to/app_sqlite
```

未通过脚本提供测试程序时，真实 App 集成测试会跳过。
Rust 仅用于集成夹具，普通 SDK 消费方不需要安装。
脚本只读构建外部 App 仓库，产物位于 .artifacts。
参考 app_sqlite 版本为 bccd78e3040c9463e802f3538f4f9ffe0fcd9522。

测试覆盖值类型、参数绑定、事务、连接池 context 生命周期、
非法或超大响应、脱敏，以及写入/提交响应丢失时不重放。
测试中的 Gateway 是合成 TLS/H2 中继，不是生产 Gateway/Agent 部署。
生产鉴权、自动休眠和存储持久化仍需部署环境验证。

首版不包含 Hrana WebSocket/protobuf、流式游标、多结果集、
服务端语句缓存、本地 SQLite 文件或自动账户登录/令牌刷新。
可运行示例见 examples/basic。

使用 Apache-2.0 许可证，见 [LICENSE](LICENSE)。

`Session.RequestID()` 返回已建立 CONNECT 连接的诊断 ID。会话返回的 `*tianasqlite.Error` 在 `RequestID` 字段保留该 ID；建连错误保留底层 SDK 的诊断身份。配置 `Gateway.OnRequestID` 可在网络操作前收到 ID，包括握手失败的情况。

### 运行基础示例

设置 `TIANA_ENDPOINT` 后运行 `go run ./examples/basic`。可选环境变量为
`TIANA_TOKEN`、`TIANA_CA_FILE`（PEM 证书）和 `TIANA_GATEWAY_ADDRESS`（host:port）。
自定义证书替换系统根证书池，仍验证证书和 Endpoint 主机名；证书不可读或无效时
在连接前报错。示例仅执行带参数的 SELECT，不创建持久化表。
