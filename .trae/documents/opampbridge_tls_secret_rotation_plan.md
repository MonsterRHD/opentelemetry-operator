# OpAMPBridge 后端 TLS Secret 轮换支持 实施计划

## 背景与需求（Repository Research）

平台管理员更新 OpAMPBridge 后端的 TLS Secret 后，要求：

1. bridge 以新证书重新建立到 OpAMP 后端的连接，Deployment 完成滚动、能力（capabilities）重新上报，三者最终一致；
2. 旧证书导致连接失败期间，CR 不能被标记为健康（Ready=True）；
3. 新 Secret 暂时不存在/不可读时，**不动**已有的 Deployment/ConfigMap（保留上一份可用配置），恢复后自动重试；
4. operator 重启后轮换仍能继续（无内存状态，全部由声明式 reconcile + watch 驱动）；
5. 未配置 TLS 的 bridge 与普通配置变更保持现有行为。

### 现状结论

- CRD：[opampbridge_types.go](file:///Users/ding/Documents/swe/09251/project-09/apis/v1alpha1/opampbridge_types.go#L129-L137) 的 `OpAMPBridgeTLSConfig` 只有 `Insecure`、`InsecureSkipVerify`，**无 Secret 引用**；`OpAMPBridgeStatus` 只有 `Version`，无条件字段。
- 控制器：[opampbridge_controller.go](file:///Users/ding/Documents/swe/09251/project-09/internal/controllers/opampbridge_controller.go) 仅 owns ConfigMap/ServiceAccount/Service/Deployment，不 watch Secret；[handle.go](file:///Users/ding/Documents/swe/09251/project-09/internal/status/opampbridge/handle.go) 只写 version。
- 资源构造：ConfigMap 直接把 `Spec.TLS` 按 yaml 序列化（[configmap.go](file:///Users/ding/Documents/swe/09251/project-09/internal/manifests/opampbridge/configmap.go#L30-L32)）；Pod 模板已有 ConfigMap 内容 hash 注解的滚动模式（[annotations.go](file:///Users/ding/Documents/swe/09251/project-09/internal/manifests/opampbridge/annotations.go)），无 Secret volume。
- bridge 二进制：[agent.go](file:///Users/ding/Documents/swe/09251/project-09/cmd/operator-opamp-bridge/internal/agent/agent.go#L228-L252) 只处理 insecure/skipVerify；capabilities 仅在 `Start()` 时上报一次；`OnConnect` 只打日志、`OnConnectFailed` 只记错误。
- 健康端点：[healthcheck/server.go](file:///Users/ding/Documents/swe/09251/project-09/cmd/operator-opamp-bridge/internal/healthcheck/server.go) 的 `/healthz` 恒返回 200，与 OpAMP 连接状态无关；容器 liveness/readiness 都指向它。
- 仓库先例：Instrumentation exporter TLS 用 `SecretName` + 约定键（[instrumentation_types.go](file:///Users/ding/Documents/swe/09251/project-09/apis/v1alpha1/instrumentation_types.go#L114-L136)、[exporter.go](file:///Users/ding/Documents/swe/09251/project-09/internal/instrumentation/exporter.go)）；collector 状态用 `metav1.Condition` Ready（[handle.go](file:///Users/ding/Documents/swe/09251/project-09/internal/status/collector/handle.go)）；TA controller 用 `Watches + EnqueueRequestsFromMapFunc` 监听非自有资源。

### 已确认的设计决策

- Secret 同时承载 CA（`ca.crt`）与客户端证书/密钥（`tls.crt`/`tls.key`，mTLS）；按实际存在的键渲染，至少要有 `ca.crt` 或成对的 `tls.crt`+`tls.key`。
- Ready 条件反映**真实连接状态**：bridge 仅在 OpAMP 连接成功后置就绪，operator 仅在 Deployment 滚动完成且 Pod 就绪后置 Ready=True。
- 轮换方式：Secret 内容 hash 写入 Pod 模板注解 → Deployment 滚动重启 → 新 Pod 加载新证书、重新建连并重报 capabilities。

## 文件与改动

### API 层（apis 模块）

- `apis/v1alpha1/opampbridge_types.go`
  - `OpAMPBridgeTLSConfig` 增加 `SecretName string`（`json:"secretName,omitempty"`）：同 namespace 内用户自管 Secret。
  - 增加**仅运行时**字段 `CAFile/CertFile/KeyFile string`，tag 用 `json:"-" yaml:"ca_file,omitempty"` 等：不进入 CRD schema，由 operator 渲染进 bridge 的 ConfigMap；加注释说明非 CRD API、用户不可设置。
  - `OpAMPBridgeStatus` 增加 `ObservedGeneration int64` 与 `Conditions []metav1.Condition`，kubebuilder 标记参照 v1beta1 collector status。
- `apis/v1alpha1/zz_generated.deepcopy.go`：由 `make update` 重新生成（status 新切片需要 deepcopy 代码）。

### operator 层（root 模块）

- `internal/naming/main.go`：新增 `OpAMPBridgeTLSVolume() = "opamp-bridge-tls"`；挂载路径常量 `/tls` 放在 manifests 包。
- `internal/manifests/params.go`：`Params` 增加 `TLSSecret *corev1.Secret`。
- `internal/manifests/opampbridge/configmap.go`：`secretName` 为空时输出与现在完全一致；非空时显式渲染 `tls` map（`insecure`/`insecure_skip_verify` 按值，`ca_file: /tls/ca.crt`、`cert_file: /tls/tls.crt`、`key_file: /tls/tls.key` 按 Secret 中实际存在的键输出；不输出 `secretName`）。
- `internal/manifests/opampbridge/volume.go`：`secretName` 非空时追加只读 secret volume（默认权限 0640/0400 沿用 k8s 默认即可，不额外设置）。
- `internal/manifests/opampbridge/container.go`：非空时追加 `/tls` 只读 VolumeMount。
- `internal/manifests/opampbridge/annotations.go`：新增 `opentelemetry-opampbridge-tls-secret/hash` 注解，对 Secret Data（按键排序后的键值字节）求 sha256；`PodAnnotations` 接收 params 中的 secret。
- `internal/manifests/opampbridge/deployment.go`：透传 params 给 `PodAnnotations`。
- `internal/webhook/opampbridge_webhook.go`：校验 `Insecure=true` 与 `SecretName!=""` 互斥（insecure 下证书无意义）。
- `internal/controllers/opampbridge_controller.go`：
  1. `secretName` 非空时先 Get Secret：
     - NotFound：**跳过** `BuildOpAMPBridge` 与对象 reconcile（上一份配置原样保留），置 Ready=False（Reason=`TLSSecretNotFound`），返回 `RequeueAfter`（约 5min 兜底，恢复主要靠 watch）；
     - 其他错误：直接返回 error 走退避重试（同样在构建之前，不改动对象）。
  2. 成功：`params.TLSSecret = &secret` 后走现有构建/对账流程。
  3. `SetupWithManager` 增加 `Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(...))`：map 函数在 Secret 所在 namespace 列出 OpAMPBridge，仅入队 `spec.tls.secretName == secret.Name` 者（不 Owns，Secret 为用户自管，避免被级联删除）。
  4. 增加 RBAC marker：`//+kubebuilder:rbac:groups="",resources=secrets,verbs=get;list;watch`。
- `internal/status/opampbridge/handle.go`：
  - reconcile 出错：Ready=False（Reason=`ReconcileError`，message=错误），patch 后返回原错误（对齐 collector 写法），保留 Warning event。
  - 成功：读取同名 Deployment 的 status：`ObservedGeneration>=Generation && UpdatedReplicas>=Replicas && AvailableReplicas>=Replicas`（Replicas 默认 1）→ Ready=True（Reason=`Reconciled`），否则 Ready=False（Reason=`DeploymentNotReady`，消息说明滚动/连接尚未就绪）。Deployment 尚不存在时同样 False。
  - 始终写 `ObservedGeneration`，保留原 version 逻辑。
  - 新增导出的 degraded 入口（如 `HandleDegradedStatus`）供 Secret 缺失场景：写 Ready=False + version、返回 `ctrl.Result{RequeueAfter}`、不返回 error。
- 生成文件（`make update`）：`config/crd/bases/opentelemetry.io_opampbridges.yaml`、`config/rbac/role.yaml`、`bundle/community|openshift/...`、`docs/api/opampbridges.md`。
- `.chloggen/`：新增 `enhancement` / component `opamp` 条目（CRD schema 与行为变更，用户可见）。
- 文档：`docs/opamp-bridge/README.md` 增补 TLS Secret 用法小节（Secret 键、挂载路径、轮换行为）。

### bridge 二进制（root 模块，cmd/operator-opamp-bridge）

- `internal/agent/agent.go`：
  - 新增 `buildTLSConfig(*v1alpha1.OpAMPBridgeTLSConfig) (*tls.Config, error)`：系统根池追加 `CAFile`（存在时）；`CertFile/KeyFile` 成对时 `tls.LoadX509KeyPair`；文件不可读/证书无效返回错误；继续兼容 InsecureSkipVerify。
  - `Start()` 中非 insecure 且配置了文件路径时使用之；加载失败返回错误（Pod 启动失败 → 不就绪，不会被标健康）。
  - Agent 增加 `SetReadyCallback(func(bool))`（nil 安全）；`OnConnect`：回调 true，并重放 `SetCapabilities` + `SetHealth`（best-effort，失败仅记日志），保证重连后能力/健康报告一致；`OnConnectFailed`：回调 false。
- `internal/healthcheck/server.go`：
  - 增加按名称跟踪的就绪状态：`Track(name) func(bool)`；新增 `/readyz`，全部 runtime 就绪才 200，否则 503；`/healthz` 语义不变（liveness 用）。
- `internal/manager/manager.go`：`New`/`Start` 中为每个 runtime 注册 `healthServer.Track(runtime.Name)` 并通过 `OpAMPAgent.SetReadyCallback` 接线；healthServer 为 nil 时不接线（保持可选）。
- `internal/manifests/opampbridge/container.go`（operator 侧）：readinessProbe 改指 `/readyz`，liveness 保持 `/healthz`。
- standalone 多 runtime：`/readyz` 要求全部 agent 连上各自后端。

## 实施步骤（依赖顺序）

1. API 类型改动（TLS SecretName、运行时文件字段、Status Conditions/ObservedGeneration）。
2. `make update` 重新生成 deepcopy/CRD/bundle/API 文档（早做，避免生成漂移）。
3. naming + manifests（params 透传、configmap 渲染、volume、mount、secret hash 注解）。
4. webhook 校验。
5. controller：Secret 读取/缺失分支、Secret watch + mapper、RBAC marker。
6. status：条件计算（含 Deployment 可用性判定的纯函数）、degraded 入口。
7. bridge：TLS 文件加载 → ready 回调/重连重报 → healthcheck `/readyz` → manager 接线 → readiness 探针改路径。
8. `.chloggen` 条目与文档。
9. 再次 `make update` + `make fmt`，补齐测试并跑通。

## 测试策略（testify，跟随各文件既有表驱动风格）

- `configmap_test.go`：secretName 渲染（仅 CA / CA+mTLS / 含未知键）；无 secret 时输出与现状逐字节一致。
- `volume_test.go`、`container_test.go`：volume/mount 在有无 secret 两种情况下的差异；readiness 路径为 `/readyz`。
- `annotations_test.go`：Secret Data 变化 → hash 注解变化；无 secret → 无该注解。
- `opampbridge_webhook_test.go`：insecure+secretName 被拒；原有校验不回归。
- controller envtest（扩展 `reconcile_test.go` 中 OpAMPBridge 用例）：
  - 建 CR + Secret → Deployment 带 volume/mount/hash 注解、ConfigMap 含 ca_file；
  - 改 Secret Data 再次 reconcile → 注解变化（滚动发生）；
  - 引用的 Secret 不存在 → 既有 Deployment/ConfigMap 不被修改、Ready=False、返回 RequeueAfter；
  - Secret 恢复 → 对象更新、Ready 随 Deployment Available 变 True；
  - Deployment 未就绪（replicas/updated/available 不满足）→ Ready=False。
- bridge：
  - `agent_test.go`：扩展 TLS 表驱动用例（仅 CA、keypair、缺文件报错、坏 PEM 报错、skipVerify 保留）；`OnConnect` 回调 true 且调用 SetCapabilities/SetHealth；`OnConnectFailed` 回调 false。
  - `healthcheck/server_test.go`：`/healthz` 恒 200；`/readyz` 在未连接/部分连接/全部连接下的状态码。
  - `manager_test.go`：Track + 回调接线。
- e2e chainsaw：现有 bridge-server 不支持 TLS，新增 TLS e2e 需要改造测试应用，成本高；本次以单元 + envtest 覆盖资源形态与状态语义，PR 描述中如实说明未跑 e2e。

## 验证

- `make fmt && make lint`
- `go test ./internal/manifests/opampbridge/... ./internal/controllers/... ./internal/status/... ./internal/webhook/...`
- `go test ./cmd/operator-opamp-bridge/...`
- `cd apis && go test ./...`
- `make chlog-validate`
- `make update`（生成文件无额外漂移后提交）
- envtest 首次运行自动下载 kubebuilder binaries（需要网络，见风险）。

## 风险与处理

- **`make update` 工具链/网络**：controller-gen 等若下载失败，手动按生成格式同步 CRD/RBAC/bundle/docs，并在结果中说明；优先尝试 `make update`。
- **运行时字段与 CRD 字段混用同一类型**：以 `json:"-"` + 注释明确边界；operator 渲染时用显式 map，绝不把 `secretName` 写进 bridge 配置。
- **Secret 缺失时的热重试**：watch 负责即时触发，`RequeueAfter` 仅兜底；不返回 error 以避免刷事件，但仍写 Ready=False 与 Warning event。
- **就绪口径变化**：新增 `/readyz` 且初始为未连接 → 未配 TLS 的现有 bridge 在首次连上后端前 Ready 会短暂为 False；连接成功后立即恢复 True，行为更真实且仅为新增状态字段，不影响既有对账逻辑。
- **多 runtime（standalone）**：一个 agent 断连即整体 NotReady，语义保守且明确。
