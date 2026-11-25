# 演练资料格式

任务状态跨地域切流演练。分片的 offset 只在同一个租户分片内有效，逻辑操作标识在租户内唯一。各输入的 observed_ms 是调度台收到记录的时间。

## `groups` 业务分组和最低可读水位

members 为同一租户写操作可能一起涉及的有序分片集合。min_history 是按分片给出的无游标读最低水位。write_nodes 标明区域内执行写入的节点。

字段为 `tenant`、`group`、`members`、`min_history`、`write_nodes`。

## `opening` 初始路由及复制观察

epoch 是已发出的写授权代次。applied_offset 为对应地域分片已持久可读位置，confirmed_offset 为原权威区域已确认给调用方的写位置。初始路由由 owner 给出。

字段为 `tenant`、`shard`、`epoch`、`owner`、`confirmed_offset`、`east_applied`、`west_applied`。

## `observations` 地域回执和控制指令

fenced 行表示节点有约束力地确认失去指定代次的写权，probe_down 只表示链路健康未知。replicated 带同分片读取水位。route_ack 带目标路由代次。request_handoff 和 request_return 是值班提议。回执不能只凭到达顺序覆盖更高代次。 route_ack 的 subject 为 write 时确认新路由实际指到目标地域。缺少回执期间仍须区分网关现有读路由和写权限。

字段为 `observed_ms`、`event`、`tenant`、`group`、`region`、`subject`、`epoch`、`offset`。

## `traffic` 调用方请求和旧执行器回执

write/read 是请求，receipt 是既有执行器投递的历史写回执。offsets 为分片对应的写入位置或读的最低位置。相同 op_id 为逻辑操作重送，重送保持载荷相同。read 没有 offsets 表示历史读。origin 仅在已有回执时表示其来源。 同租户的逻辑操作跨代次沿用 op_id。receipt 代表整组已提交，不允许部分 offsets。no_effect 带同样 op_id 和 region，代表旧执行器承诺这次尝试没有影响且会拒绝后续投递。写入请求中的 epoch 如果为空，由调用端交给当前有效路由选择。

字段为 `observed_ms`、`kind`、`op_id`、`tenant`、`group`、`epoch`、`origin`、`offsets`。
