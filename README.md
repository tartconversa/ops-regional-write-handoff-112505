# 双地域业务切流中的写入授权与读可见性

任务状态跨地域切流演练。分片的 offset 只在同一个租户分片内有效，逻辑操作标识在租户内唯一。各输入的 observed_ms 是调度台收到记录的时间。

## 在本地查看资料

运行环境使用 Python 3.11.2 和标准库。启动资料浏览器无需安装额外包。

```sh
python materials.py
python materials.py groups
python -m unittest discover -s tests -v
```

资料浏览器也接受下方的其他资料名。JSONL 使用 UTF-8 编码，每行是一个对象。表内的顺序对应来源记录，事件时间与接收时间的区别以资料说明为准。各入口可以在无外网的环境中运行。

资料字段和业务口径见[演练资料格式](materials/README.md)。

## 运维控制程序 `duty_ctl.py`

`duty_ctl.py` 是仅依赖标准库的有状态控制程序（`python main.py ...` 等价；
Python 调用也可使用 `controller.ControlPlane`）。它把分组、初始水位、切流提议、
`fenced`/`probe_down`/复制/路由证据、逻辑写操作及回执保存到一个 JSON 状态文件；
命令分次调用后状态仍在，重启后可继续使用原 `op_id`/回执并查看卡点。

### 一键接管自带演练

```sh
python duty_ctl.py demo --state /tmp/metro-drill.json
python duty_ctl.py --json demo --state /tmp/metro-drill.json
```

回放后可看到本资料中的关键结论：

- `r31` 的游标是 `task=118,ack=41`，当时只有东区可见，读自东区；
- `r32` 是无游标历史读。西区 `ack=38` 虽落后于最新界线 `42`，但仍高于
  `min_history.ack=34`，所以西区有历史读资格；落后分片不能借用另一分片水位；
- `r34` 必须看见 `m72` 回执 `task=120,ack=43`。当时东区尚未复制到，只有西区可执行；
- `probe_down` 不是失权证明；`e-api`、`e-batch` 都有 epoch 7 的 `fenced` 后，
  metro/jobs 的旧东区写权才算失效；
- `m71` 的旧东区成功回执在两次 fence 之间到达，始终保留；重送只返回原回执；
- `m70` 先有明确 `no_effect`，之后才允许用同一 `op_id` 发起新尝试；
- 回放结束时，写路由已是东区 epoch 9，但 `m70` 的西区新尝试结果不明，
  因此状态会明确显示“路由已生效、业务组写仍被哪一条在途证据卡住”；
- epoch 8 的西区 `route_ack` 在 epoch 9 之后迟到，只作为历史证据保留，不会激活旧授权。

程序不把“所有机器恢复健康”作为切流条件：`probe_down`/健康探测只作线索，写权失效只认
覆盖全部 `write_nodes` 的 `fenced` 证据，再加上在途结论、副本水位和路由确认。

### 开新一轮并复用同一组区域事件

```sh
# A 轮
python duty_ctl.py --state runs/a.json init --materials materials --force
python duty_ctl.py --state runs/a.json replay \
  materials/observations.jsonl materials/traffic.jsonl

# 同一组 observations 可原样交给 B 轮复用
python duty_ctl.py --state runs/b.json init --materials materials --force
python duty_ctl.py --state runs/b.json replay materials/observations.jsonl
```

`replay` 默认按 `observed_ms` 跨文件稳定合并；需要严格按文件/行顺序时加
`--sort input`。同一运行中可继续执行下面的命令追加证据和请求，所有变更都会原子落盘。

### 当班人常用命令

全局选项 `--state PATH` 和 `--json` 放在子命令前后均可；不给 `--state` 时使用
`./.dutyctl-state.json`。下面命令假设先执行
`export DUTY_CTL_STATE=/tmp/metro-drill.json`。

```sh
# 提出东区 -> 西区 / 撤回东区
python duty_ctl.py handoff metro jobs --epoch 8
python duty_ctl.py return  metro jobs --epoch 9

# 更新检查证据
python duty_ctl.py fence       metro jobs e-batch --region east --epoch 7
python duty_ctl.py probe-down  metro jobs e-batch --region east --epoch 7
python duty_ctl.py replicate   metro jobs --region west --shard ack --offset 42
python duty_ctl.py route-ack   metro jobs --region west --epoch 8 --subject write
python duty_ctl.py gateway     metro jobs east --write closed

# 创建/重送写任务，确认整组回执或无效果
python duty_ctl.py write    m73 metro jobs
python duty_ctl.py receipt  m73 metro jobs --origin west --epoch 8 \
  --offsets '{"task":121,"ack":44}'
python duty_ctl.py no-effect m70 metro jobs --origin east --epoch 7
python duty_ctl.py reconcile metro m70 --note '执行器仍在核对'
python duty_ctl.py connection metro jobs west ok --epoch 8
# 若先 no_effect 后又出现互相矛盾的 receipt，程序会锁存 outcome_conflict；
# 当班人核对后显式裁决，才会恢复单一终态和可重试建议。
python duty_ctl.py resolve-conflict metro op1 --accept receipt

# 历史读、按成功回执游标读、强制指定候选地域
python duty_ctl.py read r40 metro jobs
python duty_ctl.py read r41 metro jobs --after m73
python duty_ctl.py read r42 metro jobs --cursor '{"task":121,"ack":44}' --prefer west

# 重启后继续追查
python duty_ctl.py status metro
python duty_ctl.py status metro jobs --json
python duty_ctl.py status metro jobs --at 20      # 重放到 observed_ms=20
python duty_ctl.py op metro m70 --json
python duty_ctl.py journal --state runs/a.json
```

读结果中的 `served_by`/`read_source` 是可执行该读的副本；`eligible_replicas`
列出所有满足水位的副本；`waiting_for` 逐分片说明还缺哪条复制/水位证据；
`alternative_scopes` 会在游标读不可见时提示可改走无游标历史范围。
写回执中的 `authorization` 说明归属地域、代次、当前路由和业务组成员。
`status --json` 的 `effective_flow` 直接给出当前是否可写、哪些地域可承担历史读或
最新游标读，以及“写停”时卡住的证据。退出码 `0` 表示控制命令完成；领域条件未满足
（写未受理、读仍在等证据、回执冲突）返回 `3`；输入/状态文件错误返回 `2`。

### Python API

```python
from duty_ctl import ControlPlane

with ControlPlane("runs/drill.json") as ctl:
    ctl.load_group({...})
    ctl.load_opening({...})
    ctl.record_observation({...})
    ctl.submit_write({...})
    ctl.record_receipt({...})
    read = ctl.plan_read("metro", "jobs", after="m71", op_id="r1")
    print(read["served_by"], read["waiting_for"])
```

不传路径时使用内存状态，适合测试或一次性模拟。

## 测试

```sh
python -m unittest discover -s tests -v
```
