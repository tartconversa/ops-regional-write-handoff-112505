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
