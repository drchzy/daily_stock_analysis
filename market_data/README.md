# Market Data Bridge

这个目录用于让 ChatGPT 通过 GitHub Actions 间接执行行情采集，然后读取 \`latest.json\` 进行分析。

## 数据源

- 主：EFinance（东方财富）
- 备：AKShare（东方财富）
- 仓库原有 PyTDX/其他数据源保持不变，本功能不修改现有主分析流程。

## 触发方式

修改并提交 \`market_data/request.json\` 即可触发 \`Market Data Bridge\` workflow。

输出写入 \`market_data/latest.json\`。

## 超短趋势筛选

- 默认排除北交所、科创板、ST，创业板保留。
- 流动性过滤，避免成交过小。
- 偏好 MA5 > MA10 > MA20、多头排列。
- 两类 setup：放量接近/突破 20 日高；MA5 附近缩量回踩。
- 对距 MA5 过远、单日涨幅过大、收盘靠近日内低位进行扣分。
- 输出参考入场区、结构止损、第一/第二止盈，以及“浮盈 4% 后高点回撤 2%”移动保护规则。

筛选只是候选池，最终仍结合板块强弱、市场情绪、公告/新闻和次日开盘结构确认。
