# bettips — 足球亚盘自动扫描提醒系统

## 策略说明

**"深盘临场首次升深 + 半场0:0 → HT大1候选"**

| 阶段 | 条件 |
|------|------|
| 赛前 | bet365主让盘口 ≥ 1球 |
| 赛前 | 开赛前15分钟内出现"首次升深"（盘口变更深，且该深度历史上从未出现） |
| 半场 | 上半场比分 0:0 |
| 半场 | 上半场无红牌 |
| 结论 | 触发提醒：考虑下半场大1 |

---

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
playwright install chromium
```

### 2. 发现球探网接口（首次必做）

```bash
python -m src.collectors.qiutan discover "https://www.qiutan.com/match/<比赛ID>"
```

程序会打开浏览器并打印所有网络请求。找到类似这样的接口：

- 比赛列表：包含 `matchList`、`schedule`、`fixture` 等关键词的 URL
- 赔率历史：包含 `odds`、`handicap`、`index` 等关键词的 URL
- 实时比分：包含 `score`、`live`、`result` 等关键词的 URL

把找到的URL关键词填入 `config.yaml` 的对应字段。

### 3. 配置

编辑 `config.yaml`，填入发现的接口关键词：

```yaml
qiutan:
  match_list_pattern: "/api/match/list"      # 替换为实际发现的URL片段
  odds_history_pattern: "/api/odds/history"
  live_score_pattern: "/api/score/live"
```

### 4. 运行

```bash
python main.py
```

日志会实时输出候选比赛：

```
[HT大1候选] 19:50 巴萨 vs 皇马 (西甲)
盘口路径: 主让1.0 → 19:48升至1.25 (首次出现)
半场: 0:0 | 红牌: 无
建议关注: 下半场大1
```

---

## 项目结构

```
bettips/
├── main.py                   # 入口，APScheduler调度器
├── config.yaml               # 配置文件
├── requirements.txt
├── src/
│   ├── storage.py            # SQLite数据库操作
│   ├── normalizer.py         # 盘口字符串→浮点数标准化
│   ├── scanner.py            # 赛前规则引擎（首次升深识别）
│   ├── halftime.py           # 半场确认引擎
│   ├── alert.py              # 提醒接口（日志/Telegram）
│   └── collectors/
│       ├── base.py           # 抽象基类
│       └── qiutan.py         # Playwright球探网爬取器
├── db/
│   └── schema.sql            # 数据库建表SQL
└── tests/
    ├── test_normalizer.py    # 盘口标准化单元测试
    └── test_scanner.py       # 首次升深算法单元测试
```

---

## Telegram推送配置（可选）

1. 在Telegram中找到 `@BotFather`，输入 `/newbot` 创建机器人，获取 Token
2. 给机器人发一条消息，然后访问 `https://api.telegram.org/bot<TOKEN>/getUpdates` 获取 Chat ID
3. 在 `config.yaml` 中填入：

```yaml
alert:
  mode: "telegram"
  telegram_token: "你的Token"
  telegram_chat_id: "你的ChatID"
```

---

## 数据库说明

系统使用SQLite，运行后自动创建 `bettips.db`，含4张表：

| 表名 | 说明 |
|------|------|
| `matches` | 比赛基本信息和比分 |
| `odds_history` | bet365盘口变化记录（含时间戳） |
| `match_events` | 进球/红牌事件 |
| `candidates` | 满足条件的候选比赛 |

---

## 盘口标准化规则

| 原始字符串 | 标准化深度 | 说明 |
|-----------|-----------|------|
| `-1` | 1.0 | 主让1球 |
| `-1/1.5` | 1.25 | 主让1/1.5（取中值） |
| `-1.5` | 1.5 | 主让1.5球 |
| `-1.5/2` | 1.75 | 主让1.5/2 |
| `0` | 0.0 | 平手盘 |
| `0.5` | 0.5 | 主让半球（方向判断依赖原始字段） |

---

## 运行测试

```bash
pytest tests/ -v
```
