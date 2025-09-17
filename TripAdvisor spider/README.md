# TripAdvisor 景点评论爬虫（增强版）

一个用于采集 TripAdvisor 景点评论与景点信息的增强脚本，支持多线程、断点续跑、CSV/JSON/数据库记录、覆盖率提示与重试退避策略。

## 功能简述
- 自动从 TripAdvisor 指定景点的“全部语言”评论页分页抓取评论与景点信息，支持多线程、断点续跑与失败重试；结果以 JSON 存档，并可选将采集记录写入 MySQL 与 CSV。

## 功能
- 全量采集：先从 “全部语言(all)” 页面分页抓取，尽可能最大覆盖
- 断点续跑：`progress.json` 记录处理过的 URL、成功与失败数
- 多线程：可配置线程数（默认 3，脚本默认 15，命令行会覆盖）
- 重试与退避：网络错误、频控、服务端错误自动退避重试
- 输出：每个景点生成独立 JSON 文件，并记录到 `collection_log.csv`
- 数据库：可选将采集记录写入 MySQL（可 `--no-db` 跳过）

## 目录结构
- `spider.py`：主脚本
- `attraction_urls.sample.csv`：示例 URL 列表（只有一列 `url`）
- `attraction_comments/`：输出 JSON 文件目录（运行时生成）
- `progress.json`：断点进度（运行时生成）
- `collection_log.csv`：采集记录（运行时生成）

## 运行环境
- Python 3.9+

## 安装依赖
```bash
python3 -m venv .venv
source .venv/bin/activate  # Windows 使用 .venv\\Scripts\\activate
pip install -r requirements.txt
```

## 配置（环境变量）
脚本支持通过环境变量覆盖数据库与请求 Header 的敏感信息（请勿在代码或仓库中硬编码任何私密值）：

- `MYSQL_HOST`（默认 `localhost`）
- `MYSQL_PORT`（默认 `3306`）
- `MYSQL_USER`
- `MYSQL_PASSWORD`
- `MYSQL_DATABASE`
- `MYSQL_CHARSET`（默认 `utf8mb4`）
- `MYSQL_AUTOCOMMIT`（默认 `true`）
- `MYSQL_CONNECT_TIMEOUT`（默认 `30`）
- `MYSQL_READ_TIMEOUT`（默认 `60`）
- `MYSQL_WRITE_TIMEOUT`（默认 `60`）
- `MYSQL_TABLE`（默认 `collection_table`，用于存储采集记录的表名）
- `TA_USER_AGENT`（可自定义 UA）
- `TA_X_TA_UID`（可选，勿提交到仓库）
- `TA_COOKIE`（可选，勿提交到仓库）
 - `COLLECTOR_NAME`（可选，采集人标识，勿提交到仓库）

你可以复制 `.env.example` 内容到 `.env` 并填入私密值（不要提交 `.env` 到仓库）。

你可以复制 `.env.example` 内容并以终端方式 `export`，或通过 CI/服务器注入。

## 准备 URL 列表
复制示例并自行替换为你的目标：
```bash
cp attraction_urls.sample.csv attraction_urls.csv
```
CSV 必须包含表头 `url`，示例见 `attraction_urls.sample.csv`。

## 使用
- 创建示例：
```bash
python spider.py --create-sample
```
- 显示进度：
```bash
python spider.py --show-progress
```
- 重置进度：
```bash
python spider.py --reset-progress
```
- 执行采集：
```bash
# 全量，使用默认 3 线程
python spider.py --csv attraction_urls.csv --threads 3

# 仅处理前 N 个用于测试
python spider.py --csv attraction_urls.csv --threads 3 --limit 10

# 测试模式（仅前 3 个）
python spider.py --test

# 跳过数据库（只写 JSON/CSV）
python spider.py --no-db

# 指定语言（默认 all）：
python spider.py --langs zhCN,en
```

## 输出
- JSON 文件：位于 `attraction_comments/`，命名为 `景点名_UUID前8位.json`
- 采集记录：`collection_log.csv`
- 进度：`progress.json`

## 注意
- 请自行准备合法的 Cookie 与标识（如需），并以环境变量注入，避免将敏感信息提交到 Git。
- 多线程访问外部站点请遵守网站服务条款与法律法规。
- 若数据库不可用，请使用 `--no-db` 模式，仅生成文件输出。

## 使用限制与合规声明（务必阅读）
- 本代码仅供学习用途，严禁任何形式的商业使用（包括但不限于付费服务、嵌入商业产品、以采集数据牟利）。
- 使用者须严格遵守目标网站（Tripadvisor）的服务条款、`robots.txt`、访问频率限制与技术保护措施，不得规避、破解或绕过任何反爬措施。
- 使用本代码进行的任何数据处理需符合所在地与数据来源地的法律法规（含但不限于反不正当竞争、著作权、网络安全与数据合规相关规定）。
- 若用于学术研究或内部测试，请在引用或发布研究成果时注明来源并对第三方权利保持克制与尊重。
- 如需商业化授权，请先征得版权所有者书面许可。

## 免责声明
- 本项目及其作者不对使用者的任何使用行为承担责任。使用者需自行评估并承担使用风险与合规义务。
- 采集、存储与使用第三方数据可能涉及第三方权利；请在取得合法授权与合理目的、最小必要的前提下进行，并避免对目标服务造成不当负载。
- 详见 `DISCLAIMER.md` 获取更详尽的合规与风险提示。

## 许可
本项目当前仓库保留 `LICENSE` 为 MIT，但已明确补充“仅供学习与非商业使用”的项目范围说明。如你需要真正的“禁止商业使用”的强约束，请联系作者以更换为非商业许可（例如 PolyForm Noncommercial 1.0.0）或签署单独授权。
