# Rsync 同步中心

轻量级 Web 管理界面，用于配置和管理 rsync 同步任务。支持本地、远程以及跨远程服务器之间的文件同步，提供定时调度、主机配置管理、微信通知等企业级功能。

## 功能特点

**同步管理**
- 支持本地 ↔ 本地、本地 ↔ 远程、远程 ↔ 远程（纯 rsync + SSH，无需 FUSE）
- 密码认证 / SSH 密钥认证，支持自定义端口
- 远程服务器文件浏览器
- `--delete`、`--checksum`、`--dry-run`、限速、包含/排除规则等完整 rsync 选项
- 实时进度条：文件数、百分比、传输速度、已用时间
- 同步日志与历史记录

**定时调度**
- 每天 / 每周 / 每月定时执行
- 自定义间隔（分钟 / 小时）
- 任务卡片显示下次执行时间
- 支持手动即时触发

**主机配置管理**
- 保存远程主机连接信息（地址、端口、用户名、认证方式）
- 创建任务时从已保存的主机配置一键选择
- 修改主机密码/端口后所有关联任务自动生效

**SSH 密钥管理**
- 上传、删除 SSH 私钥文件

**批量操作**
- 勾选多个任务 → 一键同步 / 一键删除
- 全选 / 取消全选

**Webhook 通知**
- 同步完成后推送到企业微信群机器人

**其它**
- 暗色 / 浅色 / 跟随系统 三档主题
- 桌面端两栏布局，移动端自适应
- Prometheus 指标接口（`/metrics`）
- 健康检查接口（`/health`）
- 容器优雅退出（秒级响应 SIGTERM）

## 快速开始

### Docker Compose 部署

```bash
git clone https://github.com/devbliss-ai/rysnc-centry.git
cd rysnc-centry
```

编辑 `docker-compose.yml`，按需修改挂载路径和端口：

```yaml
volumes:
  - /your/source/path:/home:ro    # 源路径，建议只读
  - /your/target/path:/data       # 目标路径，读写
ports:
  - "8856:8856"
```

启动：

```bash
docker-compose up -d
```

访问 `http://your-server-ip:8856`

## 配置说明

### 挂载路径

| 路径 | 用途 | 建议 |
|------|------|------|
| `/home` | 默认源文件路径 | 只读挂载 |
| `/data` | 默认目标文件路径 | 读写挂载 |

### 环境变量

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `TZ` | 时区 | `Asia/Shanghai` |

### 微信 Webhook

在 Web 界面「设置」标签页填入企业微信群机器人 Webhook URL，保存后每次同步完成自动推送通知。

## 目录结构

```
.
├── app/
│   ├── templates/
│   │   └── index.html        # Web 界面
│   ├── app.py                # Flask 主程序
│   └── requirements.txt      # Python 依赖
├── docker-compose.yml
├── Dockerfile
└── README.md
```

## 技术栈

- 后端：Python Flask + SQLite
- 前端：原生 JavaScript + HTML + CSS
- 容器化：Docker
- 同步引擎：Rsync + SSH

## 注意事项

- 建议在内网环境使用，公网暴露请做好安全防护
- 远程同步需要目标服务器安装 rsync 和 SSH
- 数据持久化在 `/app/data` 目录（SQLite 数据库 + SSH 密钥）
- 首次启动自动从旧 JSON 格式迁移到 SQLite
