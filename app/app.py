from flask import Flask, render_template, request, jsonify
import subprocess
import os
import json
import logging
import signal
import shutil
import sqlite3
from datetime import datetime
import schedule
import threading
import time
from pathlib import Path, PurePosixPath
import sys
import tempfile
import re
import urllib.request
from waitress import serve

app = Flask(__name__)

VERSION = '1.11'

# 结构化日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger('rsync-web')

# 确保日志立即输出
sys.stdout.reconfigure(line_buffering=True)

# ===== 容器退出清理 =====
_shutdown_event = threading.Event()


def _shutdown():
    if _shutdown_event.is_set():
        return
    _shutdown_event.set()
    logger.info('容器退出，正在清理...')

    with _running_sync_lock:
        rsync = _running_sync
    if rsync and rsync.get('process'):
        try:
            proc = rsync['process']
            if os.name != 'nt' and hasattr(proc, 'pid') and proc.pid:
                os.killpg(proc.pid, signal.SIGTERM)
            proc.terminate()
            logger.info('已终止运行中的 rsync 进程')
        except Exception:
            pass

    try:
        _maybe_archive_logs()
    except Exception:
        pass

    logger.info('清理完成')


import atexit
atexit.register(_shutdown)

# ===== 存储层：SQLite 替代 JSON 文件（线程安全 + 避免 os.replace 竞态） =====
DATA_DIR = '/app/data'
DB_FILE = os.path.join(DATA_DIR, 'rsync.db')
TASKS_JSON_BACKUP = os.path.join(DATA_DIR, 'sync_tasks.json.bak')
LOGS_JSON_BACKUP = os.path.join(DATA_DIR, 'sync_logs.json.bak')

# SQLite 会自带串行化，连接级锁用于跨连接保护迁移/备份
_db_lock = threading.RLock()


def _db():
    """获取 SQLite 连接（每次调用返回新连接；SQLite 在单文件场景下天然互斥）"""
    os.makedirs(DATA_DIR, exist_ok=True)
    conn = sqlite3.connect(DB_FILE, timeout=10, isolation_level=None)  # autocommit
    conn.row_factory = sqlite3.Row
    conn.execute('PRAGMA journal_mode=WAL')  # 读写并发不互锁
    conn.execute('PRAGMA synchronous=NORMAL')
    return conn


def _init_db():
    """创建表结构"""
    with _db_lock:
        conn = _db()
        try:
            conn.executescript('''
                CREATE TABLE IF NOT EXISTS tasks (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    source TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    delete_option INTEGER DEFAULT 1,
                    remark TEXT DEFAULT '',
                    source_auth TEXT DEFAULT '{}',
                    dest_auth TEXT DEFAULT '{}',
                    schedule_json TEXT,
                    checksum INTEGER DEFAULT 0,
                    dry_run INTEGER DEFAULT 0,
                    include_patterns TEXT DEFAULT '',
                    exclude_patterns TEXT DEFAULT '',
                    bwlimit TEXT DEFAULT '',
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    task_id INTEGER,
                    source TEXT NOT NULL,
                    destination TEXT NOT NULL,
                    success INTEGER NOT NULL,
                    message TEXT NOT NULL,
                    output TEXT,
                    files_synced INTEGER DEFAULT 0,
                    error_count INTEGER DEFAULT 0,
                    trigger TEXT DEFAULT 'manual',
                    time TEXT NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_logs_time ON logs(time DESC);
                CREATE INDEX IF NOT EXISTS idx_logs_task ON logs(task_id);
                CREATE TABLE IF NOT EXISTS hosts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    host TEXT NOT NULL,
                    port TEXT DEFAULT '22',
                    username TEXT DEFAULT 'root',
                    auth_type TEXT DEFAULT 'password',
                    password TEXT DEFAULT '',
                    key_name TEXT DEFAULT '',
                    mode TEXT DEFAULT 'rw',
                    created_at TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );
            ''')
            conn.execute("ALTER TABLE hosts ADD COLUMN mode TEXT DEFAULT 'rw'")
        except sqlite3.OperationalError:
            pass
        finally:
            conn.close()


def _migrate_from_json():
    """首次启动时从旧 JSON 文件迁移到 SQLite（一次性）"""
    with _db_lock:
        conn = _db()
        try:
            cur = conn.cursor()
            cur.execute('SELECT COUNT(*) FROM tasks')
            if cur.fetchone()[0] == 0:
                for old_path, kind in (('/app/data/sync_tasks.json', 'tasks'),
                                       ('/app/data/sync_logs.json', 'logs')):
                    if not os.path.exists(old_path):
                        continue
                    try:
                        with open(old_path, 'r', encoding='utf-8') as f:
                            data = json.load(f)
                        if not isinstance(data, list):
                            continue
                        if kind == 'tasks':
                            for t in data:
                                cur.execute(
                                    'INSERT INTO tasks (id,source,destination,delete_option,remark,'
                                    'source_auth,dest_auth,schedule_json,created_at) VALUES (?,?,?,?,?,?,?,?,?)',
                                    (t.get('id'), t.get('source', ''), t.get('destination', ''),
                                     1 if t.get('delete_option') else 0,
                                     t.get('remark', ''),
                                     json.dumps(t.get('source_auth') or {}),
                                     json.dumps(t.get('dest_auth') or {}),
                                     json.dumps(t['schedule']) if t.get('schedule') else None,
                                     t.get('created_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))))
                        else:
                            for l in data:
                                cur.execute(
                                    'INSERT INTO logs (task_id,source,destination,success,message,output,'
                                    'files_synced,error_count,trigger,time) VALUES (?,?,?,?,?,?,?,?,?,?)',
                                    (l.get('task_id'), l.get('source', ''), l.get('destination', ''),
                                     1 if l.get('success') else 0,
                                     l.get('message', ''), l.get('output', ''),
                                     l.get('files_synced', 0), l.get('error_count', 0),
                                     l.get('trigger', 'manual'),
                                     l.get('time', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))))
                        # 迁移后备份原文件
                        shutil.copy2(old_path, old_path + '.migrated')
                        logger.info(f'已从 {old_path} 迁移 {len(data)} 条记录到 SQLite')
                    except Exception as e:
                        logger.error(f'迁移 {old_path} 失败: {e}')
        finally:
            conn.close()


def _backup_db():
    """每日自动备份数据库到 /app/data/backups/"""
    try:
        backup_dir = os.path.join(DATA_DIR, 'backups')
        os.makedirs(backup_dir, exist_ok=True)
        ts = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_path = os.path.join(backup_dir, f'rsync_{ts}.db')
        with _db_lock:
            shutil.copy2(DB_FILE, backup_path)
        # 只保留最近 7 份备份
        backups = sorted(Path(backup_dir).glob('rsync_*.db'), key=lambda p: p.stat().st_mtime)
        for old in backups[:-7]:
            try:
                old.unlink()
            except OSError:
                pass
        logger.info(f'数据库已备份: {backup_path}')
    except Exception as e:
        logger.error(f'数据库备份失败: {e}')


def _row_to_task(row):
    """sqlite Row → 任务 dict（保持与旧 JSON 格式一致）"""
    schedule_data = json.loads(row['schedule_json']) if row['schedule_json'] else None
    is_remote_src = ':' in row['source'] and '@' in row['source'].split(':')[0]
    is_remote_dst = ':' in row['destination'] and '@' in row['destination'].split(':')[0]
    if is_remote_src and is_remote_dst:
        direction = 'both'
    elif is_remote_src:
        direction = 'pull'
    elif is_remote_dst:
        direction = 'push'
    else:
        direction = 'local'
    return {
        'id': row['id'],
        'source': row['source'],
        'destination': row['destination'],
        'delete_option': bool(row['delete_option']),
        'remark': row['remark'] or '',
        'source_auth': json.loads(row['source_auth'] or '{}'),
        'dest_auth': json.loads(row['dest_auth'] or '{}'),
        'schedule': schedule_data,
        'next_run': _next_run_time(schedule_data) if schedule_data else None,
        'checksum': bool(row['checksum']),
        'dry_run': bool(row['dry_run']),
        'include_patterns': row['include_patterns'] or '',
        'exclude_patterns': row['exclude_patterns'] or '',
        'bwlimit': row['bwlimit'] or '',
        'created_at': row['created_at'],
        'direction': direction,
        'stats': _get_task_stats(row['id']),
    }


def _row_to_log(row):
    """sqlite Row → 日志 dict"""
    return {
        'id': row['id'],
        'task_id': row['task_id'],
        'source': row['source'],
        'destination': row['destination'],
        'success': bool(row['success']),
        'message': row['message'],
        'output': row['output'] or '',
        'files_synced': row['files_synced'] or 0,
        'error_count': row['error_count'] or 0,
        'trigger': row['trigger'],
        'time': row['time'],
    }


def load_tasks():
    """线程安全地加载任务列表"""
    conn = _db()
    try:
        rows = conn.execute('SELECT * FROM tasks ORDER BY id').fetchall()
        return [_row_to_task(r) for r in rows]
    except sqlite3.Error as e:
        logger.error(f'加载任务失败: {e}')
        return []
    finally:
        conn.close()


def save_tasks(tasks):
    """保存任务列表（全量替换）"""
    with _db_lock:
        conn = _db()
        try:
            conn.execute('BEGIN')
            conn.execute('DELETE FROM tasks')
            for t in tasks:
                conn.execute(
                    'INSERT INTO tasks (id,source,destination,delete_option,remark,'
                    'source_auth,dest_auth,schedule_json,checksum,dry_run,'
                    'include_patterns,exclude_patterns,bwlimit,created_at) '
                    'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                    (t.get('id'), t.get('source', ''), t.get('destination', ''),
                     1 if t.get('delete_option') else 0,
                     t.get('remark', ''),
                     json.dumps(t.get('source_auth') or {}),
                     json.dumps(t.get('dest_auth') or {}),
                     json.dumps(t['schedule']) if t.get('schedule') else None,
                     1 if t.get('checksum') else 0,
                     1 if t.get('dry_run') else 0,
                     t.get('include_patterns', ''),
                     t.get('exclude_patterns', ''),
                     t.get('bwlimit', ''),
                     t.get('created_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))))
            conn.execute('COMMIT')
        except Exception:
            conn.execute('ROLLBACK')
            raise
        finally:
            conn.close()


def add_task_to_db(task):
    """新增任务，返回最终 id（None 表示自动分配）"""
    with _db_lock:
        conn = _db()
        try:
            cur = conn.execute(
                'INSERT INTO tasks (id,source,destination,delete_option,remark,'
                'source_auth,dest_auth,schedule_json,checksum,dry_run,'
                'include_patterns,exclude_patterns,bwlimit,created_at) '
                'VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)',
                (task.get('id'), task.get('source', ''), task.get('destination', ''),
                 1 if task.get('delete_option') else 0,
                 task.get('remark', ''),
                 json.dumps(task.get('source_auth') or {}),
                 json.dumps(task.get('dest_auth') or {}),
                 json.dumps(task['schedule']) if task.get('schedule') else None,
                 1 if task.get('checksum') else 0,
                 1 if task.get('dry_run') else 0,
                 task.get('include_patterns', ''),
                 task.get('exclude_patterns', ''),
                 task.get('bwlimit', ''),
                 task.get('created_at', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))))
            return cur.lastrowid
        finally:
            conn.close()


def update_task_in_db(task_id, updates):
    """更新任务字段（updates: dict）"""
    if not updates:
        return
    with _db_lock:
        conn = _db()
        try:
            sets = []
            vals = []
            for k, v in updates.items():
                if k in ('source_auth', 'dest_auth', 'schedule'):
                    k = 'source_auth' if k == 'source_auth' else ('dest_auth' if k == 'dest_auth' else 'schedule_json')
                    # auth 为空 dict 视为「无变更」跳过（避免 API 层静默清空密码/端口）
                    if k != 'schedule_json' and isinstance(v, dict) and not v:
                        continue
                    v = json.dumps(v) if v else (v if k == 'schedule_json' and v is None else None)
                elif k == 'delete_option' or k == 'checksum' or k == 'dry_run':
                    v = 1 if v else 0
                sets.append(f'{k}=?')
                vals.append(v)
            vals.append(task_id)
            conn.execute(f'UPDATE tasks SET {",".join(sets)} WHERE id=?', vals)
        finally:
            conn.close()


def delete_task_from_db(task_id):
    """从数据库删除任务"""
    with _db_lock:
        conn = _db()
        try:
            conn.execute('DELETE FROM tasks WHERE id=?', (task_id,))
        finally:
            conn.close()


def load_logs(limit=200):
    """加载同步日志（默认最近 200 条）"""
    conn = _db()
    try:
        rows = conn.execute('SELECT * FROM logs ORDER BY id DESC LIMIT ?', (limit,)).fetchall()
        return [_row_to_log(r) for r in rows]
    except sqlite3.Error as e:
        logger.error(f'加载日志失败: {e}')
        return []
    finally:
        conn.close()


def save_log_entry(entry):
    """追加一条同步日志，并触发按日归档"""
    with _db_lock:
        conn = _db()
        try:
            conn.execute(
                'INSERT INTO logs (task_id,source,destination,success,message,output,'
                'files_synced,error_count,trigger,time) VALUES (?,?,?,?,?,?,?,?,?,?)',
                (entry.get('task_id'), entry.get('source', ''), entry.get('destination', ''),
                 1 if entry.get('success') else 0,
                 entry.get('message', ''), (entry.get('output') or '')[:8000],
                 entry.get('files_synced', 0), entry.get('error_count', 0),
                 entry.get('trigger', 'manual'),
                 entry.get('time', datetime.now().strftime('%Y-%m-%d %H:%M:%S'))))
        finally:
            conn.close()
    # 检查归档
    _maybe_archive_logs()


def _maybe_archive_logs():
    """当 DB 中日志超过 500 条时把旧日志按月归档到 SQLite 归档表/文件"""
    try:
        conn = _db()
        try:
            count = conn.execute('SELECT COUNT(*) FROM logs').fetchone()[0]
            if count <= 500:
                return
            # 把 30 天前的日志归档到 logs_archive 表
            conn.execute('''CREATE TABLE IF NOT EXISTS logs_archive (
                id INTEGER PRIMARY KEY, task_id INTEGER, source TEXT, destination TEXT,
                success INTEGER, message TEXT, output TEXT, files_synced INTEGER,
                error_count INTEGER, trigger TEXT, time TEXT
            )''')
            cutoff = datetime.now().strftime('%Y-%m-%d')
            # 仅保留最近 200 条在线
            ids_to_archive = conn.execute(
                'SELECT id FROM logs ORDER BY id DESC LIMIT -1 OFFSET 200'
            ).fetchall()
            if ids_to_archive:
                ids = [r['id'] for r in ids_to_archive]
                placeholders = ','.join('?' * len(ids))
                conn.execute(f'INSERT INTO logs_archive SELECT * FROM logs WHERE id IN ({placeholders})', ids)
                conn.execute(f'DELETE FROM logs WHERE id IN ({placeholders})', ids)
                logger.info(f'已归档 {len(ids)} 条旧日志到 logs_archive')
        finally:
            conn.close()
    except Exception as e:
        logger.error(f'日志归档失败: {e}')


def clear_logs_all():
    """清除所有日志（包括归档表）"""
    with _db_lock:
        conn = _db()
        try:
            conn.execute('DELETE FROM logs')
            conn.execute('DROP TABLE IF EXISTS logs_archive')
        finally:
            conn.close()


def list_hosts():
    conn = _db()
    try:
        rows = conn.execute('SELECT * FROM hosts ORDER BY id').fetchall()
        return [dict(r) for r in rows]
    finally:
        conn.close()


def add_host(data):
    with _db_lock:
        conn = _db()
        try:
            cur = conn.execute(
                'INSERT INTO hosts (name,host,port,username,auth_type,password,key_name,mode,created_at) '
                'VALUES (?,?,?,?,?,?,?,?,?)',
                (data.get('name', ''), data.get('host', ''), str(data.get('port', '22')),
                 data.get('username', 'root'), data.get('auth_type', 'password'),
                 data.get('password', ''), data.get('key_name', ''),
                 str(data.get('mode', 'rw')),
                 datetime.now().strftime('%Y-%m-%d %H:%M:%S')))
            return cur.lastrowid
        finally:
            conn.close()


def update_host(host_id, data):
    with _db_lock:
        conn = _db()
        try:
            sets = []
            vals = []
            for k in ('name', 'host', 'port', 'username', 'auth_type', 'password', 'key_name', 'mode'):
                if k in data:
                    sets.append(f'{k}=?')
                    vals.append(str(data[k]) if k == 'port' else data[k])
            if sets:
                vals.append(host_id)
                conn.execute(f'UPDATE hosts SET {",".join(sets)} WHERE id=?', vals)
        finally:
            conn.close()


def delete_host(host_id):
    with _db_lock:
        conn = _db()
        try:
            conn.execute('DELETE FROM hosts WHERE id=?', (host_id,))
        finally:
            conn.close()


def get_setting(key, default=''):
    conn = _db()
    try:
        row = conn.execute('SELECT value FROM settings WHERE key=?', (key,)).fetchone()
        return row['value'] if row else default
    finally:
        conn.close()


def set_setting(key, value):
    with _db_lock:
        conn = _db()
        try:
            conn.execute('INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)', (key, value))
        finally:
            conn.close()


def send_webhook_notification(task_name, source, dest, success, message, files_count, error_count, duration):
    webhook_url = get_setting('webhook_url')
    if not webhook_url:
        return
    content = f"""## Rsync 同步通知
> 任务: {task_name}
> 源: {source}
> 目标: {dest}
> 状态: {'✅ 成功' if success else '❌ 失败'}
> 文件: {files_count} 个
> 耗时: {duration}
{f'> 错误: {error_count} 个' if error_count else ''}
{f'> 详情: {message}' if not success else ''}"""
    payload = json.dumps({"msgtype": "markdown", "markdown": {"content": content}}).encode('utf-8')
    req = urllib.request.Request(webhook_url, data=payload, headers={'Content-Type': 'application/json'})
    try:
        urllib.request.urlopen(req, timeout=10)
    except Exception as e:
        logger.error(f'Webhook通知发送失败: {e}')


def _get_task_stats(task_id):
    conn = _db()
    try:
        total = conn.execute('SELECT COUNT(*) FROM logs WHERE task_id=?', (task_id,)).fetchone()[0]
        success_count = conn.execute('SELECT COUNT(*) FROM logs WHERE task_id=? AND success=1', (task_id,)).fetchone()[0]
        fail_count = total - success_count
        rows = conn.execute('SELECT success FROM logs WHERE task_id=? ORDER BY id DESC LIMIT 5', (task_id,)).fetchall()
        last_5 = [bool(r['success']) for r in rows]
        return {'total': total, 'success': success_count, 'fail': fail_count, 'last_5': last_5}
    finally:
        conn.close()


def _next_task_id():
    """生成新任务 ID（基于当前最大 id + 1）"""
    conn = _db()
    try:
        row = conn.execute('SELECT MAX(id) AS m FROM tasks').fetchone()
        return (row['m'] or 0) + 1
    finally:
        conn.close()


# 兼容旧文件路径变量（仅供日志显示/迁移检测）
TASKS_FILE = DB_FILE
LOGS_FILE = DB_FILE

# ===== 定时任务：用简单时间判断替代 schedule 库（更可靠、无边界情况） =====
_scheduled_tasks = {}  # {task_id: {'func': worker_fn, 'type': ..., 'params': ...}}
_scheduled_tasks_lock = threading.Lock()

# schedule 库用于 interval 类型的简单计时器
_schedule_lock = threading.Lock()


def _should_run_schedule(schedule_info):
    """根据 schedule 配置判断 now 是否应该触发。返回 True 表示应触发。"""
    now = datetime.now()
    s_type = schedule_info['type']
    if s_type == 'daily':
        target = schedule_info['time']  # "HH:MM"
        return now.strftime('%H:%M') == target
    elif s_type == 'weekly':
        target = schedule_info['time']
        target_day = int(schedule_info['day'])  # 0=Sun, 1=Mon, ..., 6=Sat
        return now.strftime('%H:%M') == target and now.weekday() == (target_day - 1) % 7
    elif s_type == 'monthly':
        target = schedule_info['time']
        target_day = int(schedule_info['day'])
        return now.strftime('%H:%M') == target and now.day == target_day
    elif s_type == 'interval':
        return False  # interval 类型用 schedule 库处理
    return False


def _next_run_time(schedule_info):
    """计算下次执行时间字符串，供前端展示。返回如 '2026-06-04 19:18' 或 None。"""
    if not schedule_info:
        return None
    now = datetime.now()
    s_type = schedule_info['type']
    try:
        target_time_str = schedule_info.get('time', '00:00')
        h, m = map(int, target_time_str.split(':'))
        today_target = now.replace(hour=h, minute=m, second=0, microsecond=0)
    except (ValueError, KeyError):
        return None

    if s_type == 'daily':
        if today_target <= now:
            from datetime import timedelta
            today_target += timedelta(days=1)
        return today_target.strftime('%Y-%m-%d %H:%M')
    elif s_type == 'weekly':
        target_day = int(schedule_info.get('day', 0))  # 0=Sun, ...
        target_weekday = (target_day - 1) % 7  # 0=Mon in Python weekday()
        days_ahead = target_weekday - now.weekday()
        if days_ahead < 0 or (days_ahead == 0 and today_target <= now):
            days_ahead += 7
        from datetime import timedelta
        next_dt = now.replace(hour=h, minute=m, second=0, microsecond=0) + timedelta(days=days_ahead)
        return next_dt.strftime('%Y-%m-%d %H:%M')
    elif s_type == 'monthly':
        target_day = int(schedule_info.get('day', 1))
        this_month = now.replace(day=min(target_day, 28), hour=h, minute=m, second=0, microsecond=0)
        if this_month <= now:
            # 下个月同一天
            if now.month == 12:
                next_month = now.replace(year=now.year + 1, month=1)
            else:
                next_month = now.replace(month=now.month + 1)
            this_month = next_month.replace(day=min(target_day, 28),
                                             hour=h, minute=m, second=0, microsecond=0)
        return this_month.strftime('%Y-%m-%d %H:%M')
    elif s_type == 'interval':
        interval_val = int(schedule_info.get('interval', 1))
        unit = schedule_info.get('unit', 'minutes')
        from datetime import timedelta
        if unit == 'hours':
            delta = timedelta(hours=interval_val)
        else:
            delta = timedelta(minutes=interval_val)
        return (now + delta).strftime('%Y-%m-%d %H:%M')
    return None


def _register_scheduled_task(task):
    """向定时循环注册一个任务。支持 daily/weekly/monthly/interval"""
    schedule_info = task.get('schedule')
    if not schedule_info:
        return

    if schedule_info.get('type') == 'interval':
        # interval 类型仍用 schedule 库的简单计时器
        interval_val = int(schedule_info['interval'])
        with _schedule_lock:
            if schedule_info['unit'] == 'minutes':
                schedule.every(interval_val).minutes.do(
                    _make_sync_func(task)).tag(f"task_{task['id']}")
            else:
                schedule.every(interval_val).hours.do(
                    _make_sync_func(task)).tag(f"task_{task['id']}")
        return

    # daily/weekly/monthly 用自主判断（不依赖 schedule 库的 at()）
    with _scheduled_tasks_lock:
        _scheduled_tasks[task['id']] = {
            'func': _make_sync_func(task),
            'schedule': schedule_info,
        }
    logger.info(f'定时任务已注册: task_id={task["id"]} type={schedule_info["type"]} '
                f'time={schedule_info.get("time","")} day={schedule_info.get("day","")}')


def _unregister_scheduled_task(task_id):
    """从定时循环中移除一个任务"""
    with _scheduled_tasks_lock:
        _scheduled_tasks.pop(task_id, None)
    with _schedule_lock:
        schedule.clear(f"task_{task_id}")
    logger.info(f'定时任务已取消: task_id={task_id}')


def _make_sync_func(task):
    """创建一个同步执行闭包（供定时循环调用）"""
    delete_option = task.get('delete_option', True)
    source_auth = task.get('source_auth', {})
    dest_auth = task.get('dest_auth', {})
    checksum = task.get('checksum', False)
    dry_run = task.get('dry_run', False)
    bwlimit = task.get('bwlimit', '')
    include_pats = [p for p in (task.get('include_patterns') or '').split('\n') if p.strip()]
    exclude_pats = [p for p in (task.get('exclude_patterns') or '').split('\n') if p.strip()]
    task_id = task.get('id')
    task_source = task.get('source', '')
    task_dest = task.get('destination', '')
    task_remark = task.get('remark', '') or f'Task #{task_id}'

    def do_sync():
        logger.info(f'[定时] do_sync 被调用: task_id={task_id} source={task_source} -> {task_dest}')
        def worker():
            global _running_sync
            logger.info(f'[定时] worker 启动: task_id={task_id}')
            if not _sync_execution_lock.acquire(blocking=False):
                msg = f'定时同步跳过：已有同步正在运行 (task_id={task_id})'
                logger.warning(msg)
                save_log_entry({
                    'task_id': task_id,
                    'source': task_source,
                    'destination': task_dest,
                    'success': False,
                    'message': msg,
                    'output': msg,
                    'files_synced': 0,
                    'error_count': 0,
                    'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'trigger': 'schedule'
                })
                return
            sync_start = time.time()
            try:
                with _running_sync_lock:
                    if _running_sync and _running_sync.get('settled_time') and \
                            time.time() - _running_sync['settled_time'] > 30:
                        _running_sync = None
                result = run_sync(task_source, task_dest, delete_option, source_auth, dest_auth,
                                  sync_key=f'task_{task_id}', task_id=task_id, trigger='schedule',
                                  checksum=checksum, dry_run=dry_run,
                                  include_patterns=include_pats, exclude_patterns=exclude_pats,
                                  bwlimit=bwlimit)
                duration = time.time() - sync_start
                _record_metric(result, 'schedule', duration)
                save_log_entry({
                    'task_id': task_id,
                    'source': task_source,
                    'destination': task_dest,
                    'success': result['success'],
                    'message': result['message'],
                    'output': result['output'][:2000],
                    'files_synced': result.get('files_synced', 0),
                    'error_count': result.get('error_count', 0),
                    'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'trigger': 'schedule'
                })
                _finalize_sync_status(f'task_{task_id}', result['success'])
                send_webhook_notification(task_remark, task_source, task_dest, result['success'],
                                          result['message'], result.get('files_synced', 0),
                                          result.get('error_count', 0), f'{duration:.1f}s')
                logger.info(f'[定时] worker 完成: task_id={task_id} success={result["success"]}')
            except Exception as e:
                logger.error(f'定时同步异常 (task_id={task_id}): {e}', exc_info=True)
                # 即使异常也保存一条失败日志，确保用户能看到"发生过"
                save_log_entry({
                    'task_id': task_id,
                    'source': task_source,
                    'destination': task_dest,
                    'success': False,
                    'message': f'定时同步异常: {e}',
                    'output': f'异常: {e}\n',
                    'files_synced': 0,
                    'error_count': 1,
                    'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'trigger': 'schedule'
                })
                _finalize_sync_status(f'task_{task_id}', False)
                send_webhook_notification(task_remark, task_source, task_dest, False,
                                          f'定时同步异常: {e}', 0, 1, '0s')
            finally:
                _sync_execution_lock.release()
                logger.info(f'[定时] worker 退出: task_id={task_id}')
        threading.Thread(target=worker, daemon=True, name=f'sync-task-{task_id}').start()

    return do_sync


def run_scheduled_tasks():
    """定时任务轮询循环（每 15 秒检查一次）"""
    logger.info('定时任务轮询线程已启动 (pid=%s)', os.getpid())
    _last_fired = {}  # {task_id: 'YYYYMMDD_HHMM'} 防止同分钟重复触发
    _heartbeat_count = 0
    while True:
        # ---- 第 1 步：daily/weekly/monthly 自主检查（放在前面，不依赖 schedule） ----
        with _scheduled_tasks_lock:
            task_snapshot = list(_scheduled_tasks.items())
        now = datetime.now()
        now_key = now.strftime('%Y%m%d_%H%M')
        if task_snapshot:
            for task_id, entry in task_snapshot:
                try:
                    sched = entry.get('schedule', {})
                    if _should_run_schedule(sched):
                        if _last_fired.get(task_id) == now_key:
                            continue  # 本轮已触发过，跳过
                        _last_fired[task_id] = now_key
                        logger.info(f'定时触发同步: task_id={task_id} type={sched.get("type")} '
                                    f'time={sched.get("time","")} now={now.strftime("%H:%M:%S")}')
                        entry['func']()
                except Exception as e:
                    logger.error(f'检查定时任务失败 (task_id={task_id}): {e}', exc_info=True)
        else:
            # 没有注册任何定时任务，降低日志频率为每 5 分钟一次
            if _heartbeat_count % 20 == 0:
                logger.info('定时轮询心跳: 当前无已注册的定时任务')

        # ---- 第 2 步：interval 类型用 schedule 库 ----
        try:
            with _schedule_lock:
                schedule.run_pending()
        except Exception as e:
            logger.error(f'schedule.run_pending() 异常（不影响 daily/weekly 检查）: {e}')

        # ---- 心跳日志：每 5 分钟一次 ----
        _heartbeat_count += 1
        if _heartbeat_count % 20 == 0:
            logger.info(f'定时轮询心跳: 已注册 {len(task_snapshot)} 个定时任务, '
                        f'当前时间={now.strftime("%Y-%m-%d %H:%M:%S")}')
            if task_snapshot:
                for task_id, entry in task_snapshot:
                    logger.info(f'  - task_id={task_id} schedule={entry.get("schedule",{})}')

        if _shutdown_event.wait(15):
            break
    logger.info('定时轮询线程已退出')

# 启动定时轮询线程
threading.Thread(target=run_scheduled_tasks, daemon=True, name='scheduler').start()

# 运行中的同步状态（线程安全），同一时间只允许一个同步运行
_running_sync = None     # {process, sync_key, source, destination, start_time, files_count, current_file, status, trigger, task_id, total_bytes, elapsed_seconds}
_running_sync_lock = threading.Lock()

# 同步执行互斥锁：保证任意时刻只有一个同步在执行（手动/定时统一排队）
_sync_execution_lock = threading.Lock()

# rsync 安全网超时（秒）：仅当 rsync 完全无响应时触发
# 正常情况下 rsync 的 --timeout=300 会处理单个文件/连接的空闲超时
# 只要还在传文件，不管多少文件、多大，都不会被这个超时杀掉
RSYNC_SAFETY_TIMEOUT = 86400 * 3  # 3天，几乎不可能触发

# rsync 单文件空闲超时（秒）：单个文件超过此时间无数据传输则跳过
RSYNC_IDLE_TIMEOUT = 300  # 5分钟

# 网络中断重试：指数退避（最多 3 次）
SYNC_RETRY_MAX = 3
SYNC_RETRY_BASE_DELAY = 5  # 首次重试延迟 5 秒

# 磁盘空间安全余量：预检时要求目标端至少这么多剩余（字节）
DISK_SPACE_SAFETY_MARGIN = 100 * 1024 * 1024  # 100MB

# ===== Prometheus 指标（不依赖外部库，直接以文本格式输出） =====
_metrics = {
    'sync_total': {},          # {trigger: count}
    'sync_success_total': {},  # {trigger: count}
    'sync_failure_total': {},  # {trigger: count}
    'sync_duration_seconds': [],  # [(trigger, duration)]
    'sync_files_total': [],    # [files]
    'sync_errors_total': [],    # [errors]
    'start_time': time.time(),
}
_metrics_lock = threading.Lock()


def _record_metric(result, trigger, duration):
    """记录一次同步的指标"""
    with _metrics_lock:
        _metrics['sync_total'][trigger] = _metrics['sync_total'].get(trigger, 0) + 1
        if result.get('success'):
            _metrics['sync_success_total'][trigger] = _metrics['sync_success_total'].get(trigger, 0) + 1
        else:
            _metrics['sync_failure_total'][trigger] = _metrics['sync_failure_total'].get(trigger, 0) + 1
        _metrics['sync_duration_seconds'].append((trigger, duration))
        _metrics['sync_files_total'].append(result.get('files_synced', 0))
        _metrics['sync_errors_total'].append(result.get('error_count', 0))
        # 滑动窗口：只保留最近 200 条
        for k in ('sync_duration_seconds', 'sync_files_total', 'sync_errors_total'):
            _metrics[k] = _metrics[k][-200:]


def _format_prometheus_metrics():
    """生成 Prometheus 文本格式的指标"""
    with _metrics_lock:
        lines = []
        lines.append('# HELP rsync_web_info Rsync-web service info')
        lines.append('# TYPE rsync_web_info gauge')
        lines.append(f'rsync_web_info{{version="{VERSION}"}} 1')
        lines.append(f'rsync_web_uptime_seconds {int(time.time() - _metrics["start_time"])}')

        lines.append('# HELP rsync_web_sync_total Total sync runs by trigger')
        lines.append('# TYPE rsync_web_sync_total counter')
        for trig, n in _metrics['sync_total'].items():
            lines.append(f'rsync_web_sync_total{{trigger="{trig}"}} {n}')

        lines.append('# HELP rsync_web_sync_success_total Successful sync runs')
        lines.append('# TYPE rsync_web_sync_success_total counter')
        for trig, n in _metrics['sync_success_total'].items():
            lines.append(f'rsync_web_sync_success_total{{trigger="{trig}"}} {n}')

        lines.append('# HELP rsync_web_sync_failure_total Failed sync runs')
        lines.append('# TYPE rsync_web_sync_failure_total counter')
        for trig, n in _metrics['sync_failure_total'].items():
            lines.append(f'rsync_web_sync_failure_total{{trigger="{trig}"}} {n}')

        # 同步耗时（最近 200 次）
        durations = _metrics['sync_duration_seconds']
        if durations:
            avg = sum(d for _, d in durations) / len(durations)
            mx = max(durations, key=lambda x: x[1])
            lines.append('# HELP rsync_web_sync_duration_seconds_avg Average sync duration')
            lines.append('# TYPE rsync_web_sync_duration_seconds_avg gauge')
            lines.append(f'rsync_web_sync_duration_seconds_avg {avg:.2f}')
            lines.append('# HELP rsync_web_sync_duration_seconds_max Maximum recent sync duration')
            lines.append('# TYPE rsync_web_sync_duration_seconds_max gauge')
            lines.append(f'rsync_web_sync_duration_seconds_max{{trigger="{mx[0]}"}} {mx[1]:.2f}')

        # 文件统计
        files = _metrics['sync_files_total']
        if files:
            lines.append(f'rsync_web_sync_files_last {files[-1]}')
            lines.append(f'rsync_web_sync_files_avg {sum(files)/len(files):.1f}')
        errs = _metrics['sync_errors_total']
        if errs:
            lines.append(f'rsync_web_sync_errors_last {errs[-1]}')
            lines.append(f'rsync_web_sync_errors_avg {sum(errs)/len(errs):.1f}')

        return '\n'.join(lines) + '\n'


def _check_disk_space(path, required_bytes):
    """预检目标端磁盘空间（仅本地路径）。返回 (ok, available_bytes, message)"""
    try:
        if not path or ':' in path and '@' in path.split(':')[0]:
            return True, -1, ''  # 远程路径跳过
        target = os.path.dirname(path.rstrip('/')) or path
        if not os.path.exists(target):
            try:
                os.makedirs(target, exist_ok=True)
            except OSError:
                return True, -1, ''  # 无法创建则跳过预检
        usage = shutil.disk_usage(target)
        available = usage.free
        if available < required_bytes + DISK_SPACE_SAFETY_MARGIN:
            return False, available, \
                f'目标磁盘空间不足：可用 {available//1024//1024}MB，预估需要 {required_bytes//1024//1024}MB'
        return True, available, ''
    except Exception as e:
        return True, -1, f'磁盘预检异常（已跳过）: {e}'


def _estimate_source_bytes(source, is_remote):
    """估算源端待传输字节数（用于磁盘空间预检）。仅作粗略估计。"""
    if is_remote:
        return -1  # 远程源难以精确估算
    try:
        total = 0
        for dirpath, _, filenames in os.walk(source):
            for fn in filenames:
                fp = os.path.join(dirpath, fn)
                try:
                    if os.path.isfile(fp) and not os.path.islink(fp):
                        total += os.path.getsize(fp)
                except OSError:
                    continue
        return total
    except Exception:
        return -1


def _is_retryable_error(process, stderr):
    """判断 rsync 失败是否可重试（网络类错误）"""
    if process.returncode == 0:
        return False
    if not stderr:
        return True  # 无 stderr 输出大概率是连接断开
    s = stderr.lower()
    indicators = [
        'connection reset', 'connection refused', 'connection closed',
        'broken pipe', 'timed out', 'timeout',
        'network is unreachable', 'host unreachable',
        'no route to host', 'ssh: connect',
        'unexpected EOF', 'connection unexpectedly',
        'rsync error: error in socket IO',
        'ssh_exchange_identification',
    ]
    return any(ind.lower() in s for ind in indicators)


# SSH 密钥存储目录
SSH_KEYS_DIR = '/app/data/keys'


def _list_ssh_keys():
    """列出已保存的SSH密钥"""
    os.makedirs(SSH_KEYS_DIR, exist_ok=True)
    keys = []
    for f in os.listdir(SSH_KEYS_DIR):
        fp = os.path.join(SSH_KEYS_DIR, f)
        if os.path.isfile(fp):
            keys.append({'name': f, 'size': os.path.getsize(fp)})
    keys.sort(key=lambda x: x['name'])
    return keys


def _save_ssh_key(filename, content):
    """保存SSH密钥文件"""
    os.makedirs(SSH_KEYS_DIR, exist_ok=True)
    # 安全检查：防止路径穿越
    filename = os.path.basename(filename)
    if not filename:
        raise ValueError('无效的文件名')
    filepath = os.path.join(SSH_KEYS_DIR, filename)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(content)
    os.chmod(filepath, 0o600)
    return filename


def _delete_ssh_key(filename):
    """删除SSH密钥文件"""
    filename = os.path.basename(filename)
    filepath = os.path.join(SSH_KEYS_DIR, filename)
    if os.path.exists(filepath):
        os.remove(filepath)
        return True
    return False


def _get_key_path(key_name):
    """获取密钥文件的完整路径"""
    if not key_name:
        return ''
    key_name = os.path.basename(key_name)
    path = os.path.join(SSH_KEYS_DIR, key_name)
    return path if os.path.isfile(path) else ''


def _get_auth_info(auth, default_type='password'):
    """从认证配置中获取认证信息，返回 (auth_type, password, key_path)"""
    if not auth or not isinstance(auth, dict):
        # 向后兼容：旧任务可能没有 auth 结构
        return default_type, '', ''
    auth_type = auth.get('type', 'password')
    if auth_type == 'key':
        return 'key', '', _get_key_path(auth.get('key', ''))
    else:
        return 'password', auth.get('password', ''), ''


def _build_ssh_cmd_options(source, destination, source_auth, dest_auth, is_remote_source, is_remote_dest):
    """根据认证配置构建 rsync 的 SSH 命令选项
    支持场景：本地↔本地、本地↔远程、远程↔远程
    远程↔远程时，若两端使用不同密钥，自动生成临时SSH config文件
    """
    env = os.environ.copy()
    base_opts = 'ssh -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -o ConnectTimeout=15'

    src_type, src_pwd, src_key = _get_auth_info(source_auth)
    dst_type, dst_pwd, dst_key = _get_auth_info(dest_auth)
    src_port = str(source_auth.get('port', '22') if source_auth else '22').strip() or '22'
    dst_port = str(dest_auth.get('port', '22') if dest_auth else '22').strip() or '22'
    logger.info(f'SSH认证: src={src_type} port={src_port}, dst={dst_type} port={dst_port}')

    # 解析远程主机名（用于SSH config）
    src_host = dst_host = ''
    if is_remote_source:
        try: _, src_host, _ = _parse_remote_path(source)
        except Exception: pass
    if is_remote_dest:
        try: _, dst_host, _ = _parse_remote_path(destination)
        except Exception: pass

    # 远程↔远程 + 密钥认证：生成SSH config文件支持不同主机用不同密钥
    if is_remote_source and is_remote_dest and (src_type == 'key' or dst_type == 'key'):
        config_lines = []
        if src_type == 'key' and src_key and src_host:
            config_lines.extend([f'Host {src_host}', f'    IdentityFile {src_key}'])
            if src_port != '22': config_lines.append(f'    Port {src_port}')
            config_lines.append('')
        if dst_type == 'key' and dst_key and dst_host:
            config_lines.extend([f'Host {dst_host}', f'    IdentityFile {dst_key}'])
            if dst_port != '22': config_lines.append(f'    Port {dst_port}')
            config_lines.append('')

        if config_lines:
            config_file = tempfile.NamedTemporaryFile(
                mode='w', suffix='_ssh_config', delete=False, prefix='rsync_')
            config_file.write('\n'.join(config_lines))
            config_file.close()
            os.chmod(config_file.name, 0o600)
            return f'{base_opts} -F {config_file.name}', env, {}, config_file.name

    # 单端远程 + 密钥
    if is_remote_source and src_type == 'key' and src_key:
        port_opt = f' -p {src_port}' if src_port != '22' else ''
        return f'{base_opts} -i {src_key}{port_opt}', env, {}, None
    if is_remote_dest and dst_type == 'key' and dst_key:
        port_opt = f' -p {dst_port}' if dst_port != '22' else ''
        return f'{base_opts} -i {dst_key}{port_opt}', env, {}, None

    # 单端远程 + 密码
    password = ''
    port_val = '22'
    if is_remote_source and src_type == 'password':
        password = src_pwd
        port_val = src_port
    elif is_remote_dest and dst_type == 'password':
        password = dst_pwd
        port_val = dst_port
    if password:
        port_opt = f' -p {port_val}' if port_val != '22' else ''
        return f'sshpass -e {base_opts}{port_opt}', env, {'SSHPASS': password}, None

    return base_opts, env, {}, None


def _finalize_sync_status(sync_key, success):
    """在记录日志之后，将同步状态从 'running' 改为最终结果"""
    with _running_sync_lock:
        if _running_sync and _running_sync.get('sync_key') == sync_key:
            if _running_sync.get('status') != 'stopped':
                _running_sync['status'] = 'success' if success else 'failed'
            _running_sync['settled_time'] = time.time()
            logger.info(f'同步状态已更新: status={_running_sync["status"]}')


def _parse_remote_path(path):
    """解析 user@host:path 格式的远程路径"""
    if '@' not in path or ':' not in path:
        raise ValueError(f'无效的远程路径格式: {path}，应为 user@host:path')
    at_pos = path.index('@')
    colon_pos = path.index(':')
    if at_pos >= colon_pos:
        raise ValueError(f'无效的远程路径格式: {path}')
    username = path[:at_pos]
    host = path[at_pos + 1:colon_pos]
    remote_path = path[colon_pos + 1:]
    if not username or not host or not remote_path:
        raise ValueError(f'无效的远程路径: {path}，用户名/主机/路径均不能为空')
    return username, host, remote_path


def _posix_join(base, name):
    """用POSIX斜杠拼接路径，避免Windows的os.path.join"""
    if base.endswith('/'):
        return base + name
    return base + '/' + name


_RSYNC_ERR_RE = re.compile(
    r'rsync:\s*(?:\[(?:sender|receiver|generator)\]\s*)?'
    r'(.*?(?:failed|error))[\s:]+(.*?)(?:\s*\(\d+\))?\s*$',
    re.IGNORECASE
)

def _parse_rsync_errors(stderr_text):
    """从 rsync stderr 中提取错误列表，返回 (error_count, error_messages)"""
    errors = []
    for line in stderr_text.split('\n'):
        line = line.strip()
        if not line:
            continue
        m = _RSYNC_ERR_RE.match(line)
        if m:
            detail = f'{m.group(1).strip()} - {m.group(2).strip()}'
            detail = detail[:200]
            errors.append(detail)
        elif 'rsync:' in line.lower() and line not in errors:
            errors.append(line[:200])
    return len(errors), errors


def _run_rsync_with_progress(cmd, env=None, sync_info=None):
    """运行 rsync 并实时记录进度，支持停止同步

    sync_info: dict with keys: sync_key, source, destination, trigger, task_id
    """
    global _running_sync

    # 在 cmd 中检测是否需要启用 --info=progress2（用于显示真实百分比）
    # dry_run(--list-only) 与 progress2 互斥，仅在普通模式下追加
    has_progress2 = '--info=progress2' in cmd
    use_progress2 = has_progress2 or (
        '--list-only' not in cmd and '-n' not in cmd and '--dry-run' not in cmd
    )
    if use_progress2 and not has_progress2:
        cmd = cmd + ['--info=progress2']

    process = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        env=env,
        preexec_fn=os.setsid if os.name != 'nt' else None
    )

    start_time = time.time()

    # 注册运行中的同步
    if sync_info:
        with _running_sync_lock:
            _running_sync = {
                'process': process,
                'sync_key': sync_info.get('sync_key', ''),
                'source': sync_info.get('source', ''),
                'destination': sync_info.get('destination', ''),
                'start_time': start_time,
                'files_count': 0,
                'error_count': 0,
                'current_file': '',
                'progress_percent': 0,
                'transfer_speed': '',
                'status': 'running',
                'trigger': sync_info.get('trigger', 'manual'),
                'task_id': sync_info.get('task_id'),
                'elapsed_seconds': 0,
            }

    stderr_chunks = []
    def _drain_stderr():
        try:
            for chunk in process.stderr:
                stderr_chunks.append(chunk)
        except Exception:
            pass
    stderr_thread = threading.Thread(target=_drain_stderr, daemon=True)
    stderr_thread.start()

    last_log_time = start_time
    files_transferred = 0
    last_output_line = ''
    progress_re = re.compile(r'\b(\d{1,3})%(?:\s+([\d.]+[KMG]?B/s))?')

    try:
        for line in process.stdout:
            line = line.rstrip('\n')
            last_output_line = line
            now = time.time()

            if line and not line.startswith(('sending', 'receiving', 'sent ', 'total size')):
                files_transferred += 1

            # 解析 --info=progress2 进度百分比
            pct = 0
            speed = ''
            if use_progress2 and '\r' not in line:
                # progress2 输出：包含 " 45%  12.34MB/s  0:01:23" 这种片段
                m = progress_re.search(line)
                if m:
                    pct = int(m.group(1))
                    speed = m.group(2) or ''
            elif use_progress2:
                # progress2 用 \r 覆盖同行，取最后一次
                tail = line.split('\r')[-1]
                m = progress_re.search(tail)
                if m:
                    pct = int(m.group(1))
                    speed = m.group(2) or ''

            if sync_info:
                with _running_sync_lock:
                    if _running_sync:
                        _running_sync['files_count'] = files_transferred
                        _running_sync['current_file'] = line[:200] if line else ''
                        _running_sync['elapsed_seconds'] = int(now - start_time)
                        if pct:
                            _running_sync['progress_percent'] = pct
                        if speed:
                            _running_sync['transfer_speed'] = speed

            if now - last_log_time >= 30:
                elapsed = now - start_time
                elapsed_str = f'{int(elapsed//3600)}h{int((elapsed%3600)//60)}m'
                logger.info(
                    f'[进度] 已运行 {elapsed_str}，已传输 {files_transferred} 个文件，'
                    f'最近: {line[:100]}'
                )
                last_log_time = now

        try:
            process.wait(timeout=RSYNC_SAFETY_TIMEOUT)
        except subprocess.TimeoutExpired:
            process.kill()
            process.wait()
            process.returncode = -1
    finally:
        stderr_thread.join(timeout=10)

    stderr = ''.join(stderr_chunks)

    error_count, error_messages = _parse_rsync_errors(stderr)

    elapsed = time.time() - start_time
    elapsed_str = f'{int(elapsed//3600)}h{int((elapsed%3600)//60)}m{int(elapsed%60)}s'
    logger.info(f'[完成] 总耗时 {elapsed_str}，传输 {files_transferred} 个文件')

    if process.returncode == -1 and not stderr:
        stderr = f'rsync 运行超过 {RSYNC_SAFETY_TIMEOUT//3600} 小时，安全网超时终止'

    # 写入最终统计（不改变 running 状态，由路由处理器在记录日志后设置）
    if sync_info:
        with _running_sync_lock:
            if _running_sync and _running_sync.get('sync_key') == sync_info.get('sync_key'):
                _running_sync['files_count'] = files_transferred
                _running_sync['error_count'] = error_count
                _running_sync['elapsed_seconds'] = int(time.time() - start_time)
                if not use_progress2:
                    _running_sync['progress_percent'] = 100  # 非进度模式视为完成

    return process, last_output_line, stderr, files_transferred, error_count, error_messages, elapsed_str


def run_sync(source, destination, delete_option=True, source_auth=None, dest_auth=None, sync_key='',
             task_id=None, trigger='manual', checksum=False, dry_run=False,
             include_patterns=None, exclude_patterns=None, bwlimit='',
             retry_count=0, max_retries=SYNC_RETRY_MAX):
    """执行rsync同步，支持本地↔本地、本地↔远程、远程↔远程（纯rsync+SSH，无需FUSE）
    集成：磁盘预检 + 网络中断自动重试（指数退避）
    """
    is_remote_source = ':' in source and '@' in source.split(':')[0]
    is_remote_dest = ':' in destination and '@' in destination.split(':')[0]

    logger.info(f'开始同步: {source} -> {destination} (远程源={is_remote_source}, 远程目标={is_remote_dest}, '
                f'dry_run={dry_run}, checksum={checksum}, retry={retry_count}/{max_retries})')

    # 磁盘空间预检（仅在非 dry_run 且目标为本地时执行）
    if not dry_run and not is_remote_dest:
        est_bytes = _estimate_source_bytes(source, is_remote_source)
        if est_bytes > 0:
            ok, avail, msg = _check_disk_space(destination, est_bytes)
            if not ok:
                logger.error(f'磁盘预检失败: {msg}')
                return {
                    'success': False,
                    'message': msg,
                    'output': f'[磁盘空间预检] {msg}\n',
                    'files_synced': 0,
                    'error_count': 1,
                    'error_messages': [msg],
                }
            elif avail > 0:
                logger.info(f'磁盘预检通过: 目标端可用 {avail//1024//1024}MB, 预估需要 {est_bytes//1024//1024}MB')

    # 确保本地目标目录存在（dry_run时不创建）
    if not dry_run and not is_remote_dest:
        dest_dir = os.path.dirname(destination.rstrip('/'))
        if dest_dir:
            os.makedirs(dest_dir, exist_ok=True)

    # 只对本地源路径添加斜杠结尾
    if not is_remote_source:
        if not source.endswith('/'):
            source += '/'

    cmd = ['rsync', '-av', '--partial', '--ignore-errors',
           '--partial-dir=.rsync-partial',
           f'--timeout={RSYNC_IDLE_TIMEOUT}']
    if delete_option:
        cmd.append('--delete')
    if checksum:
        cmd.append('--checksum')
    if dry_run:
        cmd.append('--list-only')  # 仅列出变更，不实际传输
    if bwlimit:
        cmd.append(f'--bwlimit={bwlimit}')

    # 包含/排除规则
    for pat in (include_patterns or []):
        pat = pat.strip()
        if pat:
            cmd.extend(['--include', pat])
    for pat in (exclude_patterns or []):
        pat = pat.strip()
        if pat:
            cmd.extend(['--exclude', pat])

    # 根据认证配置构建 SSH 命令选项
    ssh_cmd, env, extra_env, _cleanup_file = _build_ssh_cmd_options(
        source, destination, source_auth, dest_auth, is_remote_source, is_remote_dest)
    if is_remote_source or is_remote_dest:
        cmd.extend(['-e', ssh_cmd])
        env.update(extra_env)

    cmd.extend([source, destination])

    sync_info = {
        'sync_key': sync_key,
        'source': source,
        'destination': destination,
        'trigger': trigger,
        'task_id': task_id,
    }

    try:
        process, last_line, stderr, files_processed, error_count, error_messages, elapsed_str = _run_rsync_with_progress(cmd, env=env, sync_info=sync_info)
    finally:
        if _cleanup_file and os.path.exists(_cleanup_file):
            try:
                os.remove(_cleanup_file)
            except OSError:
                pass

    success = process.returncode == 0

    # 网络中断自动重试（指数退避，仅对可重试错误生效）
    if not success and retry_count < max_retries and _is_retryable_error(process, stderr):
        delay = SYNC_RETRY_BASE_DELAY * (2 ** retry_count)
        logger.warning(f'检测到可重试错误，{delay}秒后进行第 {retry_count+1}/{max_retries} 次重试')
        time.sleep(delay)
        # 递归重试（参数保持不变）
        retry_result = run_sync(
            source, destination, delete_option, source_auth, dest_auth,
            sync_key, task_id, trigger, checksum, dry_run,
            include_patterns, exclude_patterns, bwlimit,
            retry_count=retry_count + 1, max_retries=max_retries
        )
        retry_result['message'] = f'重试 {retry_count+1} 次后: {retry_result["message"]}'
        return retry_result

    if error_count > 0:
        status_text = f'成功 {files_processed} 个文件，失败 {error_count} 个文件，耗时{elapsed_str}'
        output = f'成功 {files_processed} 个文件，失败 {error_count} 个文件，耗时 {elapsed_str}\n'
        if error_messages:
            output += '\n失败详情:\n' + '\n'.join(f'  - {e}' for e in error_messages[:20])
    else:
        status_text = f'同步{"成功" if success else "失败"}，{files_processed}个文件，耗时{elapsed_str}'
        output = f'传输 {files_processed} 个文件，耗时 {elapsed_str}\n'
    if last_line:
        output += f'最后: {last_line}\n'
    if stderr:
        output += stderr
    logger.info(f'同步完成: {status_text}, 退出码={process.returncode}')

    if retry_count > 0:
        status_text = f'[重试{retry_count}次] ' + status_text

    return {
        'success': success,
        'message': status_text,
        'output': output,
        'files_synced': files_processed,
        'error_count': error_count,
        'error_messages': error_messages,
    }



def _daily_backup_loop():
    """每日凌晨 3 点自动备份数据库"""
    while True:
        now = datetime.now()
        next_run = now.replace(hour=3, minute=0, second=0, microsecond=0)
        if next_run <= now:
            next_run = next_run.replace(day=now.day + 1)  # Python 中会自动跨月
        wait_sec = (next_run - now).total_seconds()
        if _shutdown_event.wait(wait_sec):
            break
        _backup_db()
    logger.info('每日备份线程已退出')


threading.Thread(target=_daily_backup_loop, daemon=True).start()


def get_actual_mount_points():
    """获取挂载点信息"""
    try:
        home_path = os.path.realpath('/home')
        data_path = os.path.realpath('/data')
        return {'/home': home_path, '/data': data_path}
    except Exception as e:
        logger.error(f'获取挂载点失败: {e}')
        return {'/home': '/home', '/data': '/data'}


@app.route('/')
def index():
    tasks = load_tasks()
    return render_template('index.html', tasks=tasks, version=VERSION)


@app.route('/sync', methods=['POST'])
def sync():
    global _running_sync
    # 尝试获取执行锁（非阻塞），防止并发同步
    if not _sync_execution_lock.acquire(blocking=False):
        return jsonify({'error': '已有同步任务正在运行，请等待完成'}), 409
    try:
        # 清理已结束超过30秒的旧状态
        with _running_sync_lock:
            if _running_sync and _running_sync.get('settled_time') and \
                    time.time() - _running_sync['settled_time'] > 30:
                _running_sync = None

        source = request.form.get('source')
        destination = request.form.get('destination')
        schedule_time = request.form.get('schedule_time', '')
        delete_option = request.form.get('delete_option') == 'true'
        checksum = request.form.get('checksum') == 'true'
        dry_run = request.form.get('dry_run') == 'true'
        bwlimit = request.form.get('bwlimit', '')
        include_patterns = [p for p in request.form.get('include_patterns', '').split('\n') if p.strip()]
        exclude_patterns = [p for p in request.form.get('exclude_patterns', '').split('\n') if p.strip()]
        source_auth = json.loads(request.form.get('source_auth', '{}'))
        dest_auth = json.loads(request.form.get('dest_auth', '{}'))

        if not source or not destination:
            return jsonify({'error': '请填写源路径和目标路径'}), 400

        sync_start = time.time()
        result = run_sync(source, destination, delete_option, source_auth, dest_auth,
                          sync_key='manual', trigger='manual',
                          checksum=checksum, dry_run=dry_run,
                          include_patterns=include_patterns,
                          exclude_patterns=exclude_patterns, bwlimit=bwlimit)
        duration = time.time() - sync_start
        _record_metric(result, 'manual', duration)

        # 记录同步日志
        save_log_entry({
            'task_id': None,
            'source': source,
            'destination': destination,
            'success': result['success'],
            'message': result['message'],
            'output': result['output'][:2000],
            'files_synced': result.get('files_synced', 0),
            'error_count': result.get('error_count', 0),
            'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'trigger': 'manual'
        })
        _finalize_sync_status('manual', result['success'])

        if result['success'] and schedule_time:
            schedule_data = {'type': 'daily', 'time': schedule_time}
            task = {
                'source': source,
                'destination': destination,
                'delete_option': delete_option,
                'checksum': checksum,
                'dry_run': dry_run,
                'bwlimit': bwlimit,
                'include_patterns': '\n'.join(include_patterns),
                'exclude_patterns': '\n'.join(exclude_patterns),
                'source_auth': source_auth,
                'dest_auth': dest_auth,
                'schedule': schedule_data,
                'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            new_id = add_task_to_db(task)
            task['id'] = new_id
            _register_scheduled_task(task)

        return jsonify(result)
    finally:
        _sync_execution_lock.release()


@app.route('/tasks', methods=['GET'])
def list_tasks():
    return jsonify(load_tasks())


@app.route('/logs', methods=['GET'])
def list_logs():
    return jsonify(load_logs())


@app.route('/logs', methods=['DELETE'])
def clear_logs():
    """清除所有同步日志"""
    try:
        clear_logs_all()
        logger.info('同步日志已全部清除')
        return jsonify({'success': True})
    except Exception as e:
        logger.error(f'清除日志失败: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/tasks', methods=['POST'])
def add_task():
    source = request.form.get('source')
    destination = request.form.get('destination')
    schedule_json = request.form.get('schedule')
    delete_option = request.form.get('delete_option') == 'true'
    checksum = request.form.get('checksum') == 'true'
    dry_run = request.form.get('dry_run') == 'true'
    bwlimit = request.form.get('bwlimit', '')
    include_patterns = request.form.get('include_patterns', '')
    exclude_patterns = request.form.get('exclude_patterns', '')
    remark = request.form.get('remark', '')
    source_auth = json.loads(request.form.get('source_auth', '{}'))
    dest_auth = json.loads(request.form.get('dest_auth', '{}'))

    if not source or not destination:
        return jsonify({'success': False, 'error': '请填写源路径和目标路径'}), 400

    task = {
        'source': source,
        'destination': destination,
        'delete_option': delete_option,
        'checksum': checksum,
        'dry_run': dry_run,
        'bwlimit': bwlimit,
        'include_patterns': include_patterns,
        'exclude_patterns': exclude_patterns,
        'remark': remark,
        'source_auth': source_auth,
        'dest_auth': dest_auth,
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    if schedule_json:
        task['schedule'] = json.loads(schedule_json)

    new_id = add_task_to_db(task)
    task['id'] = new_id
    if task.get('schedule'):
        _register_scheduled_task(task)

    return jsonify({'success': True, 'id': new_id})


@app.route('/tasks/<int:task_id>/sync', methods=['POST'])
def execute_sync(task_id):
    global _running_sync
    # 尝试获取执行锁（非阻塞），防止并发同步
    if not _sync_execution_lock.acquire(blocking=False):
        return jsonify({'success': False, 'error': '已有同步任务正在运行，请等待完成'}), 409
    try:
        # 清理已结束超过30秒的旧状态
        with _running_sync_lock:
            if _running_sync and _running_sync.get('settled_time') and \
                    time.time() - _running_sync['settled_time'] > 30:
                _running_sync = None

        tasks = load_tasks()
        task = next((t for t in tasks if t['id'] == task_id), None)

        if not task:
            return jsonify({'success': False, 'error': '任务不存在'}), 404

        sync_start = time.time()
        result = run_sync(task['source'], task['destination'], task.get('delete_option', True),
                          task.get('source_auth', {}), task.get('dest_auth', {}),
                          sync_key=f'task_{task_id}', task_id=task_id, trigger='manual',
                          checksum=task.get('checksum', False),
                          dry_run=task.get('dry_run', False),
                          include_patterns=[p for p in (task.get('include_patterns') or '').split('\n') if p.strip()],
                          exclude_patterns=[p for p in (task.get('exclude_patterns') or '').split('\n') if p.strip()],
                          bwlimit=task.get('bwlimit', ''))
        duration = time.time() - sync_start
        _record_metric(result, 'manual', duration)

        # 记录同步日志
        save_log_entry({
            'task_id': task['id'],
            'source': task['source'],
            'destination': task['destination'],
            'success': result['success'],
            'message': result['message'],
            'output': result['output'][:2000],
            'files_synced': result.get('files_synced', 0),
            'error_count': result.get('error_count', 0),
            'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'trigger': 'manual'
        })
        _finalize_sync_status(f'task_{task_id}', result['success'])
        send_webhook_notification(task.get('remark', '') or f'Task #{task_id}',
                                  task['source'], task['destination'], result['success'],
                                  result['message'], result.get('files_synced', 0),
                                  result.get('error_count', 0), f'{duration:.1f}s')

        return jsonify(result)
    finally:
        _sync_execution_lock.release()


@app.route('/tasks/<int:task_id>', methods=['DELETE'])
def delete_task(task_id):
    delete_task_from_db(task_id)
    _unregister_scheduled_task(task_id)
    return jsonify({'success': True})


@app.route('/list_dir', methods=['GET'])
def list_dir():
    path = request.args.get('path', '/')

    try:
        mount_info = get_actual_mount_points()

        base_path = None
        for mount_point in ['/home', '/data']:
            if path.startswith(mount_point):
                base_path = mount_point
                break

        if not base_path:
            base_path = '/home'
            path = base_path

        if base_path in mount_info:
            real_path = path.replace(base_path, mount_info[base_path], 1)
        else:
            real_path = path

        full_path = os.path.abspath(real_path)

        items = []
        if os.path.exists(full_path):
            if path not in mount_info.keys():
                parent_path = str(Path(path).parent)
                if any(parent_path.startswith(mount) for mount in mount_info.keys()):
                    items.append({
                        'name': '..',
                        'path': parent_path,
                        'type': 'directory'
                    })

            try:
                for item in os.listdir(full_path):
                    try:
                        item_path = os.path.join(full_path, item)
                        if os.access(item_path, os.R_OK):
                            display_path = os.path.join(path, item)
                            items.append({
                                'name': item,
                                'path': display_path,
                                'type': 'directory' if os.path.isdir(item_path) else 'file'
                            })
                    except (PermissionError, OSError):
                        continue
            except (PermissionError, OSError) as e:
                return jsonify({'error': f'无法访问目录 {full_path}: {str(e)}'}), 403

        return jsonify({
            'current_path': path,
            'items': items
        })
    except Exception as e:
        logger.error(f'list_dir 错误: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/list_remote_dir', methods=['POST'])
def list_remote_dir():
    data = request.get_json() or request.form
    host = data.get('host', '')
    username = data.get('username', 'root')
    path = data.get('path', '/')
    auth_type = data.get('auth_type', 'password')
    password = data.get('password', '')
    key_name = data.get('key', '')
    port = str(data.get('port', '22')).strip() or '22'

    if not host:
        return jsonify({'error': '请填写远程主机地址'}), 400

    try:
        ssh_cmd = ['ssh', '-o', 'StrictHostKeyChecking=no',
                   '-o', 'UserKnownHostsFile=/dev/null',
                   '-o', 'ConnectTimeout=10',
                   '-p', port]
        env = os.environ.copy()

        if auth_type == 'key' and key_name:
            key_path = _get_key_path(key_name)
            if not key_path:
                return jsonify({'error': f'SSH密钥不存在: {key_name}'}), 400
            ssh_cmd.extend(['-i', key_path])
        else:
            ssh_cmd = ['sshpass', '-e'] + ssh_cmd
            env['SSHPASS'] = password

        ssh_cmd.extend([f'{username}@{host}', f'ls -1pF "{path}"'])

        process = subprocess.Popen(
            ssh_cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            env=env
        )
        stdout, stderr = process.communicate(timeout=30)

        if process.returncode != 0:
            return jsonify({'error': f'SSH连接失败: {stderr.strip()}'}), 400

        items = []
        if path != '/':
            parent_path = str(PurePosixPath(path).parent)
            if parent_path == '/':
                parent_path = '/'
            items.append({
                'name': '..',
                'path': parent_path,
                'type': 'directory'
            })

        for line in stdout.strip().split('\n'):
            if not line:
                continue
            if line.endswith('/'):
                name = line[:-1]
                items.append({
                    'name': name,
                    'path': _posix_join(path, name),
                    'type': 'directory'
                })
            elif line.endswith('*'):
                name = line[:-1]
                items.append({
                    'name': name,
                    'path': _posix_join(path, name),
                    'type': 'file'
                })
            else:
                items.append({
                    'name': line,
                    'path': _posix_join(path, line),
                    'type': 'file'
                })

        items.sort(key=lambda x: (x['type'] != 'directory', x['name']))

        display_path = f"{username}@{host}:{path}"
        return jsonify({
            'current_path': display_path,
            'items': items
        })
    except subprocess.TimeoutExpired:
        return jsonify({'error': 'SSH连接超时'}), 408
    except Exception as e:
        logger.error(f'list_remote_dir 错误: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/tasks/<int:task_id>', methods=['PUT'])
def update_task(task_id):
    try:
        data = request.get_json()
        tasks = load_tasks()
        task = next((t for t in tasks if t['id'] == task_id), None)
        if not task:
            return jsonify({'success': False, 'error': '任务不存在'}), 404

        # 收集需要更新的字段
        updates = {}
        if 'schedule' in data:
            _unregister_scheduled_task(task_id)
            updates['schedule'] = data['schedule']
        if 'source_auth' in data:
            updates['source_auth'] = data['source_auth']
        if 'dest_auth' in data:
            updates['dest_auth'] = data['dest_auth']
        # 新增可编辑字段
        for fld in ('checksum', 'dry_run', 'bwlimit', 'include_patterns', 'exclude_patterns', 'delete_option', 'remark'):
            if fld in data:
                updates[fld] = data[fld]

        if updates:
            update_task_in_db(task_id, updates)

        # 在 DB 更新后再注册定时任务（保证闭包捕获最新字段值）
        if data.get('schedule'):
            fresh = next((t for t in load_tasks() if t['id'] == task_id), None)
            if fresh:
                _register_scheduled_task(fresh)

        return jsonify({'success': True})
    except Exception as e:
        logger.error(f'更新任务失败: {e}')
        return jsonify({'success': False, 'error': str(e)}), 500


@app.route('/keys', methods=['GET'])
def list_keys():
    """列出已保存的SSH密钥"""
    return jsonify(_list_ssh_keys())


def _validate_ssh_key_content(content):
    """校验上传的SSH密钥内容是否合法"""
    if not content or len(content) < 50:
        return False, '密钥内容过短或为空'
    if len(content) > 65536:  # 64KB 上限
        return False, '密钥文件过大（上限 64KB）'
    stripped = content.strip()
    # PEM 格式私钥
    pem_headers = ('-----BEGIN OPENSSH PRIVATE KEY-----',
                   '-----BEGIN RSA PRIVATE KEY-----',
                   '-----BEGIN EC PRIVATE KEY-----',
                   '-----BEGIN DSA PRIVATE KEY-----',
                   '-----BEGIN PRIVATE KEY-----',
                   '-----BEGIN ENCRYPTED PRIVATE KEY-----')
    for h in pem_headers:
        if h in content:
            if 'PRIVATE KEY-----' in content[content.index(h):]:
                return True, ''
            return False, '私钥格式不完整'
    # OpenSSH 公钥格式：ssh-rsa / ssh-ed25519 / ecdsa-sha2-... / ssh-dss
    pub_prefixes = ('ssh-rsa ', 'ssh-ed25519 ', 'ecdsa-sha2-', 'ssh-dss ')
    for p in pub_prefixes:
        if stripped.startswith(p) and len(stripped.split()) >= 2:
            return True, ''
    return False, '不是有效的 SSH 密钥格式（应为 PEM 私钥或 OpenSSH 公钥）'


@app.route('/keys', methods=['POST'])
def upload_key():
    """上传SSH密钥文件"""
    try:
        if 'file' not in request.files:
            return jsonify({'error': '请选择密钥文件'}), 400
        file = request.files['file']
        if not file.filename:
            return jsonify({'error': '请选择密钥文件'}), 400
        content = file.read().decode('utf-8', errors='replace')
        valid, msg = _validate_ssh_key_content(content)
        if not valid:
            return jsonify({'error': f'密钥内容无效: {msg}'}), 400
        filename = _save_ssh_key(file.filename, content)
        return jsonify({'success': True, 'filename': filename})
    except Exception as e:
        logger.error(f'上传密钥失败: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/keys/<filename>', methods=['DELETE'])
def delete_key(filename):
    """删除SSH密钥"""
    try:
        if _delete_ssh_key(filename):
            return jsonify({'success': True})
        return jsonify({'error': '密钥不存在'}), 404
    except Exception as e:
        logger.error(f'删除密钥失败: {e}')
        return jsonify({'error': str(e)}), 500


@app.route('/sync/status', methods=['GET'])
def sync_status():
    """获取当前同步状态（已完成的同步30秒后自动清理）"""
    with _running_sync_lock:
        if not _running_sync:
            return jsonify({'running': False})
        rs = dict(_running_sync)
        rs.pop('process', None)
        # 已完成/失败/停止超过30秒的不返回
        settled = rs.get('settled_time')
        if settled and time.time() - settled > 30:
            return jsonify({'running': False})
        rs['running'] = True
        rs['start_time_str'] = datetime.fromtimestamp(rs['start_time']).strftime('%Y-%m-%d %H:%M:%S')
        return jsonify(rs)


@app.route('/sync/stop', methods=['POST'])
def stop_sync():
    """停止正在运行的同步"""
    sync_key = request.form.get('sync_key', '')
    with _running_sync_lock:
        if not _running_sync:
            return jsonify({'success': False, 'error': '没有正在运行的同步任务'})
        # 如果指定了sync_key，验证是否匹配
        if sync_key and _running_sync.get('sync_key') != sync_key:
            return jsonify({'success': False, 'error': '同步任务标识不匹配'})
        process = _running_sync.get('process')
        if not process:
            return jsonify({'success': False, 'error': '无法获取进程信息'})
        sync_key_val = _running_sync.get('sync_key', '')

    logger.info(f'用户请求停止同步: {sync_key_val}')
    try:
        if os.name != 'nt':
            # Unix: 终止整个进程组
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGTERM)
            except (ProcessLookupError, OSError):
                pass
            time.sleep(2)
            try:
                os.killpg(os.getpgid(process.pid), signal.SIGKILL)
            except (ProcessLookupError, OSError):
                pass
        else:
            process.terminate()
            time.sleep(2)
            try:
                process.kill()
            except OSError:
                pass
    except Exception as e:
        logger.error(f'停止同步时出错: {e}')

    with _running_sync_lock:
        if _running_sync:
            _running_sync['status'] = 'stopped'

    logger.info(f'同步已停止: {sync_key_val}')
    return jsonify({'success': True, 'message': '同步任务已停止'})


@app.route('/hosts', methods=['GET'])
def route_list_hosts():
    return jsonify(list_hosts())


@app.route('/hosts', methods=['POST'])
def route_add_host():
    data = request.get_json()
    if not data or not data.get('name') or not data.get('host'):
        return jsonify({'error': '名称和主机地址为必填项'}), 400
    new_id = add_host(data)
    return jsonify({'success': True, 'id': new_id})


@app.route('/hosts/<int:host_id>', methods=['PUT'])
def route_update_host(host_id):
    data = request.get_json()
    if not data:
        return jsonify({'error': '无效的请求数据'}), 400
    update_host(host_id, data)
    return jsonify({'success': True})


@app.route('/hosts/<int:host_id>', methods=['DELETE'])
def route_delete_host(host_id):
    delete_host(host_id)
    return jsonify({'success': True})


@app.route('/hosts/<int:host_id>/test', methods=['POST'])
def test_host_connection(host_id):
    hosts = list_hosts()
    host = next((h for h in hosts if h['id'] == host_id), None)
    if not host:
        return jsonify({'error': '主机不存在'}), 404
    port = str(host.get('port', '22')).strip() or '22'
    username = host.get('username', 'root')
    hostname = host.get('host', '')
    auth_type = host.get('auth_type', 'password')
    password = host.get('password', '')
    key_name = host.get('key_name', '')
    key_path = _get_key_path(key_name) if auth_type == 'key' and key_name else ''
    timeout = 8
    base_opts = ['-o', 'StrictHostKeyChecking=no', '-o', 'UserKnownHostsFile=/dev/null',
                 '-o', 'ConnectTimeout=5']
    try:
        if auth_type == 'key' and key_path:
            cmd = ['ssh', '-p', port, '-i', key_path] + base_opts + ['-o', 'BatchMode=yes',
                  f'{username}@{hostname}', 'echo ok']
        else:
            cmd = ['sshpass', '-e', 'ssh', '-p', port] + base_opts + ['-o', 'PreferredAuthentications=password',
                  '-o', 'PubkeyAuthentication=no', f'{username}@{hostname}', 'echo ok']
        env = os.environ.copy()
        if auth_type == 'password' and password:
            env['SSHPASS'] = password
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, env=env)
        if proc.returncode == 0:
            return jsonify({'success': True, 'message': '连接成功', 'output': proc.stdout.strip()})
        else:
            return jsonify({'success': False, 'message': '连接失败', 'output': proc.stderr.strip()[:500]})
    except subprocess.TimeoutExpired:
        return jsonify({'success': False, 'message': '连接超时', 'output': f'连接超过 {timeout} 秒未响应'})
    except FileNotFoundError:
        return jsonify({'success': False, 'message': 'rsync-web 容器内未安装 ssh/sshpass', 'output': ''})


@app.route('/settings', methods=['GET'])
def route_get_settings():
    conn = _db()
    try:
        rows = conn.execute('SELECT key, value FROM settings').fetchall()
        return jsonify({r['key']: r['value'] for r in rows})
    finally:
        conn.close()


@app.route('/settings', methods=['POST'])
def route_set_settings():
    data = request.get_json()
    if not data:
        return jsonify({'error': '无效的请求数据'}), 400
    for key, value in data.items():
        set_setting(key, str(value))
    return jsonify({'success': True})


@app.route('/tasks/<int:task_id>/duplicate', methods=['POST'])
def duplicate_task(task_id):
    tasks = load_tasks()
    task = next((t for t in tasks if t['id'] == task_id), None)
    if not task:
        return jsonify({'error': '任务不存在'}), 404
    new_task = {
        'source': task['source'],
        'destination': task['destination'],
        'delete_option': task['delete_option'],
        'remark': task['remark'],
        'source_auth': task['source_auth'],
        'dest_auth': task['dest_auth'],
        'schedule': task['schedule'],
        'checksum': task['checksum'],
        'dry_run': task['dry_run'],
        'include_patterns': task['include_patterns'],
        'exclude_patterns': task['exclude_patterns'],
        'bwlimit': task['bwlimit'],
        'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    new_id = add_task_to_db(new_task)
    new_task['id'] = new_id
    if new_task.get('schedule'):
        _register_scheduled_task(new_task)
    return jsonify(_row_to_task({'id': new_id, 'source': new_task['source'],
                                  'destination': new_task['destination'],
                                  'delete_option': new_task['delete_option'],
                                  'remark': new_task['remark'],
                                  'source_auth': json.dumps(new_task['source_auth']),
                                  'dest_auth': json.dumps(new_task['dest_auth']),
                                  'schedule_json': json.dumps(new_task['schedule']) if new_task.get('schedule') else None,
                                  'checksum': new_task['checksum'],
                                  'dry_run': new_task['dry_run'],
                                  'include_patterns': new_task['include_patterns'],
                                  'exclude_patterns': new_task['exclude_patterns'],
                                  'bwlimit': new_task['bwlimit'],
                                  'created_at': new_task['created_at']}))


@app.route('/tasks/batch', methods=['POST'])
def batch_operations():
    data = request.get_json()
    if not data or 'action' not in data or 'task_ids' not in data:
        return jsonify({'error': '缺少 action 或 task_ids 参数'}), 400
    action = data['action']
    task_ids = data['task_ids']
    if not isinstance(task_ids, list):
        return jsonify({'error': 'task_ids 必须是数组'}), 400
    results = []
    tasks = load_tasks()
    if action == 'sync':
        for tid in task_ids:
            task = next((t for t in tasks if t['id'] == tid), None)
            if not task:
                results.append({'task_id': tid, 'success': False, 'error': '任务不存在'})
                continue
            if not _sync_execution_lock.acquire(blocking=False):
                results.append({'task_id': tid, 'success': False, 'error': '同步锁忙碌，已跳过'})
                continue
            try:
                sync_start = time.time()
                result = run_sync(task['source'], task['destination'], task.get('delete_option', True),
                                  task.get('source_auth', {}), task.get('dest_auth', {}),
                                  sync_key=f'task_{tid}', task_id=tid, trigger='manual',
                                  checksum=task.get('checksum', False),
                                  dry_run=task.get('dry_run', False),
                                  include_patterns=[p for p in (task.get('include_patterns') or '').split('\n') if p.strip()],
                                  exclude_patterns=[p for p in (task.get('exclude_patterns') or '').split('\n') if p.strip()],
                                  bwlimit=task.get('bwlimit', ''))
                duration = time.time() - sync_start
                _record_metric(result, 'manual', duration)
                save_log_entry({
                    'task_id': task['id'],
                    'source': task['source'],
                    'destination': task['destination'],
                    'success': result['success'],
                    'message': result['message'],
                    'output': result['output'][:2000],
                    'files_synced': result.get('files_synced', 0),
                    'error_count': result.get('error_count', 0),
                    'time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'trigger': 'manual'
                })
                _finalize_sync_status(f'task_{tid}', result['success'])
                send_webhook_notification(task.get('remark', '') or f'Task #{tid}',
                                          task['source'], task['destination'], result['success'],
                                          result['message'], result.get('files_synced', 0),
                                          result.get('error_count', 0), f'{duration:.1f}s')
                results.append({'task_id': tid, 'success': result['success'], 'message': result['message']})
            except Exception as e:
                results.append({'task_id': tid, 'success': False, 'error': str(e)})
            finally:
                _sync_execution_lock.release()
    elif action == 'delete':
        for tid in task_ids:
            task = next((t for t in tasks if t['id'] == tid), None)
            if not task:
                results.append({'task_id': tid, 'success': False, 'error': '任务不存在'})
                continue
            delete_task_from_db(tid)
            _unregister_scheduled_task(tid)
            results.append({'task_id': tid, 'success': True})
    else:
        return jsonify({'error': f'不支持的操作: {action}'}), 400
    return jsonify({'success': True, 'results': results})


@app.route('/tasks/check-conflicts', methods=['POST'])
def check_conflicts():
    data = request.get_json()
    if not data or 'source' not in data or 'destination' not in data:
        return jsonify({'error': '缺少 source 或 destination 参数'}), 400
    src = data['source']
    dst = data['destination']
    tasks = load_tasks()
    conflicts = []
    for t in tasks:
        if t['source'] == src or t['destination'] == dst:
            conflicts.append({'id': t['id'], 'source': t['source'], 'destination': t['destination'], 'remark': t['remark']})
    return jsonify({'conflicts': conflicts})


@app.route('/tasks/<int:task_id>/preview', methods=['POST'])
def preview_command(task_id):
    tasks = load_tasks()
    task = next((t for t in tasks if t['id'] == task_id), None)
    if not task:
        return jsonify({'error': '任务不存在'}), 404
    source = task['source']
    destination = task['destination']
    delete_option = task.get('delete_option', True)
    checksum = task.get('checksum', False)
    dry_run = task.get('dry_run', False)
    bwlimit = task.get('bwlimit', '')
    include_patterns = [p for p in (task.get('include_patterns') or '').split('\n') if p.strip()]
    exclude_patterns = [p for p in (task.get('exclude_patterns') or '').split('\n') if p.strip()]
    source_auth = task.get('source_auth', {})
    dest_auth = task.get('dest_auth', {})
    is_remote_source = ':' in source and '@' in source.split(':')[0]
    is_remote_dest = ':' in destination and '@' in destination.split(':')[0]
    if not is_remote_source:
        if not source.endswith('/'):
            source += '/'
    cmd = ['rsync', '-av', '--partial', '--ignore-errors',
           '--partial-dir=.rsync-partial',
           f'--timeout={RSYNC_IDLE_TIMEOUT}']
    if delete_option:
        cmd.append('--delete')
    if checksum:
        cmd.append('--checksum')
    if dry_run:
        cmd.append('--list-only')
    if bwlimit:
        cmd.append(f'--bwlimit={bwlimit}')
    for pat in include_patterns:
        pat = pat.strip()
        if pat:
            cmd.extend(['--include', pat])
    for pat in exclude_patterns:
        pat = pat.strip()
        if pat:
            cmd.extend(['--exclude', pat])
    ssh_cmd, env, extra_env, _cleanup_file = _build_ssh_cmd_options(
        source, destination, source_auth, dest_auth, is_remote_source, is_remote_dest)
    if is_remote_source or is_remote_dest:
        cmd.extend(['-e', ssh_cmd])
        env.update(extra_env)
    cmd.extend([source, destination])
    if _cleanup_file and os.path.exists(_cleanup_file):
        try:
            os.remove(_cleanup_file)
        except OSError:
            pass
    return jsonify({'command': ' '.join(cmd)})


# 在启动时清除所有现有的定时任务并重新加载
def init_app():
    """应用启动：初始化 SQLite、迁移旧数据、恢复定时任务、首次备份"""
    _init_db()
    _migrate_from_json()
    _backup_db()  # 启动时备份一次（用户可手动重启用）

    with _scheduled_tasks_lock:
        _scheduled_tasks.clear()
    with _schedule_lock:
        schedule.clear()
    tasks = load_tasks()
    for task in tasks:
        if task.get('schedule'):
            _register_scheduled_task(task)
    logger.info(f'应用初始化完成，已加载 {len(tasks)} 个任务')


@app.route('/metrics')
def prometheus_metrics():
    """Prometheus 指标端点（文本格式）"""
    return _format_prometheus_metrics(), 200, {'Content-Type': 'text/plain; version=0.0.4'}


@app.route('/health')
def health_check():
    """健康检查端点"""
    return jsonify({'status': 'ok', 'version': VERSION, 'uptime': int(time.time() - _metrics['start_time'])})


init_app()

if __name__ == '__main__':
    serve(app, host='0.0.0.0', port=8856, threads=8)
