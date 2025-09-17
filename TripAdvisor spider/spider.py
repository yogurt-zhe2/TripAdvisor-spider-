#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
TripAdvisor景点评论爬虫 - 增强版本
解决数据库连接、多线程冲突、完整采集等问题
"""

import requests
import json
import os
import time
import random
import urllib3
import csv
import re
import uuid
import argparse
import gc
import psutil
import pymysql
from datetime import datetime
from threading import Lock, RLock
from concurrent.futures import ThreadPoolExecutor, as_completed
import queue
import threading

# 禁用SSL警告
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ================================ 配置区 ================================
API_URL = "https://api.tripadvisor.cn/restapi/soa2/20997/getList"
OUTPUT_DIR = "attraction_comments"
PROGRESS_FILE = "progress.json"
COLLECTION_LOG_FILE = "collection_log.csv"

# MySQL数据库配置（可被环境变量覆盖）
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST', 'localhost'),
    'port': int(os.getenv('MYSQL_PORT', '3306')),
    'user': os.getenv('MYSQL_USER', ''),
    'password': os.getenv('MYSQL_PASSWORD', ''),
    'database': os.getenv('MYSQL_DATABASE', ''),
    'charset': os.getenv('MYSQL_CHARSET', 'utf8mb4'),
    'autocommit': (os.getenv('MYSQL_AUTOCOMMIT', 'true').lower() in ['1', 'true', 'yes']),
    'connect_timeout': int(os.getenv('MYSQL_CONNECT_TIMEOUT', '30')),
    'read_timeout': int(os.getenv('MYSQL_READ_TIMEOUT', '60')),
    'write_timeout': int(os.getenv('MYSQL_WRITE_TIMEOUT', '60'))
}

# 请求头（部分字段可由环境变量覆盖）
HEADERS = {
    'accept': 'application/json, text/plain, */*',
    'accept-encoding': 'gzip, deflate, br',
    'accept-language': 'zh-CN,zh;q=0.9,en;q=0.8',
    'content-type': 'application/json;charset=UTF-8',
    'origin': 'https://www.tripadvisor.cn',
    'referer': 'https://www.tripadvisor.cn/',
    'sec-ch-ua': '"Not)A;Brand";v="8", "Chromium";v="138", "Google Chrome";v="138"',
    'sec-ch-ua-mobile': '?0',
    'sec-ch-ua-platform': '"Windows"',
    'sec-fetch-dest': 'empty',
    'sec-fetch-mode': 'cors',
    'sec-fetch-site': 'same-site',
    'user-agent': os.getenv('TA_USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'),
    'x-ta-uid': os.getenv('TA_X_TA_UID', ''),
    'cookie': os.getenv('TA_COOKIE', ''),
    'dnt': '1',
    'pragma': 'no-cache',
    'cache-control': 'no-cache'
}

# 全局变量
last_request_time = 0
processed_urls = set()
success_count = 0
failed_count = 0
SKIP_DB_OPERATION = False
SELECTED_LANGS = None
THREAD_COUNT = 15  # 默认15线程

# 锁 - 使用RLock避免死锁
file_lock = RLock()
progress_lock = RLock()
log_lock = RLock()
db_lock = RLock()  # 数据库操作锁
request_lock = RLock()  # 请求频率锁

# User-Agent列表（若设置 TA_USER_AGENT，则优先加入池首位）
USER_AGENTS = [
    os.getenv('TA_USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'),
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36'
]

# ================================ 网络请求模块 ================================
def create_session():
    """创建新的session"""
    session = requests.Session()
    session.verify = False
    session.trust_env = False
    
    # 简单的连接池配置
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=1,
        pool_maxsize=1,
        max_retries=0
    )
    session.mount('https://', adapter)
    session.mount('http://', adapter)
    
    return session

def make_request_with_retry(url, json_data, max_retries=5):
    """网络请求函数 - 增强版，改进退避策略"""
    global last_request_time
    
    # 频率限制
    with request_lock:
        current_time = time.time()
        time_since_last = current_time - last_request_time
        min_interval = random.uniform(1.0, 2.0)
        if time_since_last < min_interval:
            sleep_time = min_interval - time_since_last
            time.sleep(sleep_time)
        last_request_time = time.time()
    
    # 每次创建新的session
    session = create_session()
    
    # 轮换User-Agent
    headers = dict(HEADERS)
    headers['user-agent'] = random.choice(USER_AGENTS)
    
    for attempt in range(max_retries):
        try:
            print(f"📡 发送请求 (尝试 {attempt + 1}/{max_retries})...")
            
            # 发送请求
            resp = session.post(
                url, 
                headers=headers, 
                json=json_data, 
                timeout=(15, 45)  # 增加超时时间
            )
            
            if resp.status_code == 200:
                print(f"✅ 请求成功")
                try:
                    data = resp.json()
                    return resp
                except:
                    print("⚠️ 响应不是有效JSON")
                    if attempt < max_retries - 1:
                        # 指数退避策略
                        wait_time = min(30, 2 ** attempt + random.uniform(0, 1))
                        print(f"⏳ 等待 {wait_time:.1f}秒后重试...")
                        time.sleep(wait_time)
                        continue
                    else:
                        return None
            elif resp.status_code in (429, 403):
                # 频率限制，使用指数退避
                wait_time = min(60, 5 * (2 ** attempt) + random.uniform(0, 5))
                print(f"⚠️  频率限制 ({resp.status_code})，等待 {wait_time:.1f}秒后重试 (尝试 {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                headers['user-agent'] = random.choice(USER_AGENTS)
                continue
            elif resp.status_code >= 500:
                # 服务器错误，使用指数退避
                wait_time = min(30, 3 * (2 ** attempt) + random.uniform(0, 3))
                print(f"⚠️  服务器错误 ({resp.status_code})，等待 {wait_time:.1f}秒后重试 (尝试 {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
                continue
            else:
                print(f"⚠️ 状态码: {resp.status_code}")
                if attempt < max_retries - 1:
                    wait_time = min(20, 2 ** attempt + random.uniform(0, 2))
                    print(f"⏳ 等待 {wait_time:.1f}秒后重试...")
                    time.sleep(wait_time)
                    continue
                else:
                    return None
                    
        except requests.exceptions.Timeout as e:
            print(f"⚠️ 超时: {type(e).__name__}")
            if attempt < max_retries - 1:
                # 指数退避策略
                wait_time = min(30, 3 * (2 ** attempt) + random.uniform(0, 3))
                print(f"⏳ 等待 {wait_time:.1f}秒后重试...")
                time.sleep(wait_time)
                continue
            else:
                return None
                
        except requests.exceptions.ConnectionError as e:
            print(f"⚠️ 连接错误: {e}")
            if attempt < max_retries - 1:
                # 指数退避策略
                wait_time = min(60, 5 * (2 ** attempt) + random.uniform(0, 5))
                print(f"⏳ 等待 {wait_time:.1f}秒后重试...")
                time.sleep(wait_time)
                continue
            else:
                return None
                
        except Exception as e:
            print(f"⚠️ 未知错误: {type(e).__name__} - {e}")
            if attempt < max_retries - 1:
                wait_time = min(15, 2 ** attempt + random.uniform(0, 2))
                print(f"⏳ 等待 {wait_time:.1f}秒后重试...")
                time.sleep(wait_time)
                continue
            else:
                return None
    
    return None

# ================================ 数据获取模块 ================================
def get_available_langs(location_id, url=None):
    """获取该景点可用的语言列表"""
    if url:
        print(f"\n🔍 正在获取语言列表... | URL: {url}")
    else:
        print(f"\n🔍 正在获取语言列表... | 景点ID: {location_id}")
    
    payload = {
        "frontPage": "USER_REVIEWS",
        "locationId": int(location_id),
        "selected": {"langs": []},
        "pageInfo": {"num": 1, "size": 1}
    }
    langs = []
    location_info = None
    try:
        resp = make_request_with_retry(API_URL, payload)
        if not resp:
            return langs, location_info
        data = resp.json()
        
        # 语言聚合 - 显示详细信息
        print(f"📊 语言聚合信息:")
        for agg in data.get('langAggs', []) or []:
            key = agg.get('key')
            count = agg.get('count', 0)
            print(f"  - {key}: {count}条评论")
            if key and key != 'all' and count > 0:
                langs.append(key)
        
        # 从任意details中获取locationInfo
        details = data.get('details', []) or []
        if details:
            loc = details[0].get('locationInfo', {})
            if loc:
                location_info = {
                    "attractionName": loc.get('name', '未知景点'),
                    "cityName": loc.get('cityName', '未知城市'),
                    "cityId": loc.get('cityId', 0),
                    "address": loc.get('address', '未知地址'),
                    "rating": str(loc.get('rating', 'N/A')),
                    "reviewCount": str(loc.get('reviewCount', '0'))
                }
                if url:
                    print(f"🏛️ 景点信息: {location_info['attractionName']}({location_info['cityName']}) | 评分:{location_info['rating']} | 总评论:{location_info['reviewCount']} | URL: {url}")
                else:
                    print(f"🏛️ 景点信息: {location_info['attractionName']}({location_info['cityName']}) | 评分:{location_info['rating']} | 总评论:{location_info['reviewCount']}")
    except Exception as e:
        print(f"⚠️  获取语言列表异常: {e}")
    return langs, location_info

def get_reviews_and_info(location_id, langs=None, max_pages_per_lang=10, url=None):
    """获取指定地点的评论和景点信息 - 完整采集版"""
    comments = []
    location_info = None

    # 解析语言列表
    languages_to_fetch = []
    if not langs or (isinstance(langs, list) and len(langs) == 1 and langs[0] == 'all'):
        # 从"全部评论"页面获取语言列表和景点信息
        languages_to_fetch, location_info = get_available_langs(location_id, url)
        if not languages_to_fetch:
            languages_to_fetch = ['all']
    else:
        languages_to_fetch = langs

    if url:
        print(f"🔍 获取到语言列表: {languages_to_fetch} | URL: {url}")
    else:
        print(f"🔍 获取到语言列表: {languages_to_fetch} | 景点ID: {location_id}")

    # 重要：始终从"全部评论"开始采集，确保获取所有评论
    print(f"\n🌐 开始采集全部评论 (all) | URL: {url if url else f'景点ID: {location_id}'}")
    
    page_num = 1
    total_comments = 0
    empty_pages_count = 0
    consecutive_empty_pages = 0  # 连续空页计数
    
    # 从"全部评论"页面无限制采集
    while True:
        # 使用"all"模式，不指定特定语言
        payload = {
            "frontPage": "USER_REVIEWS",
            "locationId": int(location_id),
            "selected": {
                "airlineIds": [],
                "airlineSeatIds": [],
                "langs": [],  # 空列表表示全部语言
                "ratings": [],
                "seasons": [],
                "tripTypes": [],
                "airlineLevel": []
            },
            "pageInfo": {"num": page_num, "size": 10}
        }

        try:
            response = make_request_with_retry(API_URL, payload)
            if not response:
                if url:
                    print(f"❌ 全部评论第 {page_num} 页请求失败 | URL: {url}")
                else:
                    print(f"❌ 全部评论第 {page_num} 页请求失败")
                break
                
            data = response.json()
            reviews_data = data.get('details', [])
            
            if not reviews_data:
                empty_pages_count += 1
                consecutive_empty_pages += 1
                if url:
                    print(f"📄 全部评论第 {page_num} 页无数据 (连续{consecutive_empty_pages}页) | URL: {url}")
                else:
                    print(f"📄 全部评论第 {page_num} 页无数据 (连续{consecutive_empty_pages}页)")
                
                # 连续5页无数据或总空页超过10页则停止
                if consecutive_empty_pages >= 5 or empty_pages_count >= 10:
                    if url:
                        print(f"🛑 全部评论连续{consecutive_empty_pages}页无数据，停止采集 | URL: {url}")
                    else:
                        print(f"🛑 全部评论连续{consecutive_empty_pages}页无数据，停止采集")
                    break
                
                page_num += 1
                time.sleep(random.uniform(1, 2))
                continue

            # 初始化location_info
            if not location_info:
                first = reviews_data[0]
                loc_info = first.get('locationInfo') if isinstance(first, dict) else None
                if loc_info:
                    location_info = {
                        "attractionName": loc_info.get('name', '未知景点'),
                        "cityName": loc_info.get('cityName', '未知城市'),
                        "cityId": loc_info.get('cityId', 0),
                        "address": loc_info.get('address', '未知地址'),
                        "rating": str(loc_info.get('rating', 'N/A')),
                        "reviewCount": str(loc_info.get('reviewCount', '0'))
                    }

            # 解析评论
            page_comments = []
            for review in reviews_data:
                try:
                    member_info = review.get('memberInfo', {}) if isinstance(review, dict) else {}
                    page_comments.append({
                        "userReviewId": str(review.get("userReviewId", "")),
                        "username": member_info.get("displayName") or member_info.get("username", "Tripadvisor用户"),
                        "userRating": review.get("rating", 0),
                        "title": review.get("title", ""),
                        "tripTypeString": review.get("tripTypeString", ""),
                        "content": review.get("content", ""),
                        "lang": review.get("lang", ""),
                        "submitTime": review.get("submitTime", ""),
                        "attribution": review.get("attribution", "")
                    })
                except Exception as e:
                    print(f"⚠️  评论解析错误: {e}")
                    continue

            comments.extend(page_comments)
            total_comments += len(page_comments)
            consecutive_empty_pages = 0  # 重置连续空页计数
            if url:
                print(f"📄 全部评论第 {page_num} 页: {len(page_comments)}条评论 | URL: {url}")
            else:
                print(f"📄 全部评论第 {page_num} 页: {len(page_comments)}条评论")
            
            page_num += 1
            time.sleep(random.uniform(1, 2))
            
        except Exception as e:
            if url:
                print(f"❌ 全部评论第 {page_num} 页处理失败: {e} | URL: {url}")
            else:
                print(f"❌ 全部评论第 {page_num} 页处理失败: {e}")
            break
    
    # 显示采集结果
    if total_comments > 0:
        if url:
            print(f"  ✅ 全部评论采集完成: {total_comments}条评论 | URL: {url}")
        else:
            print(f"  ✅ 全部评论采集完成: {total_comments}条评论 | 景点ID: {location_id}")

    return comments, location_info

# ================================ 数据库模块 - 增强版 ================================
def get_db_connection_with_retry(max_retries=5):
    """获取数据库连接 - 带重试机制"""
    for attempt in range(max_retries):
        try:
            print(f"🔗 连接数据库 (尝试 {attempt + 1}/{max_retries})...")
            connection = pymysql.connect(**MYSQL_CONFIG)
            connection.autocommit(True)
            
            # 测试连接
            with connection.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                if result and result[0] == 1:
                    print("✅ 数据库连接成功")
                    return connection
                else:
                    raise Exception("连接测试失败")
                    
        except Exception as e:
            print(f"❌ 数据库连接失败 (尝试 {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                # 指数退避策略
                wait_time = min(60, 5 * (2 ** attempt) + random.uniform(0, 5))
                print(f"⏳ {wait_time}秒后重试...")
                time.sleep(wait_time)
            else:
                print("❌ 数据库连接最终失败")
                return None
    
    return None

def test_db_connection():
    """测试数据库连接"""
    connection = get_db_connection_with_retry()
    if connection:
        try:
            connection.close()
            return True
        except:
            pass
    return False

def execute_db_operation_with_retry(operation_func, *args, **kwargs):
    """执行数据库操作的通用函数，每次重新连接，带重试"""
    max_retries = 5
    for attempt in range(max_retries):
        connection = None
        try:
            connection = get_db_connection_with_retry()
            if not connection:
                if attempt < max_retries - 1:
                    wait_time = min(30, 3 * (2 ** attempt))
                    print(f"🔄 数据库连接失败，{wait_time}秒后重试...")
                    time.sleep(wait_time)
                    continue
                else:
                    return False
            
            result = operation_func(connection, *args, **kwargs)
            return result
            
        except (pymysql.Error, Exception) as e:
            print(f"⚠️  数据库操作失败 (尝试 {attempt + 1}/{max_retries}): {e}")
            
            if attempt < max_retries - 1:
                wait_time = min(30, 3 * (2 ** attempt))
                print(f"⏳ {wait_time}秒后重试...")
                time.sleep(wait_time)
            else:
                print("❌ 数据库操作最终失败")
                return False
        finally:
            # 确保连接关闭
            if connection:
                try:
                    connection.close()
                except:
                    pass
    
    return False

def _insert_record_operation(connection, url, comment_count, json_filename, location_info):
    """实际的插入记录操作"""
    current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    relative_path = f"./{OUTPUT_DIR}/{json_filename}"
    
    # 准备插入数据 - 确保字段完整
    insert_data = {
        "采集网站": "TripAdvisor",
        "url": url,
        "采集人": os.getenv('COLLECTOR_NAME', ''),
        "采集时间": current_time,
        "评论数": str(comment_count),
        "存储地址": relative_path
    }
    
    # 构建SQL插入语句
    columns = list(insert_data.keys())
    values = list(insert_data.values())
    placeholders = ', '.join(['%s'] * len(columns))
    column_names = ', '.join([f'`{col}`' for col in columns])
    
    table_name = os.getenv('MYSQL_TABLE', 'collection_table')
    sql = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
    
    with connection.cursor() as cursor:
        cursor.execute(sql, values)
    
    print(f"💾 数据库记录已插入: {location_info.get('attractionName', '未知景点')}")
    return True

def insert_collection_record_to_db(url, comment_count, json_filename, location_info):
    """将采集记录插入数据库 - 带重试"""
    return execute_db_operation_with_retry(_insert_record_operation, url, comment_count, json_filename, location_info)

def _delete_record_operation(connection, url, location_info):
    """实际的删除记录操作"""
    table_name = os.getenv('MYSQL_TABLE', 'collection_table')
    sql = f"DELETE FROM {table_name} WHERE url = %s"
    
    with connection.cursor() as cursor:
        cursor.execute(sql, (url,))
    
    print(f"🔄 数据库记录已回滚: {location_info.get('attractionName', '未知景点')}")
    return True

def delete_collection_record_from_db(url, location_info):
    """从数据库删除采集记录（用于回滚）- 带重试"""
    return execute_db_operation_with_retry(_delete_record_operation, url, location_info)

# ================================ 进度管理模块 ================================
def save_progress():
    """保存处理进度"""
    progress_data = {
        "processed_urls": list(processed_urls),
        "success_count": success_count,
        "failed_count": failed_count,
        "timestamp": time.time()
    }
    
    try:
        with file_lock:
            with open(PROGRESS_FILE, 'w', encoding='utf-8') as f:
                json.dump(progress_data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        print(f"⚠️  保存进度失败: {e}")

def load_progress():
    """加载处理进度"""
    global processed_urls, success_count, failed_count
    
    if not os.path.exists(PROGRESS_FILE):
        return
    
    try:
        with file_lock:
            with open(PROGRESS_FILE, 'r', encoding='utf-8') as f:
                progress_data = json.load(f)
                
        processed_urls = set(progress_data.get("processed_urls", []))
        success_count = progress_data.get("success_count", 0)
        failed_count = progress_data.get("failed_count", 0)
        
        print(f"📁 已加载进度: 已处理 {len(processed_urls)} 个URL, 成功 {success_count}, 失败 {failed_count}")
        
    except Exception as e:
        print(f"⚠️  加载进度失败: {e}")

# ================================ 采集记录模块 ================================
def init_collection_log():
    """初始化采集记录CSV文件"""
    if not os.path.exists(COLLECTION_LOG_FILE):
        with log_lock:
            with open(COLLECTION_LOG_FILE, 'w', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow([
                    "采集网站", "url", "采集人", "采集时间", "评论数", "存储地址"
                ])
        print(f"📝 已创建采集记录文件: {COLLECTION_LOG_FILE}")

def log_collection_record(url, comment_count, json_filename, location_info):
    """记录采集信息到CSV - 线程安全"""
    with log_lock:
        try:
            current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            relative_path = f"./{OUTPUT_DIR}/{json_filename}"
            
            record = [
                "TripAdvisor",
                url,
                os.getenv('COLLECTOR_NAME', ''),
                current_time,
                str(comment_count),
                relative_path
            ]
            
            with open(COLLECTION_LOG_FILE, 'a', newline='', encoding='utf-8-sig') as f:
                writer = csv.writer(f)
                writer.writerow(record)
                
        except Exception as e:
            print(f"⚠️  记录采集信息失败: {e}")

# ================================ 系统监控模块 ================================
def get_memory_usage():
    """获取当前内存使用情况"""
    try:
        process = psutil.Process()
        memory_info = process.memory_info()
        return memory_info.rss / 1024 / 1024  # MB
    except:
        return 0

def log_memory_usage():
    """记录内存使用情况"""
    memory_mb = get_memory_usage()
    if memory_mb > 500:  # 超过500MB时警告
        print(f"⚠️  内存使用: {memory_mb:.1f}MB")

# ================================ 核心处理模块 ================================
def extract_ids_from_url(url):
    """从URL中提取城市ID和地点ID"""
    match = re.search(r'-g(\d+)-d(\d+)-', url)
    if match:
        return match.groups()  # (city_id, location_id)
    return None, None

def generate_safe_filename(attraction_name, location_id):
    """生成安全的文件名 - 与主程序保持一致"""
    # 移除非法字符
    safe_name = re.sub(r'[\\/*?:"<>|\x00-\x1f\x7f-\x9f]', "", attraction_name)
    safe_name = safe_name.strip()[:100]  # 与主程序保持一致的长度限制
    
    if not safe_name:
        safe_name = f"attraction_{location_id}"
    
    # 与主程序保持一致的命名格式：景点名_UUID前8位.json
    random_uuid = uuid.uuid4()
    filename = f"{safe_name}_{random_uuid.hex[:8]}.json"
    
    return filename

def process_single_attraction(url):
    """处理单个景点的数据采集 - 线程安全版"""
    global success_count, failed_count
    
    try:
        # 检查是否已处理
        with progress_lock:
            if url in processed_urls:
                print(f"⏭️  跳过已处理的URL: {url}")
                return True
        
        print(f"\n{'='*80}")
        print(f"🎯 开始处理景点: {url}")
        print(f"{'='*80}")
        
        # 提取ID
        city_id, location_id = extract_ids_from_url(url)
        if not location_id:
            print(f"❌ 无法从URL提取ID: {url}")
            with progress_lock:
                processed_urls.add(url)
                failed_count += 1
            return False
        
        print(f"📍 提取到ID: {location_id} | URL: {url}")
        
        # 获取评论和景点信息
        print(f"🔍 正在获取景点信息... | URL: {url}")
        comments, location_info = get_reviews_and_info(location_id, langs=SELECTED_LANGS, url=url)
        
        # 如果没有获取到景点信息，使用默认值
        if not location_info:
            location_info = {
                "attractionName": f"景点_{location_id}",
                "cityName": "未知城市",
                "cityId": int(city_id) if city_id else 0,
                "address": "未知地址",
                "rating": "N/A",
                "reviewCount": str(len(comments))
            }
            print(f"⚠️  使用默认景点信息 | URL: {url}")
        
        print(f"🏛️ 景点信息: {location_info['attractionName']}({location_info['cityName']}) | 总评论:{len(comments)}条 | URL: {url}")
        
        # 整合数据
        final_data = {
            "url": url,
            "cityName": location_info['cityName'],
            "cityId": int(city_id) if city_id else location_info['cityId'],
            "attractionName": location_info['attractionName'],
            "address": location_info['address'],
            "reviewCount": location_info['reviewCount'],
            "rating": location_info['rating'],
            "comments": comments,
            "采集时间": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "采集人": os.getenv('COLLECTOR_NAME', '')
        }
        
        # 保存文件 - 使用景点名称+UUID命名，与主程序保持一致
        attraction_name = location_info['attractionName']
        # 更严格的文件名清理，确保安全
        safe_filename = re.sub(r'[\\/*?:"<>|\x00-\x1f\x7f-\x9f]', "", attraction_name)
        safe_filename = safe_filename.strip()[:100]  # 限制长度
        if not safe_filename:
            safe_filename = f"attraction_{location_id}"
        random_uuid = uuid.uuid4()
        filename = f"{safe_filename}_{random_uuid.hex[:8]}.json"
        filepath = os.path.join(OUTPUT_DIR, filename)
        
        # 先尝试插入数据库（数据库是权威）
        print(f"💾 正在保存数据... | 景点: {location_info['attractionName']} | URL: {url}")
        if SKIP_DB_OPERATION:
            db_success = True
            print(f"⚠️  跳过数据库操作 | URL: {url}")
        else:
            db_success = insert_collection_record_to_db(url, len(comments), filename, location_info)
        
        if db_success:
            # 数据库插入成功，再保存JSON文件
            save_success = False
            for save_attempt in range(3):
                try:
                    with file_lock:
                        with open(filepath, 'w', encoding='utf-8') as f:
                            json.dump(final_data, f, ensure_ascii=False, indent=4)
                    print(f"💾 JSON文件保存成功: {filename} | 景点: {location_info['attractionName']} | URL: {url}")
                    save_success = True
                    break
                except Exception as e:
                    if save_attempt < 2:
                        print(f"⚠️  JSON保存失败，重试 ({save_attempt + 1}/3): {e} | URL: {url}")
                        time.sleep(1)
                    else:
                        print(f"❌ JSON保存最终失败: {e} | URL: {url}")
            
            if save_success:
                # JSON保存成功，再写入CSV
                log_collection_record(url, len(comments), filename, location_info)
                with progress_lock:
                    success_count += 1
                print(f"✅ 完整保存成功: 数据库 + JSON + CSV | 景点: {location_info['attractionName']} | URL: {url}")
            else:
                # JSON保存失败，需要回滚数据库
                if not SKIP_DB_OPERATION:
                    print(f"❌ JSON保存失败，正在回滚数据库记录... | URL: {url}")
                    rollback_success = delete_collection_record_from_db(url, location_info)
                    if rollback_success:
                        print(f"✅ 数据库回滚成功 | URL: {url}")
                    else:
                        print(f"⚠️  数据库回滚失败，需要手动处理 | URL: {url}")
                with progress_lock:
                    failed_count += 1
        else:
            # 数据库插入失败，不保存JSON和CSV
            print(f"❌ 数据库插入失败，跳过JSON和CSV保存 | URL: {url}")
            with progress_lock:
                failed_count += 1
        
        # 清理内存
        del final_data
        del comments
        gc.collect()
        
        # 更新进度
        with progress_lock:
            processed_urls.add(url)
        
        # 处理完成后间隔
        # 覆盖率分析
        if location_info and location_info['reviewCount'] != '0':
            total_reviews = int(location_info['reviewCount'])
            collected_reviews = len(comments)
            coverage = (collected_reviews / total_reviews) * 100
            print(f"\n📊 采集覆盖率分析 | URL: {url}")
            print(f"  📝 网站显示总评论数: {total_reviews}")
            print(f"  📄 实际采集评论数: {collected_reviews}")
            print(f"  📈 采集覆盖率: {coverage:.1f}%")
            
            if coverage < 90:
                print(f"  ⚠️ 覆盖率较低，可能存在遗漏 | URL: {url}")
            else:
                print(f"  ✅ 覆盖率良好 | URL: {url}")
        
        print(f"🎉 景点处理完成: {location_info['attractionName']} | URL: {url}")
        time.sleep(random.uniform(2, 4))
        return True
                
    except Exception as e:
        print(f"❌ 处理景点失败 ({url}): {e}")
        # 即使失败也要记录进度，避免重复处理
        with progress_lock:
            processed_urls.add(url)
            failed_count += 1
        # 强制垃圾回收
        gc.collect()
        # 短暂等待后继续
        time.sleep(random.uniform(1, 3))
        return False

# ================================ 主程序 ================================
def read_urls_from_csv(csv_path):
    """从CSV文件读取URL列表"""
    urls = []
    if not os.path.exists(csv_path):
        print(f"❌ CSV文件不存在: {csv_path}")
        return urls
        
    with open(csv_path, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get('url'):
                urls.append(row['url'].strip())
    
    print(f"📖 从CSV读取到 {len(urls)} 条URL")
    return urls

def create_sample_csv():
    """创建示例CSV文件"""
    sample_urls = [
        "https://www.tripadvisor.cn/Attraction_Review-g297701-d3683097-Reviews-Bali_Private_Tour_Id-Ubud_Gianyar_Regency_Bali.html",
        "https://www.tripadvisor.cn/Attraction_Review-g15880600-d20087279-Reviews-Heavenly_Spa_By_Westin_Ubud-Singakerta_Ubud_Gianyar_Regency_Bali.html",
        "https://www.tripadvisor.cn/Attraction_Review-g3961414-d16858110-Reviews-Bali_Experience_Adventure_Tour-Singapadu_Sukawati_Gianyar_Regency_Bali.html"
    ]
    
    with open('attraction_urls.csv', 'w', newline='', encoding='utf-8-sig') as f:
        writer = csv.writer(f)
        writer.writerow(["url"])
        for url in sample_urls:
            writer.writerow([url])
    
    print(f"📝 已创建示例CSV文件: attraction_urls.csv ({len(sample_urls)}条URL)")

def main():
    """主程序入口 - 增强版"""
    global success_count, failed_count, processed_urls, SELECTED_LANGS, SKIP_DB_OPERATION, THREAD_COUNT
    
    # 合规与使用限制提示横幅
    print("\n" + "="*80)
    print("⚖️  合规与使用限制提醒")
    print("- 本项目仅供学习与科研用途，严禁任何形式的商业使用。")
    print("- 请遵守 Tripadvisor 服务条款、robots.txt 与访问频率限制，不得绕过反爬。")
    print("- 使用本代码需遵守所在地与数据来源地法律法规，风险自担。")
    print("详见 README.md 与 DISCLAIMER.md。")
    print("="*80 + "\n")

    parser = argparse.ArgumentParser(description='TripAdvisor景点评论爬虫 - 增强版本')
    parser.add_argument('--csv', default='attraction_urls.csv', help='URL CSV文件路径')
    parser.add_argument('--threads', type=int, default=3, help='并发线程数，默认3')
    parser.add_argument('--test', action='store_true', help='测试模式：只处理前3个URL')
    parser.add_argument('--create-sample', action='store_true', help='创建示例CSV文件')
    parser.add_argument('--reset-progress', action='store_true', help='重置进度，从头开始')
    parser.add_argument('--show-progress', action='store_true', help='显示当前进度并退出')
    parser.add_argument('--langs', default='all', help='语言列表，例如 zhCN,en,fr；默认 all 表示全部语言')
    parser.add_argument('--limit', type=int, default=None, help='仅处理前N个URL，用于测试')
    parser.add_argument('--no-db', action='store_true', help='跳过数据库操作，仅保存JSON和CSV')
    args = parser.parse_args()

    # 设置线程数
    THREAD_COUNT = args.threads

    # 语言设置
    if args.langs.strip().lower() == 'all':
        SELECTED_LANGS = ['all']
        print('🌐 语言设置: 全部语言')
    else:
        SELECTED_LANGS = [x.strip() for x in args.langs.split(',') if x.strip()]
        print(f"🌐 语言设置: {SELECTED_LANGS}")

    # 显示进度
    if args.show_progress:
        if os.path.exists(PROGRESS_FILE):
            load_progress()
            print(f"📊 当前进度:")
            print(f"   - 已处理: {len(processed_urls)} 个URL")
            print(f"   - 成功: {success_count}")
            print(f"   - 失败: {failed_count}")
        else:
            print("📊 尚未开始处理")
        return

    # 重置进度
    if args.reset_progress:
        if os.path.exists(PROGRESS_FILE):
            os.remove(PROGRESS_FILE)
            print("🔄 进度已重置")
        return

    # 创建示例文件
    if args.create_sample:
        create_sample_csv()
        return

    # 创建输出目录
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    
    # 数据库设置
    if args.no_db:
        print("⚠️  跳过数据库操作模式")
        SKIP_DB_OPERATION = True
    else:
        print("🔗 测试数据库连接...")
        if not test_db_connection():
            print("❌ 数据库连接失败，程序退出")
            return
        SKIP_DB_OPERATION = False
    
    # 初始化采集记录文件
    init_collection_log()

    # 加载进度
    load_progress()

    # 读取URL
    all_urls = read_urls_from_csv(args.csv)
    if not all_urls:
        print("❌ CSV中没有URL，可以使用 --create-sample 创建示例文件")
        return

    # 过滤已处理的URL
    pending_urls = [url for url in all_urls if url not in processed_urls]
 
    if not pending_urls:
        print("✅ 所有URL都已处理完成！")
        print(f"📊 最终统计: 成功 {success_count}, 失败 {failed_count}")
        return

    # 测试模式
    if args.test:
        pending_urls = pending_urls[:3]
        print(f"🧪 测试模式：处理前 {len(pending_urls)} 个未处理的URL")

    # 限制数量
    if args.limit is not None and args.limit > 0:
        pending_urls = pending_urls[:args.limit]
        print(f"🔬 本次仅处理前 {len(pending_urls)} 个URL")

    print(f"📋 总URL数: {len(all_urls)}")
    print(f"✅ 已处理: {len(processed_urls)}")
    print(f"⏳ 待处理: {len(pending_urls)}")
    print(f"🔧 多线程模式: {THREAD_COUNT} 线程")

    if len(pending_urls) == 0:
        print("🎉 没有待处理的URL！")
        return

    # 多线程处理
    start_time = time.time()
    
    try:
        with ThreadPoolExecutor(max_workers=THREAD_COUNT) as executor:
            futures = [executor.submit(process_single_attraction, url) for url in pending_urls]
            
            for i, future in enumerate(as_completed(futures)):
                try:
                    future.result()
                except Exception as e:
                    print(f"❌ 任务失败: {e}")
                
                # 每处理5个保存一次进度
                if (i + 1) % 5 == 0:
                    save_progress()
                    print(f"📈 批量进度: {i+1}/{len(pending_urls)} ({(i+1)/len(pending_urls)*100:.1f}%)")
                    log_memory_usage()

    except KeyboardInterrupt:
        print("\n⚠️  用户中断程序，正在保存进度...")
        save_progress()
        print("💾 进度已保存，下次运行将从中断处继续")
        return

    # 最终保存进度
    save_progress()

    # 统计结果
    duration = int(time.time() - start_time)
    total_processed = len(processed_urls)
    
    print(f"\n🎉 本轮任务完成！")
    print(f"📊 本轮处理: {len(pending_urls)} 个景点")
    print(f"📊 总处理数: {total_processed}/{len(all_urls)} ({(total_processed/len(all_urls)*100):.1f}%)")
    print(f"✅ 累计成功: {success_count}")
    print(f"❌ 累计失败: {failed_count}")
    print(f"⏱️  本轮耗时: {duration//60:02d}:{duration%60:02d}")
    print(f"📁 文件位置: {OUTPUT_DIR}/")
    
    # 如果还有未完成的，提示用户
    if total_processed < len(all_urls):
        remaining = len(all_urls) - total_processed
        print(f"\n💡 还有 {remaining} 个URL待处理，可再次运行程序继续")
    else:
        print(f"\n🏆 所有 {len(all_urls)} 个景点都已处理完成！")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⏹️  用户停止程序")
    except Exception as e:
        print(f"❌ 程序出错: {e}")
        import traceback
        traceback.print_exc() 