import feedparser
from datetime import datetime, date, timedelta
import pandas as pd
import time
import json
import re
from openai import OpenAI
from tqdm import tqdm
from xml.etree.ElementTree import Element, SubElement, ElementTree
from email.utils import format_datetime
from pathlib import Path
import subprocess

# ========== 配置区域 ==========

# RSS 源地址
RSS_FEEDS = {
    "ChainFeeds": "https://www.chainfeeds.xyz/rss",
    "PANews_推荐": "https://www.panewslab.com/zh/rss/foryou.xml",
    "BlockBeats": "https://api.theblockbeats.news/v2/rss/all",
    "PANews_快讯": "https://www.panewslab.com/zh/rss/newsflash.xml"
}

# DeepSeek API Key
API_KEY = "sk-23abfd1963d4433eaa8a5b3b0b2ba22a"  # <-- 替换为你自己的 API Key

HISTORY_FILE = r"/Users/kay/Desktop/rss/rss_history.csv"
XML_OUTPUT   = r"/Users/kay/Desktop/rss/filtered_feed.xml"
HISTORY_DAYS = 30

# ========== 工具函数 ==========
def git_push_rss_file(file_path, commit_message="update RSS XML"):
    try:
        subprocess.run(["git", "add", file_path], check=True)
        subprocess.run(["git", "commit", "-m", commit_message], check=True)
        subprocess.run(["git", "push"], check=True)
        print("🚀 已自动推送 RSS 到 GitHub")
    except subprocess.CalledProcessError as e:
        print(f"❌ Git 操作失败：{e}")

def is_yesterday(struct_time_obj):
    if not struct_time_obj:
        return False
    published_date = datetime(*struct_time_obj[:6]).date()
    return published_date == (date.today() - timedelta(days=1))

def parse_rss_feed(name, url):
    feed = feedparser.parse(url)
    entries = []
    for entry in feed.entries:
        published_parsed = entry.get("published_parsed") or entry.get("updated_parsed")
        if is_yesterday(published_parsed):
            published_str = time.strftime("%Y-%m-%d", published_parsed)
            entries.append({
                "source": name,
                "title": entry.title,
                "link": entry.link,
                "summary": entry.summary,
                "time": published_str
            })
    return entries

def fetch_all_rss():
    all_entries = []
    for name, url in RSS_FEEDS.items():
        try:
            entries = parse_rss_feed(name, url)
            all_entries.extend(entries)
        except Exception as e:
            print(f"❌ Failed to fetch {name}: {e}")
    return pd.DataFrame(all_entries)

def filter_by_keywords(df):
    black_keywords = [
        "比特币价格", "以太坊价格", "行情", "跌破", "爆仓", "Binance Alpha", "币安 Alpha", "一周预告"
    ]

    def is_useful(entry):
        text = f"{entry['title']} {entry['summary']}"
        return not any(bad in text for bad in black_keywords)
    return df[df.apply(is_useful, axis=1)]

# ========== DeepSeek 分类逻辑 ==========

def build_batch_prompt(news_batch):
    prompt = """
你是一名专业的区块链研究员。下面是多条区块链相关新闻，请你逐条判断：

对于每条新闻，请判断以下三项内容，并返回 JSON 数组：
1. is_valuable（是否有价值）：True 或 False
2. category（若有价值，请在以下四个类别中选择一个）：
   - 交互机会
   - 投资参考
   - 风险预警
   - 行业趋势 / 热点叙事
3. suggest_action（若有价值，你建议的下一步行动）

请严格返回如下格式的 JSON 数组，每条结果按序对应：
[
  {
    "is_valuable": true,
    "category": "...",
    "suggest_action": "..."
  },
  ...
]

以下是新闻列表：
"""
    for idx, row in enumerate(news_batch, 1):
        prompt += f"\n[{idx}] 标题：{row['title']}\n内容：{row['summary']}\n"

    return prompt

def classify_batch(entries, batch_size=10):
    all_results = []

    for i in tqdm(range(0, len(entries), batch_size)):
        batch = entries[i:i+batch_size]
        prompt = build_batch_prompt(batch)

        try:
            response = client.chat.completions.create(
                model="deepseek-chat",
                messages=[{"role": "user", "content": prompt}],
                temperature=0.2
            )
            raw_text = response.choices[0].message.content

            # 提取 JSON（可加入错误容错处理）
            match = re.search(r"\[.*\]", raw_text, re.DOTALL)
            json_text = match.group(0) if match else raw_text
            result_batch = json.loads(json_text)

            if len(result_batch) != len(batch):
                raise ValueError("返回结果数量与请求数量不一致")

            all_results.extend(result_batch)

        except Exception as e:
            print(f"❌ 批次 {i} 出错：{e}")
            for _ in batch:
                all_results.append({
                    "is_valuable": False,
                    "category": None,
                    "suggest_action": f"Error: {e}"
                })

        time.sleep(2)  # 控制频率

    return all_results

def generate_rss_xml(df, outfile):
    rss = Element("rss", version="2.0")
    channel = SubElement(rss, "channel")

    SubElement(channel, "title").text       = "DeepSeek Alpha Feed"
    SubElement(channel, "link").text        = "https://zzzzzioi.github.io/rss_alpha/filtered_feed.xml"
    SubElement(channel, "description").text = "近 30 天高价值区块链资讯"
    SubElement(channel, "lastBuildDate").text = format_datetime(datetime.now())

    for _, row in df.iterrows():
        item = SubElement(channel, "item")
        SubElement(item, "title").text       = row["title"]
        SubElement(item, "link").text        = row["link"]
        SubElement(item, "description").text = f"类别：{row['category']} | 建议：{row['suggest_action']}"
        SubElement(item, "summary").text     = row['summary']
        SubElement(item, "pubDate").text     = format_datetime(pd.to_datetime(row["time"]))
        SubElement(item, "source").text      = row["source"]

    ElementTree(rss).write(outfile, encoding="utf-8", xml_declaration=True)
    print(f"✅ RSS XML 已更新：{outfile}")
# ========== 主执行入口 ==========

if __name__ == "__main__":
    print("🔍 开始抓取昨日 RSS...")
    df_rss = fetch_all_rss()

    print("🧹 开始关键词初筛...")
    df_filtered = filter_by_keywords(df_rss)
    print(f"✅ 初筛后保留 {len(df_filtered)} 条")

    print("🤖 正在调用 DeepSeek 进一步筛选分类...")

    client = OpenAI(api_key=API_KEY, base_url="https://api.deepseek.com")

    results = classify_batch(df_filtered.to_dict("records"), batch_size=10)

    df_filtered.loc[:, "is_valuable"] = [r["is_valuable"] for r in results]
    df_filtered.loc[:, "category"] = [r["category"] for r in results]
    df_filtered.loc[:, "suggest_action"] = [r["suggest_action"] for r in results]

    valuable_df = df_filtered[df_filtered["is_valuable"] == True].copy()
    valuable_df.loc[:, "time"] = pd.to_datetime(valuable_df["time"])

    if not valuable_df.empty:
        if Path(HISTORY_FILE).exists():
            df_history = pd.read_csv(HISTORY_FILE, parse_dates=["time"])
        else:
            df_history = pd.DataFrame(columns=valuable_df.columns)

        df_all = pd.concat([df_history, valuable_df], ignore_index=True)
        df_all = df_all.drop_duplicates(subset=["title", "link"])

        cutoff = datetime.now() - timedelta(days=HISTORY_DAYS)
        df_all = df_all[df_all["time"] >= cutoff]

        df_all.to_csv(HISTORY_FILE, index=False)
        generate_rss_xml(df_all, XML_OUTPUT)       # 更新 filtered_feed.xml
    else:
        print("❕ 本次没有新的有价值信息，历史 RSS 保持不变")
