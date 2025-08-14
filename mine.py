import requests
from bs4 import BeautifulSoup
import pandas as pd
from telegram import Bot
from datetime import datetime
import schedule
import time
import os
from dotenv import load_dotenv

# ====== Load environment variables securely ======
load_dotenv()
TOKEN = os.getenv("TELEGRAM_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

# ====== Initialize Telegram bot ======
bot = Bot(token=TOKEN)

# ====== Keywords for platform-related jobs ======
KEYWORDS = [
    "Platform Engineer", "Cloud Engineer", "DevOps", "Site Reliability Engineer",
    "Infrastructure", "Azure", "AWS", "Kubernetes", "CI/CD", "Microservices"
]

# ====== File to track sent job links ======
SENT_JOBS_FILE = "sent_platform_jobs.csv"

# ====== Load previously sent job links ======
try:
    sent_jobs_df = pd.read_csv(SENT_JOBS_FILE)
    sent_jobs = set(sent_jobs_df['link'].tolist())
except:
    sent_jobs = set()

# ====== Scrape Indeed UAE ======
def fetch_indeed_jobs():
    url = "https://www.indeed.ae/jobs?q=platform&l=United+Arab+Emirates"
    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')

    jobs = []
    for div in soup.find_all('div', class_='job_seen_beacon'):
        title_tag = div.find('h2')
        link_tag = div.find('a', href=True)
        if title_tag and link_tag:
            title = title_tag.text.strip()
            link = "https://www.indeed.ae" + link_tag['href']
            if any(kw.lower() in title.lower() for kw in KEYWORDS) and link not in sent_jobs:
                jobs.append({"title": title, "link": link})
    return jobs

# ====== Scrape GulfTalent UAE ======
def fetch_gulf_jobs():
    url = "https://www.gulftalent.com/uae/jobs?keywords=platform"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')

    jobs = []
    for job_div in soup.find_all('div', class_='job-card'):
        title_tag = job_div.find('h2')
        link_tag = job_div.find('a', href=True)
        if title_tag and link_tag:
            title = title_tag.text.strip()
            link = "https://www.gulftalent.com" + link_tag['href']
            if any(kw.lower() in title.lower() for kw in KEYWORDS) and link not in sent_jobs:
                jobs.append({"title": title, "link": link})
    return jobs

# ====== Scrape LinkedIn UAE ======
def fetch_linkedin_jobs():
    url = "https://www.linkedin.com/jobs/search/?keywords=platform&location=United%20Arab%20Emirates"
    headers = {"User-Agent": "Mozilla/5.0"}
    res = requests.get(url, headers=headers)
    soup = BeautifulSoup(res.text, 'html.parser')

    jobs = []
    for job_div in soup.find_all('li', class_='result-card'):
        title_tag = job_div.find('h3', class_='result-card__title')
        link_tag = job_div.find('a', class_='result-card__full-card-link')
        if title_tag and link_tag:
            title = title_tag.text.strip()
            link = link_tag['href']
            if any(kw.lower() in title.lower() for kw in KEYWORDS) and link not in sent_jobs:
                jobs.append({"title": title, "link": link})
    return jobs

# ====== Scrape Bayt UAE ======
def fetch_bayt_jobs():
    url = "https://www.bayt.com/en/uae/jobs/platform/"
    res = requests.get(url)
    soup = BeautifulSoup(res.text, 'html.parser')

    jobs = []
    for job_div in soup.find_all('div', class_='jobTitle'):
        title_tag = job_div.find('a')
        if title_tag:
            title = title_tag.text.strip()
            link = "https://www.bayt.com" + title_tag['href']
            if any(kw.lower() in title.lower() for kw in KEYWORDS) and link not in sent_jobs:
                jobs.append({"title": title, "link": link})
    return jobs

# ====== Send jobs to Telegram ======
def send_telegram_message(jobs):
    if not jobs:
        print(f"[{datetime.now()}] No new platform jobs found.")
        return
    message = "*New Platform Jobs in UAE:*\n\n"
    message += "\n\n".join([f"*{job['title']}*\n{job['link']}" for job in jobs])
    bot.send_message(chat_id=CHAT_ID, text=message, parse_mode='Markdown')
    print(f"[{datetime.now()}] Sent {len(jobs)} jobs to Telegram.")

# ====== Main job function ======
def job():
    print(f"[{datetime.now()}] Checking for platform jobs...")
    all_jobs = []
    all_jobs += fetch_indeed_jobs()
    all_jobs += fetch_gulf_jobs()
    all_jobs += fetch_linkedin_jobs()
    all_jobs += fetch_bayt_jobs()

    if all_jobs:
        send_telegram_message(all_jobs)
        for job in all_jobs:
            sent_jobs.add(job['link'])
        pd.DataFrame(list(sent_jobs), columns=['link']).to_csv(SENT_JOBS_FILE, index=False)
    else:
        print(f"[{datetime.now()}] No new jobs found.")

# ====== Schedule the job every hour ======
schedule.every().hour.do(job)

print("🚀 Platform Job Scraper started...")
while True:
    schedule.run_pending()
    time.sleep(60)