from weibo import WeiboSpider
from scrapy.crawler import CrawlerProcess
from datetime import date

# 搜索参数
KEYWORDS = ["疫情"] # 关键词
LIST_PROV = [] # 省份
START_DATE = date(2020, 3, 1)
END_DATE = date(2020, 3, 2)
COOKIES = """"""

def load_keywords():
    global KEYWORDS
    KEYWORDS = []
    with open("keywords.txt", "r", encoding = "utf-8") as f:
        for line in f:
            KEYWORDS.append(line.strip())
    print("Keywords:\n", "\n".join(KEYWORDS))

def load_provinces():
    global LIST_PROV
    LIST_PROV = []
    with open("provinces.txt", "r", encoding = "utf-8") as f:
        for line in f:
            LIST_PROV.append(line.strip())
    print("Provinces:\n", "\n".join(LIST_PROV))

if __name__ == '__main__':
    load_keywords()
    load_provinces()
    process = CrawlerProcess(settings={
        'FEED_FORMAT': 'json',
        'FEED_URI': 'articles.json'
    })
    process.crawl(WeiboSpider, keywords = KEYWORDS[0], list_prov = LIST_PROV, start_date = START_DATE, end_date = END_DATE, cookies = COOKIES)
    process.start()
    input("press any key to continue...")