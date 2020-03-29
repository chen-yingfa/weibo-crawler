from weibo import WeiboSpider
from scrapy.crawler import CrawlerProcess
from datetime import date

#### 搜索参数 ####
START_DATE = date(2020, 3, 1) # 开始日期
END_DATE = date(2020, 3, 2)   # 终止日期
COOKIES = []

""" 
    SINAGLOBAL=6332722957414.718.1582733375156; UOR=,,login.sina.com.cn; webim_unReadCount=%7B%22time%22%3A1585399696022%2C%22dm_pub_total%22%3A0%2C%22chat_group_client%22%3A0%2C%22allcountNum%22%3A52%2C%22msgbox%22%3A0%7D; Ugrow-G0=9ec894e3c5cc0435786b4ee8ec8a55cc; YF-V5-G0=260e732907e3bd813efaef67866e5183; _s_tentry=login.sina.com.cn; Apache=3626552766952.84.1585451872721; YF-Page-G0=d48792e720239493e4bd7a097d1a7d4d|1585451875|1585451875; wb_view_log_6649431249=1920*10801; ULV=1585451872776:7:5:1:3626552766952.84.1585451872721:1585371048430; login_sid_t=d9d39995e5197e3f330cdd5504d3bafe; cross_origin_proto=SSL; WBStorage=42212210b087ca50|undefined; wb_view_log=1920*10801; WBtopGlobal_register_version=3d5b6de7399dfbdb; SCF=AvrCXQfTl-oC4aeQpjOBOpJieiuP8EXGN2x7JNBQF1af4ROQSrHaD1eTox3raOq52JIlH9_JNbGBmw_3tSIkzyg.; SUB=_2A25zhGPfDeRhGeNP41AQ-SvPyDiIHXVQ8NIXrDV8PUJbmtAfLUGskW9NTnWgYjRRykYtXTYXOyySYfypuvKI6luB; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WhNUxk6X0sM.uXvAOcJX8CB5JpX5K2hUgL.Fo-p1hzp1K-0e0B2dJLoIEzLxK.L1-zLBoMLxK-L1h-L1hnLxKML1KBL1-9Fdc43dNHE; SUHB=05i6oOHEA-JvUJ; SSOLoginState=1585451924; un=13021237150
"""

# 以下：关键词和省份从keywords.txt和provinces.txt读入
KEYWORDS = ["疫情"] # 关键词
LIST_PROV = [] # 省份
KEYWORDS_FILENAME = "../keywords.txt"
PROV_FILENAME = "../provinces.txt"
COOKIES_FILENAME = "../cookies.txt"

#### 搜索参数终 ####

def load_keywords():
    """
    载入关键词
    """
    global KEYWORDS
    KEYWORDS = []
    with open(KEYWORDS_FILENAME, "r", encoding = "utf-8") as f:
        for line in f:
            KEYWORDS.append(line.strip())
    print("Keywords:\n", "\n".join(KEYWORDS))

def load_provinces():
    """
    载入省份
    """
    global LIST_PROV
    LIST_PROV = []
    with open(PROV_FILENAME, "r", encoding = "utf-8") as f:
        for line in f:
            LIST_PROV.append(line.strip())
    print("Provinces:\n", "\n".join(LIST_PROV))

def load_cookies():
    global COOKIES
    COOKIES = ""
    with open(COOKIES_FILENAME, "r", encoding = "utf-8") as f:
        for line in f:
            COOKIES += line

if __name__ == '__main__':
    load_keywords()
    load_provinces()
    load_cookies()

    process = CrawlerProcess(settings={
        'FEED_FORMAT': 'json',
        'FEED_URI': 'articles.json'
    })
    process.crawl(WeiboSpider, keywords = KEYWORDS, list_prov = LIST_PROV, start_date = START_DATE, end_date = END_DATE, cookies = COOKIES)
    process.start()

    input("press any key to continue...") # 阻止终端自动退出