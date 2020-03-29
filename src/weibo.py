# -*- coding: utf-8 -*-
import scrapy
import sqlite3
from bs4 import BeautifulSoup
from datetime import date
import datetime
import re
import csv
import time
from scrapy.crawler import CrawlerProcess
import random

MIN_SLEEP_TIME = 2
MAX_SLEEP_TIME = 4

# 每个微博条是一个类
class Post:
    mid = 0
    author = ""
    upload_date = date(1, 1, 1)
    prov = ""
    city = ""
    reposts = 0
    comments = 0
    likes = 0
    content = ""
    
    def __init__(self, mid, author, upload_date, prov, city, reposts, comments, likes, content):
        self.mid = mid
        self.author = author
        self.upload_date = upload_date
        self.prov = prov
        self.city = city
        self.reposts = reposts
        self.comments = comments
        self.likes = likes
        self.content = content
    
# 整个爬虫
class WeiboSpider(scrapy.Spider):
    name = 'weibo' # 爬虫名称
    cookies = """  由构造函数赋值
    """
    allowed_domains = ['weibo.com']

    # 搜索参数（由构造函数赋值）
    keywords = [] # 关键词
    start_date = date(2020, 1, 1) 
    end_date = date(2020, 1, 1)
    list_prov = [] # 省份
    
    locations = [] # 所有（市级）地点
    baseUrl = "https://s.weibo.com/weibo/{}&region=custom:{}:{}&typeall=1&suball=1&timescope=custom:{}:{}&Refer=g&page={}" 
    posts = {} # 爬取的所有微博条都存到posts，是一个dict of list of Post。dict的key是省份名称。

    data_dir = "../data/" # 数据库文件的文件夹
    num_unsaved_post = 0 # 记录仍未保存多少条
    num_post_total = 0   # 记录总共爬取多少条

    ###### utility ########
    def get_sleep_time(self):
        global MIN_SLEEP_TIME, MAX_SLEEP_TIME
        return random.randint(MIN_SLEEP_TIME, MAX_SLEEP_TIME)


    def add_post(self, post):
        self.posts[post.prov].append(post)
        # print("< added post >")

    def strToDate(self, s):
        """
        str转换成date
        """
        return date(int(s[:4]), int(s[5:7]), int(s[8:10]))

    def list_str_to_Post(self, l):
        """
        将list of str转换成Post，其中str顺序：mid, author, upload_date, prov, city, reposts, comments, likes, content
        """
        mid = int(l[0])
        upload_date = self.strToDate(l[2])
        prov = l[3]
        city = l[4]
        reposts = int(l[5])
        comments = int(l[6])
        likes = int(l[7])
        content = l[8]

        return Post(mid, l[1], upload_date, prov, city, reposts, comments, likes, content)

    def get_url(self, keyword, prov_id, city_id, start_time, end_time, page = 1):
        """
        返回对应着参数的url
        """
        return self.baseUrl.format(keyword, str(prov_id), str(city_id), start_time, end_time, str(page))

    def get_db_name(self, date, prov):
        """
        返回对应日期和省份的数据库文件夹名称
        """
        prov_id = self.get_prov_id(prov)
        return self.data_dir + str(date) + "_" + str(prov_id) + ".db"

    def get_prov_id(self, prov):
        """
        返回省份的编号
        """
        for location in self.locations:
            # print(location[1])
            if prov == location[1]:
                return location[0]
        return -1

    ###### utility end ####

    def __init__(self, keywords, list_prov, start_date, end_date, cookies):
        """
        获取搜索参数
        """
        self.keywords = keywords
        self.list_prov = list_prov
        self.start_date = start_date
        self.end_date = end_date
        self.cookies = cookies

    def create_db(self, date, prov):
        """
        若没有db文件（数据库）对应此日期和省份，创建对应的db文件
        """
        prov_id = self.get_prov_id(prov)
        self.db_name = self.data_dir + str(date) + "_" + str(prov_id) + ".db"

        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()

        # create table
        cmd = """CREATE TABLE IF NOT EXISTS posts(
            mid KEY, 
            author TEXT, 
            date TEXT, 
            province TEXT,
            city TEXT,
            reposts INTEGER, 
            comments INTEGER, 
            likes INTEGER, 
            content TEXT)"""
        c.execute(cmd)

        conn.commit()
        conn.close()

    def load_locations(self):
        """
        载入所有市级地点
        """
        self.locations = []
        with open('locations.csv', mode = 'r', encoding = 'utf-8') as locFile:
            r = csv.reader(locFile)
            next(r)
            for line in r: # first row is description
                self.locations += [[int(line[0]), line[1], int(line[2]), line[3]]]
        # print(self.locations)
        print('< loaded locations >')

    def load_posts(self):
        """
        从数据库载入微博
        """
        conn = sqlite3.connect(self.db_name)
        c = conn.cursor()
        
        table = c.execute("SELECT * FROM posts")
        for row in table:
            post = self.list_str_to_Post(row)
            self.add_post(post)

        print('< loaded posts >')

        conn.commit()
        conn.close()

    def save_posts(self, date, prov):
        """
        保存微博条
        """
        db_name = self.get_db_name(date, prov)
        conn = sqlite3.connect(db_name)
        c = conn.cursor()

        # self.load_posts()
        # cmd = "DELETE FROM posts"
        # c.execute(cmd)

        cnt = 0 # 用于记录总共保存多少条微博
        for p in self.posts[prov][:]:
            if p.prov != prov: 
                continue
            c.execute("INSERT INTO posts values(?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                    p.mid, 
                    p.author, 
                    p.upload_date,
                    p.prov,
                    p.city,
                    p.reposts,
                    p.comments,
                    p.likes,
                    p.content)
                )
            self.posts[prov].remove(p) # 去掉已经保存的微博
            cnt += 1
        self.num_unsaved_post -= cnt
        conn.commit()
        conn.close()
        print("< saved posts, count: {} >".format(cnt))

    def save(self, date, prov):
        self.save_posts(date, prov)

    def start_requests(self):
        """
        根据输入搜索参数，生成对应的Request并且发送请求。
        """
        self.load_locations()

        # 创建posts和数据库文件
        for prov in self.list_prov:
            self.create_db(self.start_date, prov) # create a db file for each province
            self.posts[prov] = []   # init dict of list of posts

        # 将cookies格式化
        self.cookies = {i.split("=")[0]:i.split("=")[1] for i in self.cookies.split("; ")}

        # 设置一个时间范围防止搜索结果多于50页（1000条），因为微博不会返回多于50页的结果，所以会某些微博条
        time_range = 2 # 默认2小时

        # 生成url
        day = self.start_date
        while day != self.end_date: # 遍历天
            for location in self.locations: # 遍历（市级）地点
                prov_id, prov, city_id, city = location[0], location[1], location[2], location[3]

                if not prov in self.list_prov: # 不是目标省份的地点则跳过
                    continue
                for keyword in self.keywords: # 遍历关键词
                    for i in range(0, 23, time_range): # 遍历一天内的每个时间段（默认两个小时）
                        start_time = str(day) + "-" + str(i)
                        end_time = str(day) + "-" + str(min(23, i + time_range))

                        prov_id, prov, city_id, city = location[0], location[1], location[2], location[3]
                        print("< yielding {}, from {} to {}, keyword: '{}' >".format(location, start_time, end_time, keyword))

                        start_page = 1
                        url = self.get_url(keyword, prov_id, city_id, start_time, end_time, start_page)
                        # self.start_urls += [url]

                        # go to url and start parsing
                        yield scrapy.Request(
                            url,
                            callback = self.parse,
                            cookies = self.cookies,
                            meta = {
                                "prov_id": prov_id,
                                "prov": prov,
                                "city_id": city_id,
                                "city": city,
                                "start_time": start_time,
                                "end_time": end_time,
                                "url": url,
                                "page_num": start_page,
                                "keyword": keyword
                            }
                        )
                        # time.sleep(1)

                    # 最后23:00到下一天的00:00的数据
                    start_time = str(day) + "-23"
                    end_time = str(day + datetime.timedelta(days = 1)) + "-0"
                    print("< yielding {}, from {} to {}, keyword: '{}' >".format(location, start_time, end_time, keyword))

                    start_page = 1
                    url = self.get_url(keyword, prov_id, city_id, start_time, end_time, start_page)
                    # self.start_urls += [url]

                    # go to url and start parsing
                    yield scrapy.Request(
                        url,
                        callback = self.parse,
                        cookies = self.cookies,
                        meta = {
                            "prov_id": prov_id,
                            "prov": prov,
                            "city_id": city_id,
                            "city": city,
                            "start_time": start_time,
                            "end_time": end_time,
                            "url": url,
                            "page_num": start_page,
                            "keyword": keyword
                        }
                    )
                    # time.sleep(1)
                self.save(day, prov) # 保存
            day += datetime.timedelta(days = 1) # 下一天
        self.save(day, prov)
    def parse(self, response):
        """
        对于html进行parsing
        """

        # meta数据
        meta = response.meta
        url = meta["url"]
        page_num = meta["page_num"]
        prov_id = meta["prov_id"]
        prov = meta["prov"]
        city_id = meta["city_id"]
        city = meta["city"]
        start_time = meta["start_time"]
        end_time = meta["end_time"]
        keyword = meta["keyword"]

        print("< parsing {}, {}, from {} to {}, keyword: {}, page num: {} >".format(prov, city, start_time, end_time, keyword, page_num))
        # print("< url: {} >".format(url))

        soup = BeautifulSoup(response.body, "html.parser")
        
        # 检查是否到达尾页
        errorTag = soup("div", {"class": "m-error"})
        if len(errorTag) != 0:
            # 页不存在
            print("< page not found >")
            return

        # loop and parse each card
        list_card_wrap = soup("div", {"mid": True})
        cnt = 0 # 记录多少card
        for card_wrap in list_card_wrap:
            mid = 0
            author = ""
            content = ""
            reposts = 0
            comments = 0
            likes = 0
            upload_date = date(1, 1, 1)
            # tags = []

            # get mid (unique id of each weibo) and check if processed
            mid = int(card_wrap["mid"].strip())
            if mid in self.posts:
                continue

            # get author
            nameTag = card_wrap("a", {"class": "name"})[0]
            
            # get content tag
            content_tag = card_wrap.find("div", {"class": "content"})
            list_txt_tag = content_tag.findAll("p", {"class": "txt"}, recursive = False)
            txt_tag = None
            if len(list_txt_tag) == 2:
                txt_tag = list_txt_tag[1]
            else:
                txt_tag = list_txt_tag[0]

            # get reposts, comments and likes tag
            card_act = card_wrap("div", {"class": "card-act"})[0]
            liTag = card_act("li")
            repostTxt = liTag[1].get_text().strip()
            commentTxt = liTag[2].get_text().strip()
            likeTxt = liTag[3].get_text().strip()

            # get date tag
            fromTag = content_tag.find("p", {"class": "from"}, recursive = False)
            fromTxt = fromTag.get_text().strip()

            # 给post的参数赋值
            author = nameTag.string.strip()
            content = txt_tag.get_text().strip()
            if content[-5:] == "收起全文d":
                content = content[:-5]

            if len(repostTxt) < 4:
                reposts = 0
            else:
                reposts = int(repostTxt[3:])
            
            if len(commentTxt) < 4:
                comments = 0
            else:
                comments = int(commentTxt[3:])

            if len(likeTxt) < 1:
                likes = 0
            else:
                likes = int(likeTxt)

            if "月" in fromTxt:
                mIdx = fromTxt.find("月")
                dIdx = fromTxt.find("日")
                upload_date = date(2020, int(fromTxt[mIdx - 2:mIdx]), int(fromTxt[dIdx - 1:dIdx]))
            else:
                upload_date = date.today()
            
            post = Post(mid, author, upload_date, prov, city, reposts, comments, likes, content)
            
            self.add_post(post)

            cnt += 1
        
        # 记录总共爬取多少
        self.num_unsaved_post += cnt
        self.num_post_total += cnt
        print("< done parsing posts: {}, total: {}, unsaved: {} >".format(cnt, self.num_post_total, self.num_unsaved_post))

        # go to next page
        # print("< next page >")
        next_url = url[: - len(str(page_num))] + str(page_num + 1)
        time.sleep(self.get_sleep_time())
        yield scrapy.Request(
            next_url,
            callback = self.parse,
            cookies = self.cookies,
            meta = {
                "prov_id": prov_id,
                "prov": prov,
                "city_id": city_id,
                "city": city,
                "start_time": start_time,
                "end_time": end_time,
                "url": next_url,
                "page_num": page_num + 1,
                "keyword": keyword
            }
        )

# 主函数
if __name__ == '__main__':
    process = CrawlerProcess(settings={
        'FEED_FORMAT': 'json',
        'FEED_URI': 'articles.json'
    })
    process.crawl(WeiboSpider)
    process.start()
