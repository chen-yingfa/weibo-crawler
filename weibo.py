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

SLEEP_TIME = 2

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
    
class WeiboSpider(scrapy.Spider):
    name = 'weibo'
    cookies = """
    """
    allowed_domains = ['weibo.com']

    # 搜索参数
    keywords = [] # 关键词
    start_date = date(2020, 1, 1)
    end_date = date(2020, 1, 1)
    list_prov = [] # 省份
    
    
    locations = []
    # ex_url = "https://s.weibo.com/weibo/疫情&region=custom:11:8&typeall=1&suball=1&timescope=custom:2020-02-01-0:2020-02-01-1&Refer=g&page=11"
    baseUrl = "https://s.weibo.com/weibo/{}&region=custom:{}:{}&typeall=1&suball=1&timescope=custom:{}:{}&Refer=g&page={}"
    start_urls = []
    posts = {}

    data_dir = "./data/"
    num_unsaved_post = 0
    num_post_total = 0

    ###### utility ########
    def add_post(self, post):
        self.posts[post.prov].append(post)
        # print("< added post >")

    def strToDate(self, s):
        return date(int(s[:4]), int(s[5:7]), int(s[8:10]))

    def list_str_to_Post(self, l):
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
        return self.baseUrl.format(keyword, str(prov_id), str(city_id), start_time, end_time, str(page))

    def get_db_name(self, date, prov):
        prov_id = self.get_prov_id(prov)
        return self.data_dir + str(date) + "_" + str(prov_id) + ".db"

    def get_prov_id(self, prov):
        for location in self.locations:
            # print(location[1])
            if prov == location[1]:
                return location[0]
        return -1

    ###### utility end ####

    def __init__(self, keywords, list_prov, start_date, end_date, cookies):
        self.keywords = keywords
        self.list_prov = list_prov
        self.start_date = start_date
        self.end_date = end_date
        self.cookies = cookies

    def create_db(self, date, prov):
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
        self.locations = []
        with open('locations.csv', mode = 'r', encoding = 'utf-8') as locFile:
            r = csv.reader(locFile)
            next(r)
            for line in r: # first row is description
                self.locations += [[int(line[0]), line[1], int(line[2]), line[3]]]
        # print(self.locations)
        print('< loaded locations >')

    def load_posts(self):
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
        db_name = self.get_db_name(date, prov)
        conn = sqlite3.connect(db_name)
        c = conn.cursor()

        # self.load_posts()
        # cmd = "DELETE FROM posts"
        # c.execute(cmd)

        cnt = 0
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
            self.posts[prov].remove(p)
            cnt += 1
        self.num_unsaved_post -= cnt
        conn.commit()
        conn.close()
        print("< saved posts, count: {} >".format(cnt))

    def save(self, date, prov):
        self.save_posts(date, prov)

    def start_requests(self):
        self.load_locations()

        for prov in self.list_prov:
            self.create_db(self.start_date, prov) # create a db file for each province
            self.posts[prov] = []   # init dict of list of posts

        # add cookies for logging in
        self.cookies = {i.split("=")[0]:i.split("=")[1] for i in self.cookies.split("; ")}

        # set time range of results
        time_range = 2

        # gen urls
        day = self.start_date
        while day != self.end_date:
            for location in self.locations:
                prov_id, prov, city_id, city = location[0], location[1], location[2], location[3]
                if not prov in self.list_prov:
                    continue
                for keyword in self.keywords: 
                    for i in range(0, 23, time_range):
                        start_time = str(day) + "-" + str(i)
                        end_time = str(day) + "-" + str(min(23, i + time_range))

                        prov_id, prov, city_id, city = location[0], location[1], location[2], location[3]
                        print("< yielding {}, from {} to {}, keyword: '{}' >".format(location, start_time, end_time, keyword))

                        start_page = 1
                        url = self.get_url(keyword, prov_id, city_id, start_time, end_time, start_page)
                        self.start_urls += [url]

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
                        time.sleep(1)

                    start_time = str(day) + "-23"
                    end_time = str(day + datetime.timedelta(days = 1)) + "-0"
                    print("< yielding {}, from {} to {}, keyword: '{}' >".format(location, start_time, end_time, keyword))

                    start_page = 1
                    url = self.get_url(keyword, prov_id, city_id, start_time, end_time, start_page)
                    self.start_urls += [url]

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
                    time.sleep(1)
                self.save(day, prov)
            day += datetime.timedelta(days = 1)

    def parse(self, response):
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
        
        # check if this page exist
        errorTag = soup("div", {"class": "m-error"})
        if len(errorTag) != 0:
            # page does not exist
            print("< page not found >")
            return

        # loop and parse each card
        list_card_wrap = soup("div", {"mid": True})
        cnt = 0
        for card_wrap in list_card_wrap:
            mid = 0
            author = ""
            content = ""
            reposts = 0
            comments = 0
            likes = 0
            upload_date = date(1, 1, 1)
            # tags = []

            # get mid and check if processed
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

            #set values
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
        
        self.num_unsaved_post += cnt
        self.num_post_total += cnt
        print("< done parsing posts: {}, total: {}, unsaved: {} >".format(cnt, self.num_post_total, self.num_unsaved_post))

        # go to next page
        # print("< next page >")
        next_url = url[: - len(str(page_num))] + str(page_num + 1)
        time.sleep(SLEEP_TIME)
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

if __name__ == '__main__':
    process = CrawlerProcess(settings={
        'FEED_FORMAT': 'json',
        'FEED_URI': 'articles.json'
    })
    process.crawl(WeiboSpider)
    process.start()
