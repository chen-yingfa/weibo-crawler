# -*- coding: utf-8 -*-
import scrapy
import sqlite3
from bs4 import BeautifulSoup
from datetime import date
import datetime
import re # for parsing cookies
import csv
import time

# class for storing weibo posts
class Post:
    mid = 0
    author = ""
    uploadDate = date(1, 1, 1)
    prov = ""
    city = ""
    reposts = 0
    comments = 0
    likes = 0
    content = ""
    
    def __init__(self, mid, author, uploadDate, prov, city, reposts, comments, likes, content):
        self.mid = mid
        self.author = author
        self.uploadDate = uploadDate
        self.prov = prov
        self.city = city
        self.reposts = reposts
        self.comments = comments
        self.likes = likes
        self.content = content
    
class WeiboSpider(scrapy.Spider):
    name = 'weibo'
    cookies = """
    manually login to weibo using a browser,
    then look at the packets your browser has sent,
    find the cookies that allows the login
    and paste it here.
    no need to format the string.
            """
    allowed_domains = ['weibo.com']      
    dbName = "weibo"                     # name of database (unused)
    keywords = ["疫情", "肺炎", "口罩"]   # the keywords to be search
    # time range of results
    startDate = date(2020, 3, 1)        
    endDate = date(2020, 3, 2)
    # provinces to be crawled, use data from "location.db" or 
    # "location.csv" to loop through all locations
    prov = ["北京", "天津", "重庆", "上海", "湖北", "广东", "香港"] 

    locations = []
    # ex_url = "https://s.weibo.com/weibo/疫情&region=custom:11:8&typeall=1&suball=1&timescope=custom:2020-02-01-0:2020-02-01-1&Refer=g&page=11"
    baseUrl = "https://s.weibo.com/weibo/{}&region=custom:{}:{}&typeall=1&suball=1&timescope=custom:{}:{}&Refer=g&page={}"
    start_urls = []
    posts = {} 
    tags = [] # [[tagId, tagString]], tags that have been found 
    tagPost = [] # [[tagId, PosId]], is the relationship between tags and posts

    ###### utility ########
    def addPost(self, post):
        self.posts[post.mid] = post
        # print("< added post >")

    def addTag(self, tag):
        for t in self.tags:
            if tag == t[1]:
                return
        self.tags += [[len(self.tags), tag]]
        # print("< added tag >")
    
    def addTagPost(self, tagId, postId):
        self.tagPost += [[tagId, postId]]
        # print("< added tagpost >")

    # return the id of a tag, -1 when tag is not in database
    def getTagId(self, tag):
        for t in self.tags:
            if t[1] == tag:
                return int(t[0])
        # print("< got id >")
        return -1

    # convert and return date from a string
    def strToDate(self, s):
        return date(int(s[:4]), int(s[5:7]), int(s[8:10]))

    # convert and return the Post object from a string
    def listStrToPost(self, l):
        mid = int(l[0])
        uploadDate = self.strToDate(l[2])
        prov = l[3]
        city = l[4]
        reposts = int(l[5])
        comments = int(l[6])
        likes = int(l[7])
        content = l[8]

        return Post(mid, l[1], uploadDate, prov, city, reposts, comments, likes, content)

    # return string of url with the specified parameters, for searching
    def getUrl(self, keyword, provId, cityId, startTime, endTime, page = 1):
        return self.baseUrl.format(keyword, str(provId), str(cityId), startTime, endTime, str(page))

    # returns the id of a province
    def getProvId(self, prov):
        for loc in self.locations:
            # print(loc[1])
            if prov == loc[1]:
                return loc[0]
        return -1

    ###### utility end ####

    # creates the db files and tables if not exist
    def createDb(self, date, prov):
        provId = self.getProvId(prov)
        self.dbName += str(date) + "_" + str(provId) + ".db"

        conn = sqlite3.connect(self.dbName)
        c = conn.cursor()

        # create tables
        cmd = """CREATE TABLE IF NOT EXISTS posts(
            mid INTEGER PRIMARY KEY, 
            author TEXT, 
            date TEXT, 
            province TEXT,
            city TEXT,
            reposts INTEGER, 
            comments INTEGER, 
            likes INTEGER, 
            content TEXT)"""
        c.execute(cmd)
        cmd = """CREATE TABLE IF NOT EXISTS tags(
            tagId INTEGER PRIMARY KEY,
            tag TEXT)"""
        c.execute(cmd)
        cmd = """CREATE TABLE IF NOT EXISTS tagPost(
            tagID INTEGER,
            postID INTEGER)"""
        c.execute(cmd)

        conn.commit()
        conn.close()

    # load locations from location.csv
    def loadLocations(self):
        self.locations = []
        with open('locations.csv', mode = 'r', encoding = 'utf-8') as locFile:
            r = csv.reader(locFile)
            next(r)
            for line in r: # first row is description
                self.locations += [[int(line[0]), line[1], int(line[2]), line[3]]]
        # print(self.locations)
        print('< loaded locations >')

    def loadTags(self):
        conn = sqlite3.connect(self.dbName)
        c = conn.cursor()

        table = c.execute("SELECT * FROM tags")
        for row in table:
            self.tags += [[int(row[0]), row[1]]]

        conn.commit()
        conn.close()
    
    def loadTagPost(self):
        conn = sqlite3.connect(self.dbName)
        c = conn.cursor()

        table = c.execute("SELECT * FROM tagPost")
        
        for row in table:
            # print("row:", row)
            if not list(row) in self.tagPost:
                self.addTagPost(row[0], row[1])

        conn.commit()
        conn.close()
        print("< loaded tagpost >")

    def loadPosts(self):
        conn = sqlite3.connect(self.dbName)
        c = conn.cursor()
        
        table = c.execute("SELECT * FROM posts")
        for row in table:
            post = self.listStrToPost(row)
            self.addPost(post)

        print('< loaded posts >')

        conn.commit()
        conn.close()

    def savePosts(self):
        conn = sqlite3.connect(self.dbName)
        c = conn.cursor()

        self.loadPosts()

        cmd = "DELETE FROM posts"
        c.execute(cmd)

        for mid, p in self.posts.items():
            c.execute("INSERT INTO posts values(?, ?, ?, ?, ?, ?, ?, ?, ?)", (
                    p.mid, 
                    p.author, 
                    p.uploadDate,
                    p.prov,
                    p.city,
                    p.reposts,
                    p.comments,
                    p.likes,
                    p.content)
                )
            
        conn.commit()
        conn.close()
        print("< saved posts, count: {} >".format(len(self.posts)))

    # save tags
    def saveTags(self):
        conn = sqlite3.connect(self.dbName)
        c = conn.cursor()

        cmd = "DELETE FROM tags"
        c.execute(cmd)

        for t in self.tags:
            cmd = """INSERT INTO tags values
                ('{}', '{}')
                """.format(
                    t[0], 
                    t[1]
                )
            c.execute(cmd)
            
        conn.commit()
        conn.close()
        print("< saved tags, count: {} >".format(len(self.tags)))

    # tagPost is the relationship between tags and posts
    def saveTagPost(self):
        conn = sqlite3.connect(self.dbName)
        c = conn.cursor()

        self.loadTagPost()
        cmd = "DELETE FROM tagPost"
        c.execute(cmd)

        for tp in self.tagPost:
            # print("tp", tp)
            cmd = """INSERT INTO tagPost values
                ('{}', '{}')
                """.format(
                    tp[0], 
                    tp[1]
                )
            c.execute(cmd)
            
        conn.commit()
        conn.close()
        print("< saved tagpost, count: {}>".format(len(self.tagPost)))

    def save(self):
        self.savePosts()
        # self.saveTags()
        # self.saveTagPost()

    def start_requests(self):
        self.loadLocations() # load id of all locations

        searchProv = self.prov[0]

        self.createDb(self.startDate, searchProv)
        # self.loadTags()
        # self.loadTagPost()

        # add cookies for logging in
        self.cookies = {i.split("=")[0]:i.split("=")[1] for i in self.cookies.split("; ")}

        # set time range of each search, because exceeding 50 page limit will result 
        # in some results not accessible, so we have to devide the time range into 
        # smaller chunks or time. this will make the spider slighty slower
        timeRange = 2

        # generate urls
        day = self.startDate
        while day != self.endDate:
            for keyword in self.keywords[2:3]: 
                for i in range(0, 23, timeRange):
                    startTime = str(day) + "-" + str(i)
                    endTime = str(day) + "-" + str(min(23, i + timeRange))

                    for loc in self.locations:

                        if loc[1] != searchProv: # only search the locations in self.prov
                            continue

                        print("< yielding {}, from {} to {}, keyword: '{}' >".format(loc, startTime, endTime, keyword))

                        startPage = 1
                        url = self.getUrl(keyword, loc[0], loc[2], startTime, endTime, startPage)
                        self.start_urls += [url]

                        # go to url and start parsing
                        yield scrapy.Request(
                            url,
                            callback = self.parse,
                            cookies = self.cookies,
                            meta = {
                                "provId": loc[0],
                                "prov": loc[1],
                                "cityId": loc[2],
                                "city": loc[3],
                                "startTime": startTime,
                                "endTime": endTime,
                                "url": url,
                                "pageNum": startPage,
                                "keyword": keyword
                            }
                        )
                        time.sleep(1)
                startTime = str(day) + "-23"
                endTime = str(day + datetime.timedelta(days = 1)) + "-0"
                for loc in self.locations[:18]:
                    if loc[1] != searchProv:
                        continue
                    print("< yielding {}, from {} to {}, keyword: '{}' >".format(loc, startTime, endTime, keyword))

                    startPage = 1
                    url = self.getUrl(keyword, loc[0], loc[2], startTime, endTime, startPage)
                    self.start_urls += [url]

                    # go to url and start parsing
                    yield scrapy.Request(
                        url,
                        callback = self.parse,
                        cookies = self.cookies,
                        meta = {
                            "provId": loc[0],
                            "prov": loc[1],
                            "cityId": loc[2],
                            "city": loc[3],
                            "startTime": startTime,
                            "endTime": endTime,
                            "url": url,
                            "pageNum": startPage,
                            "keyword": keyword
                        }
                    )
                    time.sleep(1)
                self.save()
            day += datetime.timedelta(days = 1)

    def parse(self, response):
        # get meta data
        meta = response.meta
        url = meta["url"]
        pageNum = meta["pageNum"]
        provId = meta["provId"]
        prov = meta["prov"]
        cityId = meta["cityId"]
        city = meta["city"]
        startTime = meta["startTime"]
        endTime = meta["endTime"]
        keyword = meta["keyword"]

        print("< parsing {}, {}, from {} to {}, keyword: {}, page num: {} >".format(prov, city, startTime, endTime, keyword, pageNum))
        # print("< url: {} >".format(url))

        soup = BeautifulSoup(response.body, "html.parser")
        
        # check if this page exist
        errorTag = soup("div", {"class": "m-error"})
        if len(errorTag) != 0:
            # page does not exist (has reached the last page of results)
            print("< page not found >")
            return

        # loop and parse each card
        cardWraps = soup("div", {"mid": True})
        cnt = 0
        for cardWrap in cardWraps:
            mid = 0
            author = ""
            content = ""
            reposts = 0
            comments = 0
            likes = 0
            uploadDate = date(1, 1, 1)
            # tags = []

            # get mid and check if processed
            mid = int(cardWrap["mid"].strip())
            if mid in self.posts:
                continue

            # get author
            nameTag = cardWrap("a", {"class": "name"})[0]
            
            # get content tag
            contentTag = cardWrap.find("div", {"class": "content"})
            txtTags = contentTag.findAll("p", {"class": "txt"}, recursive = False)
            txtTag = None
            if len(txtTags) == 2:
                txtTag = txtTags[1]
            else:
                txtTag = txtTags[0]

            # get reposts, comments and likes tag
            cardAct = cardWrap("div", {"class": "card-act"})[0]
            liTag = cardAct("li")
            repostTxt = liTag[1].get_text().strip()
            commentTxt = liTag[2].get_text().strip()
            likeTxt = liTag[3].get_text().strip()

            # get date tag
            fromTag = contentTag.find("p", {"class": "from"}, recursive = False)
            fromTxt = fromTag.get_text().strip()

            #set values
            author = nameTag.string.strip()
            content = txtTag.get_text().strip()
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
                uploadDate = date(2020, int(fromTxt[mIdx - 2:mIdx]), int(fromTxt[dIdx - 1:dIdx]))
            else:
                uploadDate = date.today()
            
            # getting tags
            # i = 0
            # while i < len(content):
            #     c = content[i]
            #     if c == "#":
            #         j = i + 1
            #         s = ""
            #         while j < len(content):
            #             c = content[j]
            #             if c == "#":
            #                 break
            #             s += c
            #             j += 1
            #         if j == len(content): # if there are an odd number of '#'
            #             break
            #         tags += [s]
            #         content = content.replace("#" + s + "#", "")
            #         i -= 1
            #     i += 1 
            
            post = Post(mid, author, uploadDate, prov, city, reposts, comments, likes, content)
            
            self.addPost(post)

            # for tag in tags:
            #     self.addTag(tag)
            #     self.addTagPost(self.getTagId(tag), mid)

            cnt += 1
        
        print("< done parsing, # posts: {} >".format(cnt))
        print("< total # posts, tags, tagPosts: {}, {}, {} >".format(len(self.posts), len(self.tags), len(self.tagPost)))

        # go to next page
        # print("< next page >")
        nextUrl = url[: - len(str(pageNum))] + str(pageNum + 1)
        time.sleep(1)
        yield scrapy.Request(
            nextUrl,
            callback = self.parse,
            cookies = self.cookies,
            meta = {
                "provId": provId,
                "prov": prov,
                "cityId": cityId,
                "city": city,
                "startTime": startTime,
                "endTime": endTime,
                "url": nextUrl,
                "pageNum": pageNum + 1,
                "keyword": keyword
            }
        )
