# -*- coding: utf-8 -*-
import scrapy
from bs4 import BeautifulSoup
import sqlite3
import time

class LocationSpider(scrapy.Spider):
    name = 'location'
    allowed_domains = ['weibo.com']
    start_urls = []
    base_url = "https://s.weibo.com/weibo/疫情&region=custom:{}:{}&typeall=1&suball=1&Refer=g&page=1"
    
    # provs = [[11, "北京"], [12, "天津"], [13, "河北"], [14, "山西"], [15, "内蒙古"], 
    #     [21," 辽宁"], [22, "吉林"], [23, "黑龙江"], [31, "上海"], [32, "江苏"], 
    #     [33, "浙江"], [35, "福建"], [36, "江西"], [37, "山东"], [41, "河南"], 
    #     [42, "湖北"], [43, "湖南"], [44, "广东"], [45, "广西"], [46, "海南"], 
    #     [50, "重庆"], [51, "四川"], [52, "贵州"], [53, "云南"], [54, "西藏"], 
    #     [61, "陕西"], [62, "甘肃"], [63, "青海"], [64, "宁夏"], [65, "新疆"], 
    #     [71, "台湾"], [81, "香港"], [82, "澳门"], [100," 其他"], [400, "海外"]]
    provs = [[44, "广东"], [50, "重庆"]]

    locations = []

    def initDb(self):
        conn = sqlite3.connect("location.db")
        c = conn.cursor()
        
        cmd = "CREATE TABLE IF NOT EXISTS locations (provId INTEGER, prov TEXT, cityId INTEGER, city TEXT)"
        c.execute(cmd)

        conn.commit()
        conn.close()

    def saveLocations(self):
        conn = sqlite3.connect("location.db")
        c = conn.cursor()

        print(self.locations)
        for d in self.locations:
            cmd = "INSERT INTO locations values ('{}', '{}', '{}', '{}')".format(d[0], d[1], d[2], d[3])
            c.execute(cmd)

        conn.commit()
        conn.close()
        print("< saved locations >")

    def saveLocation(self, loc):
        conn = sqlite3.connect("location.db")
        c = conn.cursor()

        cmd = "INSERT INTO locations values ('{}', '{}', '{}', '{}')".format(loc[0], loc[1], loc[2], loc[3])
        # print(cmd)
        c.execute(cmd)

        conn.commit()
        conn.close()
        print("< saved location >")

    def start_requests(self):
        self.initDb()

        # cookies for logging in
        cookies = """SINAGLOBAL=166.111.106.27_1582733363.370263; SCF=AvrCXQfTl-oC4aeQpjOBOpJieiuP8EXGN2x7JNBQF1aflpMPwjVOjqYErgbOn0cCQpo4WvoDGPst-JkJO-pFURU.; sso_info=v02m6alo5qztKWRk5yljpOQpZCToKWRk5iljoOgpZCjnLaNo5C5jYOMsYyjkLmJp5WpmYO0to2jkLmNg4yxjKOQuQ==; Apache=183.172.166.113_1584238799.207428; SUB=_2A25zaeCNDeRhGeBI71sV8y_OzzWIHXVQH1VFrDV_PUNbm9AfLWndkW9NRpFU0jiP_bmici4BHNzWb0Do2Jjw3zlP; SUBP=0033WrSXqPxfM725Ws9jqgMF55529P9D9WWXbsgG5LDANTs4eLQyO.p85NHD95QcSoB4ShepeoB4Ws4DqcjMi--NiK.Xi-2Ri--ciKnRi-zNSoqX1KB0eKzX1Btt; ALF=1615774813
            """
        cookies = {i.split("=")[0]:i.split("=")[1] for i in cookies.split("; ")}

        for prov in self.provs:
            for i in range(40, 101): 
                url = self.base_url.format(prov[0], i)
                self.start_urls += [url]
                print("< yielding url >", url)
                yield scrapy.Request(
                    url, 
                    callback = self.parse,
                    cookies = cookies,
                    meta = {
                        "provId": prov[0],
                        "prov": prov[1],
                        "cityId": i
                    }
                )
                time.sleep(2)
                

    def parse(self, response):
        meta = response.meta
        # print(meta)

        soup = BeautifulSoup(response.body, "html.parser")
        cTip = soup("span", {"class": "ctips"})[0]
        filterTxt = cTip.get_text().strip()

        if not "~" in filterTxt: # this city id does not correspond to a city
            print("< no city found >")
            return

        city = filterTxt[filterTxt.find("~") + 1 : -1]
        print("< city found >", city)
        prov = meta["prov"]
        provId = meta["provId"]
        cityId = meta["cityId"]

        self.locations += [[provId, prov, cityId, city]]
        self.saveLocation(self.locations[-1])
