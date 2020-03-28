# 微博爬虫
2020年4月

可以爬取的数据：给定**时间**和**地点**和**关键词**，爬取所有搜索结果。
参考了网上很多不同的代码，发现都用不了（微博经常更新），而且很少是用来爬取给定地点、时间和关键词的搜索结果。

所有细节都嵌套在weibo.py中，只需要将搜索参数（关键词，地点和日期）给main.py，在直接运行main.py即可。关键词和地点分别在keywords.txt和provinces.txt中填写，但是日期要求在代码中设置。

本程序要求手动用浏览器登录，然后通过浏览器发送的包获取cookies（欢迎你帮我搞登录，微博太难搞了）。

一台计算机一个账号，理想情况可以一秒5条，即一分钟300条，以天432000条，当然如果你有账号池，或者会多线程，可以自己进行改进。目前每页sleep两秒，不然有captcha，欢迎你帮我弄掉微博captcha。

#### 作者：

陈英发

最后一次更新：2020年3月14日
