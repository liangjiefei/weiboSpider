# coding=utf-8

"""
微博话题榜的爬虫，专门针对社会话题榜（包括话题榜下的所有微博，所有评论，话题榜信息）
"""

import requests
from random import choice
import time


class TopicBandSpider(object):

    def __init__(self):
        self.cookie_list = []
        self.user_agent_list = []
        self.proxy_list = []

    def load_setting(self):
        file = open('setting.json', 'r', encoding='utf8')
        setting = eval(file.read())
        self.cookie_list = setting["cookie_list"]
        self.user_agent_list = setting["user_agent_list"]

    def get_proxy_list(self):
        try:
            url = "http://api3.xiguadaili.com/ip/"
            params = {
                "tid": "556351517278224",
                "num": "10",
                "sortby": "time ",
                "format": "json",
                "protocol": "https",
                "category": "2",
                "delay": "5"
            }
            res = requests.get(url=url, params=params)
            if res.json() and type(res.json()) is list:
                self.proxy_list = res.json()
        except Exception:
            pass

    def get_proxy(self):
        proxies = choice(self.proxy_list)
        return {
            "https": "https://{}:{}".format(proxies["host"], proxies["port"]),
            "http": "http://{}:{}".format(proxies["host"], proxies["port"]),
        }

    def req(self, url, params):
        self.get_proxy_list()
        headers = self.get_headers()
        proxies = self.get_proxy()
        try:
            res = requests.get(url=url, params=params, headers=headers, timeout=10, proxies=proxies)
            if res.status_code != 200:
                self.req(url, params)
            else:
                if res.json():
                    print(res.url)
                    return res
                else:
                    self.req(url, params)
        except Exception:
            self.req(url, params)

    def get_headers(self):
        """随机构成headers"""
        return {
            'user-agent': choice(self.user_agent_list),
            'Cookie': choice(self.cookie_list)
        }

    def topic_bands(self):
        """获取社会话题榜的前50（目前无法突破50个）"""
        result = []
        url = "https://m.weibo.cn/api/container/getIndex"
        params = {
            "containerid": "231648_-_1_-_1_-_社会话题榜_-_2",
            "luicode": "10000011",
            "lfid": "106003type=25&t=3&disable_hot=1&filter_type=topicband",
            "page_type": "08",
            "page": ""
        }
        try:
            for page in range(1, 4):
                params["page"] = page
                res = self.req(url=url, params=params)
                card_group = res.json()["data"]["cards"][0]["card_group"]
                for card in card_group:
                    result.append(card["title_sub"])
            return True, result
        except Exception as ex:
            print(ex)
            return False, result

    def topic_weibos(self, topic_name):
        """获取话题下的所有微博"""
        result = []
        url = "https://m.weibo.cn/api/container/getIndex"
        containerid = "231522type=1&t=10&q={}".format(topic_name)
        params = {
            "containerid": containerid,
            "isnewpage": "1",
            "extparam": "c_type=81&pos=1-0-1",
            "luicode": "10000011",
            "lfid": "231648_-_1_-_1_-_社会话题榜_-_2",
            "page_type": "searchall",
            "page": 1
        }
        while True:
            try:
                res = self.req(url=url, params=params).json()
                if res["ok"] == 1:
                    data = res["data"]
                    cards = data["cards"]
                    for card in cards:
                        if card["card_type"] == 9:
                            mblog = card["mblog"]
                            user = mblog["user"]
                            weibo = {
                                "话题名字": topic_name,
                                "微博id": mblog.get("id", ""),
                                "创建时间": mblog.get("created_at", ""),
                                "更新时间": time.time(),
                                "来源": mblog.get("source", ""),
                                "微博内容": mblog.get("text", ""),
                                "用户id": user.get("id", ""),
                                "用户名": user.get("screen_name", ""),
                                "评论数": mblog.get("comments_count", ""),
                                "点赞数": mblog.get("attitudes_count", ""),
                                "转发数": mblog.get("reposts_count", ""),
                            }
                            result.append(weibo)
                            print(weibo)
                    params["page"] = params["page"] + 1
                else:
                    print("话题{}微博爬取完毕, 总共{}条微博".format(topic_name, len(result)))
                    return True, result
            except Exception as ex:
                return False, None

    def topic_info(self, topic_name):
        """获取话题详情"""
        url = "https://m.weibo.cn/api/container/getIndex"
        containerid = "231522type=1&t=10&q={}".format(topic_name)
        params = {
            "containerid": containerid,
            "isnewpage": "1",
            "extparam": "c_type=81&pos=1-0-1",
            "luicode": "10000011",
            "lfid": "231648_-_1_-_1_-_社会话题榜_-_2",
            "page_type": "searchall",
        }
        try:
            card_list_info = self.req(url=url, params=params).json()["data"][
                "cardlistInfo"]
            print("获取{}话题详情成功".format(topic_name))
            return True, {
                "微博话题": topic_name,
                "话题开始时间": card_list_info["starttime"],
                "主持人": card_list_info['cardlist_head_cards'][0]["head_data"]["downtext"].split("主持人：")[1],
                "阅读量": card_list_info['cardlist_head_cards'][0]["head_data"]["midtext"].split()[0].split("阅读")[1],
                "讨论量": card_list_info['cardlist_head_cards'][0]["head_data"]["midtext"].split()[1].split("讨论")[1],
                "话题贡献值排行": self.contributor(topic_name=topic_name)[1],
                "更新时间": time.time()
            }
        except Exception as ex:
            print("获取{}话题详情失败, 错误为{}".format(topic_name, ex))
            return False, None

    def contributor(self, topic_name):
        """获取话题贡献者排行前100"""
        result = []
        url = "https://m.weibo.cn/api/container/getIndex"
        containerid = "231522type=103&q={}".format(topic_name)
        params = {
            "containerid": containerid,
            "title": "话题贡献者排行",
            "luicode": "10000011",
            "lfid": containerid,
            "page": "",
        }
        try:
            for page in range(1, 100):
                params["page"] = page
                res = self.req(url=url, params=params).json()
                if res["ok"] == 1:
                    card_group = res["data"]["cards"][0]["card_group"]
                    for card in card_group:
                        result.append({
                            "用户id": card["user"]["id"],
                            "用户名": card["user"]["name"],
                            "贡献度": card["desc1"].split("贡献度：")[1]
                        })
                else:
                    return True, result
        except Exception:
            return False, None

    def comments(self, weibo_id):
        result = []
        url = "https://m.weibo.cn/comments/hotflow"
        params = {
            "id": weibo_id,
            "mid": weibo_id,
            "max_id": None,
            "max_id_type": "0"
        }
        while True:
            try:
                res = self.req(url=url, params=params).json()
                if res["ok"] == 1:
                    res = res["data"]
                    max_id = res["max_id"]
                    max_id_type = res["max_id_type"]
                    comments = res["data"]
                    for comment in comments:
                        comment_comments = comment["comments"]
                        result.append({
                            "评论内容id": comment["id"],
                            "评论用户id": comment["user"]["id"],
                            "评论用户名": comment["user"]["screen_name"],
                            "评论时间": comment["created_at"],
                            "评论内容": comment["text"],
                            "微博id": weibo_id,
                            "跟随评论内容id": comment["rootid"]
                        })
                        if comment_comments:
                            for comment_comment in comment_comments:
                                result.append({
                                    "评论内容id": comment_comment["id"],
                                    "评论用户id": comment_comment["user"]["id"],
                                    "评论用户名": comment_comment["user"]["screen_name"],
                                    "评论时间": comment_comment["created_at"],
                                    "评论内容": comment_comment["text"],
                                    "微博id": weibo_id,
                                    "跟随评论内容id": comment["rootid"]
                                })
                    params["max_id"] = str(max_id)
                    params["max_id_type"] = str(max_id_type)
                else:
                    print("微博{}评论爬取完毕, 总共{}条评论".format(weibo_id, len(result)))
                    return True, result
            except Exception as ex:
                return False, None

    def all_topic_bands_info(self):
        """获取社会热榜上的话题的详细信息"""
        try:
            result_list = []
            result, topic_bands = self.topic_bands()
            print(result, topic_bands)
            for topic in topic_bands:
                result, topic_info = self.topic_info(topic)
                result_list.append(topic_info)
            return True, result_list
        except Exception:
            return False, None


# # 测试
# if __name__ == '__main__':
#     a = TopicBandSpider()
#     a.load_setting()
    # a.get_proxy_list()
    # a, b = a.all_topic_bands_info()
    # print(a)
    # print(b)
    # data = a.topic_weibos("#明天还上班#")
    # print(a.get_publish_time("2分钟前"))
