# coding=utf-8
if __name__ == '__main__':
    from Spider import TopicBandSpider
    import time
    import pymongo

    Spider = TopicBandSpider()

    # 连接数据库
    client = pymongo.MongoClient()
    db = client["微博话题_db"]
    topic_bands_col = db["话题榜_col"]
    weibos_col = db["微博_col"]

    # 设置微博数据库索引
    weibos_col.create_index(key="微博id")

    # 不间断更新每个话题的最新微博
    while True:
        try:
            Spider.load_setting()
            topic_bands = list(topic_bands_col.find())
            for topic in topic_bands:
                now = time.time()
                if now - topic["更新时间"] < 900:
                    result, weibos = Spider.topic_weibos(topic_name=topic["微博话题"])
                    if result:
                        weibos_col.insert_many(weibos)
            time.sleep(5)
        except Exception:
            pass


