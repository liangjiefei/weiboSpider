# coding=utf-8
"""
抓取实时社会话题榜的数据
"""
if __name__ == '__main__':
    from Spider import TopicBandSpider
    import time
    import pymongo

    Spider = TopicBandSpider()

    # 连接数据库
    client = pymongo.MongoClient()
    db = client["微博话题_db"]
    topic_bands_col = db["话题榜_col"]

    # 设置话题榜数据库索引
    topic_bands_col.create_index(keys="微博话题")

    # 10分钟更新一次话题榜
    while True:
        try:
            Spider.load_setting()
            result, topic_bands = Spider.all_topic_bands_info()
            if result:
                topic_bands_col.insert_many(topic_bands)
        except Exception:
            pass
        finally:
            time.sleep(600)
