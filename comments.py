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
    comments_col = db["评论_col"]

    # 设置评论数据库索引
    comments_col.create_index(key="评论内容id")

    # 不间断更新每个话题的最新微博
    while True:
        try:
            Spider.load_setting()
            weibos = list(weibos_col.find())
            for weibo in weibos:
                now = time.time()
                # 判断话题是否还在话题榜上
                if now - topic_bands_col.find_one({"微博话题": weibo["话题名字"]})["更新时间"] < 900:
                    result, comments = Spider.comments(weibo["微博id"])
                    if comments:
                        comments_col.insert_many(comments)
        except Exception:
            pass
        finally:
            time.sleep(5)
