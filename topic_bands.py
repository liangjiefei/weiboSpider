# coding=utf-8
"""
抓取实时社会话题榜的数据
"""
if __name__ == '__main__':
    from Spider import TopicBandSpider
    import time

    Spider = TopicBandSpider()

    # 10分钟更新一次话题榜
    while True:
        try:
            Spider.load_setting()
            Spider.all_topic_bands_info()
        except Exception:
            pass
        finally:
            time.sleep(600)
