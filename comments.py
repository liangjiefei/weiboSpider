# coding=utf-8

if __name__ == '__main__':
    from Spider import TopicBandSpider
    import time

    Spider = TopicBandSpider()

    # 不间断更新每个话题的最新微博
    while True:
        try:
            Spider.load_setting()
            Spider.all_weibos_comments()
        except Exception:
            pass
        finally:
            time.sleep(5)
