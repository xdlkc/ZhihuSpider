# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy


class ZhihuSpiderItem(scrapy.Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    pass


class TopicItem(scrapy.Item):
    id = scrapy.Field()
    name = scrapy.Field()
    url = scrapy.Field()
    excerpt = scrapy.Field()
    introduction = scrapy.Field()
    avatar_url = scrapy.Field()
    is_black = scrapy.Field()
    is_vote = scrapy.Field()
    parent_id = scrapy.Field()
    type = scrapy.Field()
    category = scrapy.Field()
