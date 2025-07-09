# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
# import pymongo

class ShoresPipeline:

    # def __init__(self):
    #     self.conn = pymongo.MongoClient(
    #         'localhost',
    #         27017
    #     )
    #     db  = self.conn['Shores_Record']
    #     self.collection = db['Shores_Sale']

    def process_item(self, item, spider):
        # self.collection.insert(dict(item))
        # self.collection.insert_one(dict(item))
        return item
