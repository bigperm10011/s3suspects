# -*- coding: utf-8 -*-
from s3suspects import settings
import sqlalchemy
from sqlalchemy import create_engine, Column, Integer, String, DateTime
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import mapper, sessionmaker
from helpers import load_tables, gen_html, send_mail
import datetime
# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html


class SuspectPipeline(object):
    def process_item(self, item, spider):
        print('***** Pipeline Processing Started ******')
        #getting sqlalchemy session from spider
        sesh = spider.sesh
        if item['link'] in spider.link_list:
            print('Duplicate Suspect. Skipping...')
            print('***** Pipeline Processing Complete ******')
            return None
        else:
            print('New Suspect Found')
            suspect = spider.Suspect()
            suspect.name=item['name']
            suspect.leaverid=item['ident']
            ts_format = datetime.datetime.now(datetime.timezone.utc).isoformat()
            suspect.datetimeadded = ts_format
            try:
                suspect.srole=item['role']
            except:
                suspect.srole = None
            try:
                suspect.sfirm=item['firm']
            except:
                suspect.sfirm= None
            try:
                suspect.slocation=item['location']
            except:
                suspect.slocation= None
            suspect.slink=item['link']
            try:
                print('Updating DB...')
                sesh.add(suspect)
                sesh.commit()
            except IntegrityError:
                print('DB Integrity Error....', item['name'])
        print('***** Pipeline Processing Complete ******')
        return item

    def close_spider(self, spider):
        sesh = spider.sesh
        susps = sesh.query(spider.Suspect).filter_by(result=None).all()
        today = datetime.date.today()
        added = []
        for s in susps:
            try:
                when = s.datetimeadded
                date = when.date()
                if date == today:
                    added.append(s)
            except:
                pass
        if len(added) > 0:
            try:
                html = gen_html(added)
                resp_code = send_mail(html)
                print(resp_code)
            except:
                pass
