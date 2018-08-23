import scrapy
import re
import datetime
from sqlalchemy import create_engine, Column, Integer, String, DateTime, MetaData, Table
from sqlalchemy.engine.url import URL
import sqlalchemy
from s3suspects import settings
from sqlalchemy.orm import mapper, sessionmaker
from s3suspects.items import S3SuspectsItem
from helpers import load_tables, remove_html_markup, clean_string, score_name, list2string, zone1, zone2, zone3a



class QuotesSpider(scrapy.Spider):
    name = "suspects"
    sesh, Suspect, Leaver = load_tables()
    lvr = sesh.query(Leaver).filter_by(result='Lost', inprosshell='Yes').order_by(Leaver.lasttracked).limit(5).all()
    slinks = sesh.query(Suspect).all()
    link_list = []
    for s in slinks:
        link_list.append(s.link)

    def start_requests(self, sesh=sesh, Leaver=Leaver, lvr=lvr):
        print('number of names to be scraped:', len(lvr))
        if len(lvr) > 0:
            for l in lvr:
                print('Leaver Selected: ', l.name)
                lid = l.id
                string = str('https://www.google.com/search?q=' + l.name + ' ' + 'site:www.linkedin.com/in/' + ' ' + 'language:en')
                url = string
                #l.timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
                sesh.commit()

                yield scrapy.Request(url=url, callback=self.parse, meta={'lid': l.id, 'name': l.name})
        else:
            raise CloseSpider('All Leavers Have Suspects')
    def parse(self, response):
        db_name = response.meta['name']
        for i in response.xpath("//div[contains(@class, 'g')]"):
            print('**** G CLASS ****', i)
            raw_lnk = str(i.xpath(".//cite").extract())
            clink = zone2(raw_lnk)
            print('Testing New Zone2: ', clink)
            if 'https://www.linkedin.com/in/' in clink:
                h3a = i.xpath(".//h3/a").extract()
                name, role1, firm1 = zone1(h3a)
                slp_xtract = i.xpath(".//div[contains(@class, 'slp')]/descendant::text()").extract()
                print('Raw SLP Xtract: ', slp_xtract)
                print('LENGTH of SLP Xtract: ', len(slp_xtract))

                if len(slp_xtract) > 0:
                    txt = str(slp_xtract)
                    print('length of slp: ', len(txt))
                    print('slp class detected. Running Zone3a Analysis...')
                    city, role, firm = zone3a(txt)
                    print('results from zone3a analysis: ')
                    item = S3SuspectsItem()
                    item['name'] = name
                    item['link'] = clink
                    item['ident'] = response.meta['lid']
                    item['location'] = city
                    if role1 == None:
                        item['role'] = role
                    else:
                        item['role'] = role1
                    if firm1 == None:
                        item['firm'] = firm
                    else:
                        item['firm'] = firm1
                    score = score_name(item['name'], db_name)
                    if score > 80:
                        item['status'] = 'Success'
                        yield item
                    else:
                        yield None

                else:
                    print('no slp class found.  salvaging text')
                    st_class = i.xpath(".//span[contains(@class, 'st')]/descendant::text()").extract()
                    print('ST Text Extracted: ', st_class)
                    salvage_string = list2string(st_class)
                    print('st class converted to string: ', salvage_string)
                    cleaned_str = clean_string(salvage_string, name)
                    cleaned_str = cleaned_str.strip()
                    print('st string filtered: ', cleaned_str)
                    item = S3SuspectsItem()
                    item['name'] = name
                    item['link'] = clink
                    item['location'] = None
                    item['ident'] = response.meta['lid']
                    if role1 == None:
                        item['role'] = None
                    else:
                        item['role'] = role1
                    if firm1 == None:
                        if len(cleaned_str) > 100:
                            print(">>Cleaned string too long for db. Reducing to: ", cleaned_str[:99])
                            item['firm'] = cleaned_str[:99]
                        else:
                            item['firm'] = cleaned_str
                    else:
                        item['firm'] = firm1
                    score = score_name(item['name'], db_name)
                    if score > 80:
                        item['status'] = 'Success'
                        yield item
                    else:
                        yield None
