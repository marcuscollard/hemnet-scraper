# -*- coding: utf-8 -*-

import re
import json
import scrapy
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy_playwright.page import PageMethod
from twisted.internet.error import TimeoutError, TCPTimedOutError

from sqlalchemy.orm import sessionmaker

from hemnet.models import (
    HemnetItem as HemnetSQL,
    HemnetCompItem as HemnetCompSQL,
    db_connect,
    create_hemnet_table
)


class HemnetSpider(scrapy.Spider):
    name = 'hemnetcompspider'
    rotate_user_agent = True

    def __init__(self, use_browser='1', *args, **kwargs):
        super(HemnetSpider, self).__init__(*args, **kwargs)
        self.use_browser = str(use_browser).lower() in ('1', 'true', 'yes', 'y')
        self.playwright_page_methods = [
            PageMethod("wait_for_load_state", "networkidle"),
            PageMethod("wait_for_timeout", 1000),
        ]
        engine = db_connect()
        create_hemnet_table(engine)
        self.session = sessionmaker(bind=engine)()

    def _make_request(self, url, callback, errback=None, meta=None):
        meta = dict(meta or {})
        if self.use_browser:
            meta.setdefault("playwright", True)
            meta.setdefault("playwright_context", "default")
            meta.setdefault("playwright_page_methods", self.playwright_page_methods)
        return scrapy.Request(url, callback, errback=errback, meta=meta)

    def _write_err(self, code, url):
        with open(self.name + '_err.txt', 'a') as f:
            f.write('{}: {}\n'.format(code, url))

    def download_err_back(self, failure):
        if failure.check(HttpError):
            response = failure.value.response
            self._write_err(response.status, response.url)
        elif failure.check(TimeoutError, TCPTimedOutError):
            request = failure.request
            self._write_err('TimeoutError', request.url)
        else:
            request = failure.request
            self._write_err('Other', request.url)

    def start_requests(self):
        session = self.session
        salda_items = session.query(HemnetSQL.hemnet_id, HemnetSQL.url).all()
        comp_ids = [i for i, in session.query(HemnetCompSQL.salda_id).all()]
        for salda_id, url in salda_items:
            if salda_id not in comp_ids:
                yield self._make_request(url, self.parse_salda,
                                         errback=self.download_err_back,
                                         meta={'salda_id': salda_id})

    def parse_salda(self, response):
        prev_page_url = response.css('link[rel=prev]::attr(href)')\
            .extract_first()
        pattern = r'coordinate.*\[(\d{2}\.\d+\,\d{2}\.\d+)\]'
        g = re.search(pattern, response.text)
        salda_id = response.meta['salda_id']
        try:
            lat, lon = map(float, g.group(1).split(','))
        except:
            lat, lon = None, None
        if prev_page_url:
            yield self._make_request(prev_page_url, self.parse_detail_page,
                                     meta={'lat': lat, 'lon': lon, 'salda_id': salda_id},
                                     errback=self.download_err_back)

    def parse_detail_page(self, response):
        pattern = r'dataLayer\s*=\s*(\[[\s\S]*?\]);'
        g = re.search(pattern, response.text)
        try:
            d = json.loads(g.group(1))
        except:
            self._write_err('JSONError', response.url)
        else:
            prop = next((el for el in d if u'property' in el), None)['property']

            item = HemnetCompItem()

            item['url'] = response.url

            item['lattitude'] = response.meta['lat']
            item['longitude'] = response.meta['lon']

            item['salda_id'] = response.meta['salda_id']

            locations = prop.get('locations', {})

            item['city'] = locations.get('city')
            item['postal_city'] = locations.get('postal_city')
            item['district'] = locations.get('district')
            item['country'] = locations.get('country')
            item['region'] = locations.get('region')
            item['municipality'] = locations.get('municipality')
            item['street'] = locations.get('street')

            item['offers_selling_price'] = prop.get('offers_selling_price')
            item['living_area'] = prop.get('living_area')
            item['rooms'] = prop.get('rooms')
            item['hemnet_id'] = prop.get('id')
            item['cost_per_year'] = prop.get('driftkostnad')
            item['new_production'] = prop.get('new_production')
            item['broker_firm'] = prop.get('broker_firm')
            item['upcoming_open_houses'] = prop.get('upcoming_open_houses')
            item['location'] = prop.get('location')
            item['home_swapping'] = prop.get('home_swapping')
            item['has_price_change'] = prop.get('has_price_change')
            item['status'] = prop.get('status')
            item['price'] = prop.get('price')
            item['monthly_fee'] = prop.get('borattavgift')
            item['main_location'] = prop.get('main_location')
            item['publication_date'] = prop.get('publication_date')
            item['has_active_toplisting'] = prop.get('has_active_toplisting')
            item['images_count'] = prop.get('images_count')
            item['item_type'] = prop.get('item_type')
            item['price_per_m2'] = prop.get('price_per_m2')
            item['street_address'] = prop.get('street_address')

            yield item
