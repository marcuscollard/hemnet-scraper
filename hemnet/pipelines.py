# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: http://doc.scrapy.org/en/latest/topics/item-pipeline.html

import json
import os
from urllib.error import URLError
from urllib.request import Request, urlopen

from sqlalchemy.orm import sessionmaker
from .models import db_connect, create_hemnet_table
from .models import HemnetItem as HemnetDBItem
from .models import HemnetCompItem as HemnetCompDBItem
from .items import HemnetItem


class HemnetPipeline(object):
    def __init__(self):
        engine = db_connect()
        create_hemnet_table(engine)
        self.Session = sessionmaker(bind=engine)
        self.store_images = os.getenv("HEMNET_STORE_IMAGES", "1").lower() not in (
            "0",
            "false",
            "no",
        )
        self.max_image_bytes = int(os.getenv("HEMNET_MAX_IMAGE_BYTES", "10000000"))
        self.image_user_agent = os.getenv(
            "HEMNET_IMAGE_UA",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
        )

    def _load_json(self, value):
        if value is None or isinstance(value, (dict, list)):
            return value
        if isinstance(value, str):
            try:
                return json.loads(value)
            except Exception:
                return None
        return None

    def _extract_image_url(self, image):
        if not isinstance(image, dict):
            return None
        preferred = ["ITEMGALLERY_L", "ITEMGALLERY_CUT", "ITEMGALLERY_M", "ITEMGALLERY_S"]
        for fmt in preferred:
            key = f'url({{"format":"{fmt}"}})'
            if key in image and isinstance(image[key], str):
                return image[key]
        for key, value in image.items():
            if key.startswith("url(") and isinstance(value, str):
                return value
        return None

    def _select_image_urls(self, item):
        images_payload = self._load_json(item.get("images"))
        images = []
        if isinstance(images_payload, dict):
            images = images_payload.get("images") or []
        elif isinstance(images_payload, list):
            images = images_payload

        main_url = None
        floor_url = None
        for image in images:
            url = self._extract_image_url(image)
            if not url:
                continue
            labels = image.get("labels") or []
            is_floor = "FLOOR_PLAN" in labels
            if is_floor and not floor_url:
                floor_url = url
            elif not is_floor and not main_url:
                main_url = url
            if main_url and floor_url:
                break

        if not main_url:
            thumbnail = self._load_json(item.get("thumbnail"))
            main_url = self._extract_image_url(thumbnail)

        if not floor_url:
            floor_images = self._load_json(item.get("floor_plan_images")) or []
            if isinstance(floor_images, list):
                for image in floor_images:
                    url = self._extract_image_url(image)
                    if url:
                        floor_url = url
                        break

        return main_url, floor_url

    def _download_image(self, url):
        if not url:
            return None, None
        try:
            req = Request(
                url,
                headers={
                    "User-Agent": self.image_user_agent,
                    "Accept": "image/avif,image/webp,image/*,*/*",
                },
            )
            with urlopen(req, timeout=20) as response:
                content_type = response.headers.get("Content-Type")
                content_length = response.headers.get("Content-Length")
                if content_length:
                    try:
                        if int(content_length) > self.max_image_bytes:
                            return None, None
                    except ValueError:
                        pass
                data = response.read(self.max_image_bytes + 1)
                if len(data) > self.max_image_bytes:
                    return None, None
                return data, content_type
        except (URLError, TimeoutError, ValueError):
            return None, None

    def _attach_images(self, item):
        if item.get("main_image_bytes") or item.get("floorplan_image_bytes"):
            return
        main_url, floor_url = self._select_image_urls(item)
        if main_url:
            data, content_type = self._download_image(main_url)
            if data:
                item["main_image_url"] = main_url
                item["main_image_bytes"] = data
                item["main_image_mime"] = content_type
        if floor_url:
            data, content_type = self._download_image(floor_url)
            if data:
                item["floorplan_image_url"] = floor_url
                item["floorplan_image_bytes"] = data
                item["floorplan_image_mime"] = content_type

    def process_item(self, item, spider):
        session = self.Session()
        if isinstance(item, HemnetItem):
            if self.store_images:
                self._attach_images(item)
            deal = HemnetDBItem(**item)
        else:
            deal = HemnetCompDBItem(**item)

        try:
            session.add(deal)
            session.commit()
        except:
            session.rollback()
            raise
        finally:
            session.close()

        return item
