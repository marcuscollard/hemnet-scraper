# -*- coding: utf-8 -*-

import re
import json
from pathlib import Path
from datetime import datetime
import scrapy

from urllib.parse import urlparse, urljoin, urlencode

from itertools import product

from scrapy import Selector
from scrapy.spidermiddlewares.httperror import HttpError
from scrapy_playwright.page import PageMethod
from twisted.internet.error import TimeoutError, TCPTimedOutError
from sqlalchemy.orm import sessionmaker

from hemnet.items import HemnetItem, HemnetCompItem
from hemnet.models import (
    HemnetItem as HemnetSQL,
    db_connect,
    create_hemnet_table
)


BASE_URL = 'https://www.hemnet.se/bostader?published_since=3d&location_ids%5B%5D=17744'

location_ids = [17744]
item_types = ['radhus', 'bostadsratt', 'villa']
rooms = [None, 1, 1.5, 2, 2.5, 3, 3.5, 4, 5, 100]
living_area = [None, 20, 25, 30, 35, 40, 45, 50, 60, 70, 80, 500]
fee = [None, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 5000, 7000, 30000]


def url_queries(sold_age):
    d_ = {
        'location_ids': location_ids,
        'item_types': item_types,
        'rooms': rooms,
        'living_area': zip(living_area, living_area[1:]),
        'fee': zip(fee, fee[1:]),
    }

    def _encode_query(params):
        url_query = {}
        url_query['location_ids[]'] = params['location_ids']
        url_query['item_types[]'] = params['item_types']

        rooms_value = params['rooms']
        if rooms_value is not None:
            url_query['rooms_min'] = rooms_value
            url_query['rooms_max'] = rooms_value

        living_min, living_max = params['living_area']
        if living_min is not None:
            url_query['living_area_min'] = living_min
        if living_max is not None:
            url_query['living_area_max'] = living_max

        fee_min, fee_max = params['fee']
        if fee_min is not None:
            url_query['fee_min'] = fee_min
        if fee_max is not None:
            url_query['fee_max'] = fee_max

        url_query['sold_age'] = sold_age
        return urlencode(url_query)

    param_list = [dict(zip(d_, v)) for v in product(*d_.values())]
    return [_encode_query(p) for p in param_list]


def start_urls(sold_age):
    return [BASE_URL]


def extract_listing_urls(response):
    selectors = [
        '#search-results li > div > a::attr("href")',
        'a[data-test="search-result-item-link"]::attr("href")',
        'a[data-testid="listing-card-link"]::attr("href")',
        'a[data-testid="search-result-item-link"]::attr("href")',
        'a.listing-card__link::attr("href")',
        'a.hcl-link::attr("href")',
    ]

    urls = []
    for selector in selectors:
        urls.extend(response.css(selector).getall())

    if not urls:
        for href in response.css('a::attr("href")').getall():
            if not href:
                continue
            if "/bostad/" not in href and "/salda/" not in href:
                continue
            if not re.search(r"-\d+$", href.strip("/")):
                continue
            urls.append(href)

    # Preserve order while deduplicating.
    return list(dict.fromkeys(urls))


def _extract_next_data(response):
    script = response.css('script#__NEXT_DATA__::text').get()
    if not script:
        return None
    try:
        return json.loads(script)
    except Exception:
        return None


def _find_property_data(node):
    if isinstance(node, dict):
        if "sold_property" in node and isinstance(node["sold_property"], dict):
            return node["sold_property"]
        if "soldProperty" in node and isinstance(node["soldProperty"], dict):
            return node["soldProperty"]
        if "property" in node and isinstance(node["property"], dict):
            if "id" in node["property"]:
                return node["property"]
        if "id" in node and (
            "selling_price" in node
            or "sellingPrice" in node
            or "sold_at_date" in node
            or "soldAtDate" in node
        ):
            return node
        for value in node.values():
            found = _find_property_data(value)
            if found:
                return found
    elif isinstance(node, list):
        for item in node:
            found = _find_property_data(item)
            if found:
                return found
    return None


def _normalize_props(props):
    if not props:
        return {}
    mapping = {
        "sellingPrice": "selling_price",
        "soldAtDate": "sold_at_date",
        "livingArea": "living_area",
        "streetAddress": "street_address",
        "brokerAgency": "broker_agency",
        "pricePerSqm": "price_per_square_meter",
        "pricePerSquareMeter": "price_per_square_meter",
        "askingPrice": "price",
    }
    normalized = dict(props)
    for camel, snake in mapping.items():
        if camel in normalized and snake not in normalized:
            normalized[snake] = normalized.get(camel)
    return normalized


def _extract_active_listing(next_data):
    state = next_data.get('props', {}).get('pageProps', {}).get('__APOLLO_STATE__', {})
    if not isinstance(state, dict):
        return None, {}
    for key, value in state.items():
        if key.startswith("ActivePropertyListing:") and isinstance(value, dict):
            return value, state
    return None, state


def _resolve_ref(ref, state):
    if isinstance(ref, dict):
        key = ref.get("__ref")
        if key:
            return state.get(key, {})
    return {}


def _money_amount(value):
    if isinstance(value, dict):
        return value.get("amount")
    return value


def _resolve_location_name(ref, state):
    data = _resolve_ref(ref, state)
    if not data:
        return None
    return data.get("fullName") or data.get("name")


def _resolve_locations(refs, state):
    if not refs:
        return None
    names = []
    for ref in refs:
        name = _resolve_location_name(ref, state)
        if name:
            names.append(name)
    return names or None


def _parse_datetime(value):
    if not value:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(value)
        except Exception:
            return None
    if isinstance(value, str):
        try:
            if value.endswith("Z"):
                value = value.replace("Z", "+00:00")
            return datetime.fromisoformat(value)
        except Exception:
            return None
    return None


class HemnetSpider(scrapy.Spider):
    name = 'hemnetspider'
    rotate_user_agent = True

    def __init__(self, sold_age='1m', use_browser='1', *args, **kwargs):
        super(HemnetSpider, self).__init__(*args, **kwargs)
        self.sold_age = sold_age
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

    def start_requests(self):
        for url in start_urls(self.sold_age):
            yield self._make_request(url, self.parse,
                                     errback=self.download_err_back)

    def _write_err(self, code, url):
        with open(self.name + '_err.txt', 'a') as f:
            f.write('{}: {}\n'.format(code, url))

    def _save_debug_html(self, response, reason):
        slug = urlparse(response.url).path.strip("/").replace("/", "_")
        if not slug:
            slug = "listing"
        safe_slug = re.sub(r"[^a-zA-Z0-9_-]", "_", slug)
        out_dir = Path(__file__).resolve().parents[2] / "debug_html"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{safe_slug}_{reason}.html"
        if not out_path.exists():
            out_path.write_text(response.text, encoding="utf-8")

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

    def parse(self, response):
        urls = extract_listing_urls(response)
        for url in urls:
            url = urljoin(response.url, url)
            try:
                hemnet_id = get_hemnet_id(url)
            except Exception:
                self._write_err('BadUrl', url)
                continue
            session = self.session
            q = session.query(HemnetSQL)\
                .filter(HemnetSQL.hemnet_id == hemnet_id)
            if not session.query(q.exists()).scalar():
                yield self._make_request(url, self.parse_detail_page,
                                         errback=self.download_err_back)

        next_href = response.css('a.next_page::attr("href")').extract_first()
        if next_href:
            next_url = urljoin(response.url, next_href)
            yield self._make_request(next_url, self.parse,
                                     errback=self.download_err_back)

    @staticmethod
    def _get_layer_data(response):
        pattern = r'dataLayer\s*=\s*(\[[\s\S]*?\]);'
        g = re.search(pattern, response.text)
        if not g:
            raise ValueError("dataLayer not found")
        d = json.loads(g.group(1))
        return d

    def parse_detail_page(self, response):

        props = {}
        try:
            layer_data = self._get_layer_data(response)
        except Exception:
            self._write_err('JSONError', response.url)
            layer_data = []

        if layer_data:
            sold_entry = next(
                (el for el in layer_data if u'sold_property' in el), None
            )
            if sold_entry:
                props = sold_entry.get('sold_property', {})
            else:
                prop_entry = next(
                    (el for el in layer_data if u'property' in el), None
                )
                if prop_entry:
                    props = prop_entry.get('property', {})

        next_data = None
        active_listing = None
        apollo_state = {}
        if not props:
            next_data = _extract_next_data(response)
            if next_data:
                active_listing, apollo_state = _extract_active_listing(next_data)
                if not active_listing:
                    props = _find_property_data(next_data) or {}
                    props = _normalize_props(props)

        if not props and not active_listing:
            self._write_err('NoProps', response.url)
            self._save_debug_html(response, "no_props")
            return

        item = HemnetItem()

        broker_sel = response.css('.broker-contact-card__information')
        broker_node = broker_sel[0] if broker_sel else None
        property_attributes = get_property_attributes(response)

        item['url'] = response.url
        slug = urlparse(response.url).path.split('/')[-1]
        if active_listing:
            item['hemnet_id'] = int(active_listing.get('id') or get_hemnet_id(response.url))
            housing_form = active_listing.get('housingForm') or {}
            item['type'] = housing_form.get('name') or slug.split('-')[0]
        else:
            item['hemnet_id'] = props.get('id') or get_hemnet_id(response.url)
            item['type'] = slug.split('-')[0]

        raw_rooms = (
            active_listing.get('numberOfRooms') if active_listing else props.get('rooms')
        )
        try:
            if raw_rooms is not None:
                item['rooms'] = float(raw_rooms)
        except Exception:
            pass

        fee = None
        if active_listing:
            fee = _money_amount(active_listing.get('fee'))
        else:
            try:
                fee = int(property_attributes.get(u'Avgift/månad', '')
                          .replace(u' kr/m\xe5n', '').replace(u'\xa0', u''))
            except ValueError:
                fee = None
        item['monthly_fee'] = fee

        try:
            living_area = (
                active_listing.get('livingArea')
                if active_listing
                else props.get('living_area')
            )
            if living_area is not None:
                item['square_meters'] = float(living_area)
        except Exception:
            pass

        cost = None
        if active_listing:
            cost = _money_amount(active_listing.get('runningCosts'))
        else:
            try:
                cost = int(property_attributes.get(u'Driftskostnad', '')
                           .replace(u' kr/\xe5r', '').replace(u'\xa0', u''))
            except Exception:
                cost = None
        item['cost_per_year'] = cost

        # can be '2008-2009'
        if active_listing and active_listing.get('legacyConstructionYear'):
            item['year'] = str(active_listing.get('legacyConstructionYear'))
        else:
            item['year'] = property_attributes.get(u'Byggår', '')

        try:
            association = property_attributes.get(u'Förening').strip()
        except:
            association = None
        item['association'] = association

        try:
            lot_size = int(property_attributes.get(u'Tomtarea')
                           .strip().rsplit(' ')[0].replace(u'\xa0', ''))
        except:
            lot_size = None
        item['lot_size'] = lot_size

        try:
            biarea = int(property_attributes.get(u'Biarea').strip()
                         .rsplit(' ')[0].replace(u'\xa0', ''))
        except:
            biarea = None
        item['biarea'] = biarea

        if active_listing:
            broker = _resolve_ref(active_listing.get('broker'), apollo_state)
            agency = _resolve_ref(active_listing.get('brokerAgency'), apollo_state)
            item['broker_name'] = broker.get('name', '') or ''
            item['broker_phone'] = broker.get('phoneNumber', '') or ''
            item['broker_email'] = broker.get('email', '') or ''
            item['broker_firm'] = agency.get('name')
            item['broker_firm_phone'] = agency.get('phoneNumber')
        elif broker_node is not None:
            broker_name = broker_node.css('strong::text').extract_first()
            item['broker_name'] = broker_name.strip() if broker_name else ""
            broker_links = broker_node.css(
                'a.broker-contact__link::attr("href")'
            ).extract()
            if broker_links:
                item['broker_phone'] = strip_phone(broker_links[0])
            if len(broker_links) > 1:
                try:
                    item['broker_email'] = decode_email(broker_links[1]).split('?')[0]
                except Exception:
                    pass

            item['broker_firm'] = props.get('broker_agency')

            broker_firm_phone = None
            firm_links = broker_node.css('.phone-number::attr("href")').extract()
            if len(firm_links) > 1:
                broker_firm_phone = strip_phone(firm_links[1])
            item['broker_firm_phone'] = broker_firm_phone

        if active_listing:
            item['listing_url'] = active_listing.get('listingHemnetUrl')
            item['title'] = active_listing.get('title')
            item['description'] = active_listing.get('description')
            item['housing_form'] = (active_listing.get('housingForm') or {}).get('name')
            item['tenure'] = (active_listing.get('tenure') or {}).get('name')
            item['days_on_hemnet'] = active_listing.get('daysOnHemnet')
            item['is_new_construction'] = active_listing.get('isNewConstruction')
            item['is_project'] = active_listing.get('isProject')
            item['is_project_unit'] = active_listing.get('isProjectUnit')
            item['is_upcoming'] = active_listing.get('isUpcoming')
            item['is_foreclosure'] = active_listing.get('isForeclosure')
            item['is_bidding_ongoing'] = active_listing.get('isBiddingOngoing')
            item['bidding_started'] = active_listing.get('biddingStarted')
            item['published_at'] = _parse_datetime(active_listing.get('publishedAt'))
            item['times_viewed'] = active_listing.get('timesViewed')
            item['verified_bidding'] = active_listing.get('verifiedBidding')
            item['listing_broker_url'] = active_listing.get('listingBrokerUrl')
            item['listing_broker_gallery_url'] = active_listing.get('listingBrokerGalleryUrl')
            item['post_code'] = active_listing.get('postCode')
            item['municipality_name'] = _resolve_location_name(
                active_listing.get('municipality'), apollo_state
            )
            item['region_name'] = _resolve_location_name(
                active_listing.get('region'), apollo_state
            )
            item['county_name'] = _resolve_location_name(
                active_listing.get('county'), apollo_state
            )
            item['districts'] = _resolve_locations(
                active_listing.get('districts'), apollo_state
            )
            item['labels'] = active_listing.get('labels')
            item['relevant_amenities'] = active_listing.get('relevantAmenities')
            item['listing_collection_ids'] = active_listing.get('listingCollectionIds')
            item['breadcrumbs'] = active_listing.get('breadcrumbs')
            item['ad_targeting'] = active_listing.get('adTargeting')
            item['attachments'] = active_listing.get('attachments')
            item['images'] = active_listing.get('images({"limit":300})')
            item['images_preview'] = active_listing.get('images({"limit":0})')
            item['thumbnail'] = active_listing.get('thumbnail')
            item['photo_attribution'] = active_listing.get('photoAttribution')
            item['price_change'] = active_listing.get('priceChange')
            item['upcoming_open_houses'] = active_listing.get('upcomingOpenHouses')
            item['floor_plan_images'] = active_listing.get('floorPlanImages')
            item['video_attachment'] = active_listing.get('attachment({"type":"VIDEO"})')
            item['three_d_attachment'] = active_listing.get('attachment({"type":"THREE_D"})')
            item['energy_classification'] = active_listing.get('energyClassification')
            item['active_package'] = active_listing.get('activePackage')
            item['seller_package_recommendation'] = active_listing.get('sellerPackageRecommendation')
            housing_cooperative = _resolve_ref(
                active_listing.get('housingCooperative'), apollo_state
            )
            item['housing_cooperative'] = housing_cooperative or None
            if housing_cooperative:
                item['housing_cooperative_name'] = housing_cooperative.get('name')
            item['yearly_arrende_fee'] = _money_amount(active_listing.get('yearlyArrendeFee'))
            item['yearly_leasehold_fee'] = _money_amount(active_listing.get('yearlyLeaseholdFee'))
            item['land_area'] = active_listing.get('landArea')
            item['formatted_land_area'] = active_listing.get('formattedLandArea')
            item['formatted_living_area'] = active_listing.get('formattedLivingArea')
            item['formatted_supplemental_area'] = active_listing.get('formattedSupplementalArea')
            item['supplemental_area'] = active_listing.get('supplementalArea')
            item['formatted_floor'] = active_listing.get('formattedFloor')
            item['closest_water_distance_meters'] = active_listing.get('closestWaterDistanceMeters')
            item['coastline_distance_meters'] = active_listing.get('coastlineDistanceMeters')
            item['raw_listing'] = active_listing
            item['raw_apollo_state'] = apollo_state
            item['broker_raw'] = broker or None
            item['broker_agency_raw'] = agency or None

            item['price'] = _money_amount(active_listing.get('askingPrice'))
            item['asked_price'] = item.get('price')
            item['price_per_square_meter'] = _money_amount(
                active_listing.get('squareMeterPrice')
            )
            item['sold_date'] = None
            item['address'] = active_listing.get('streetAddress') or ''
            item['geographic_area'] = active_listing.get('area') or ''
        else:
            item['price'] = props.get('selling_price')
            item['asked_price'] = props.get('price')
            item['price_per_square_meter'] = props.get('price_per_square_meter')
            item['sold_date'] = props.get('sold_at_date')
            item['address'] = props.get('street_address')
            item['geographic_area'] = props.get('location')
        lat, lon = extract_coords(response)
        item['latitude'] = lat
        item['longitude'] = lon
        yield item

        prev_page_url = response.css('link[rel=prev]::attr(href)')\
            .extract_first()

        if prev_page_url:
            yield self._make_request(prev_page_url, self.parse_prev_page,
                                     meta={'lat': lat, 'lon': lon,
                                           'salda_id': props.get('id')},
                                     errback=self.download_err_back)

    def parse_prev_page(self, response):
        try:
            layer_data = self._get_layer_data(response)
        except:
            self._write_err('JSONError', response.url)
        else:
            prop = next((e for e in layer_data if u'property' in e),
                        {}).get('property', {})

            item = HemnetCompItem()

            item['url'] = response.url

            item['lattitude'] = response.meta['lat']
            item['longitude'] = response.meta['lon']

            item['salda_id'] = response.meta['salda_id']

            locations = prop.get('locations', {})

            item['city'] = locations.get('city')
            item['district'] = locations.get('district')
            item['postal_city'] = locations.get('postal_city')
            item['country'] = locations.get('country')
            item['municipality'] = locations.get('municipality')
            item['region'] = locations.get('county')
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


def extract_coords(response):
    coord_pattern = r'coordinate.*\[(\d{2}\.\d+\,\d{2}\.\d+)\]'
    g = re.search(coord_pattern, response.text)
    try:
        lat, lon = map(float, g.group(1).split(','))
    except:
        lat, lon = None, None
    return lat, lon


def cfDecodeEmail(encodedString):
    r = int(encodedString[:2],16)
    email = ''.join([chr(int(encodedString[i:i+2], 16) ^ r) for i in
                     range(2, len(encodedString), 2)])
    return email


def decode_email(encoded_str):
    # u'/cdn-cgi/l/email-protection#b2d8d7c1c2d7c09cdead...'
    try:
        decoded = cfDecodeEmail(encoded_str.split('#')[-1])
    except:
        decoded = None
    return decoded


def get_hemnet_id(url):
    slug = urlparse(url).path.split('/')[-1]
    return int(slug.split('-')[-1])


def get_property_attributes(response):
    a = response.css('.sold-property__attributes > dt::text').extract()
    x = [x.strip() for x in a]
    b = response.css('.sold-property__attributes > dd::text').extract()

    return dict(zip(x, b))


def strip_phone(phone_text):
    if phone_text:
        return phone_text.replace(u'tel:', u'')
    else:
        return u''
