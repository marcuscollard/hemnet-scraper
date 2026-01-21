from datetime import datetime

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    Date,
    Boolean,
    BigInteger,
    DateTime,
    Text,
    JSON,
    LargeBinary,
)
from sqlalchemy.engine.url import URL
from sqlalchemy.ext.declarative import declarative_base

from . import settings

DeclarativeBase = declarative_base()


def db_connect():
    database_url = getattr(settings, "DATABASE_URL", None)
    if database_url:
        return create_engine(database_url)
    return create_engine(URL(**settings.DATABASE))


def create_hemnet_table(engine):
    DeclarativeBase.metadata.create_all(engine)


class HemnetItem(DeclarativeBase):
    __tablename__ = "hemnet_items"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    hemnet_id = Column(BigInteger, index=True)

    url = Column(String)

    broker_name = Column(String, default='')
    broker_phone = Column(String, default='')
    broker_email = Column(String, default='', index=True)

    broker_firm = Column(String, nullable=True)
    broker_firm_phone = Column(String, nullable=True)

    sold_date = Column(Date, nullable=True)

    price_per_square_meter = Column(Float, nullable=True)
    price = Column(Integer, nullable=True)
    asked_price = Column(Integer, nullable=True)
    price_trend_flat = Column(Integer, nullable=True)
    price_trend_percentage = Column(Integer, nullable=True)

    rooms = Column(Float, nullable=True)
    monthly_fee = Column(Integer, nullable=True)
    square_meters = Column(Float, nullable=True)
    cost_per_year = Column(Integer, nullable=True)
    year = Column(String, default='')
    type = Column(String, default='')
    association = Column(String, nullable=True)
    lot_size = Column(Integer, nullable=True)
    biarea = Column(Integer, nullable=True)

    address = Column(String, default='')
    geographic_area = Column(String, default='')
    collected_at = Column(Date, default=datetime.now())

    listing_url = Column(String, nullable=True)
    title = Column(String, nullable=True)
    description = Column(Text, nullable=True)
    housing_form = Column(String, nullable=True)
    tenure = Column(String, nullable=True)
    days_on_hemnet = Column(Integer, nullable=True)
    is_new_construction = Column(Boolean, nullable=True)
    is_project = Column(Boolean, nullable=True)
    is_project_unit = Column(Boolean, nullable=True)
    is_upcoming = Column(Boolean, nullable=True)
    is_foreclosure = Column(Boolean, nullable=True)
    is_bidding_ongoing = Column(Boolean, nullable=True)
    bidding_started = Column(Boolean, nullable=True)
    published_at = Column(DateTime, nullable=True)
    times_viewed = Column(Integer, nullable=True)
    verified_bidding = Column(JSON, nullable=True)
    listing_broker_url = Column(String, nullable=True)
    listing_broker_gallery_url = Column(String, nullable=True)
    post_code = Column(String, nullable=True)
    municipality_name = Column(String, nullable=True)
    region_name = Column(String, nullable=True)
    county_name = Column(String, nullable=True)
    districts = Column(JSON, nullable=True)
    labels = Column(JSON, nullable=True)
    relevant_amenities = Column(JSON, nullable=True)
    listing_collection_ids = Column(JSON, nullable=True)
    breadcrumbs = Column(JSON, nullable=True)
    ad_targeting = Column(JSON, nullable=True)
    attachments = Column(JSON, nullable=True)
    images = Column(JSON, nullable=True)
    images_preview = Column(JSON, nullable=True)
    thumbnail = Column(JSON, nullable=True)
    photo_attribution = Column(JSON, nullable=True)
    price_change = Column(JSON, nullable=True)
    upcoming_open_houses = Column(JSON, nullable=True)
    floor_plan_images = Column(JSON, nullable=True)
    video_attachment = Column(JSON, nullable=True)
    three_d_attachment = Column(JSON, nullable=True)
    energy_classification = Column(JSON, nullable=True)
    active_package = Column(JSON, nullable=True)
    seller_package_recommendation = Column(JSON, nullable=True)
    housing_cooperative = Column(JSON, nullable=True)
    housing_cooperative_name = Column(String, nullable=True)
    yearly_arrende_fee = Column(Integer, nullable=True)
    yearly_leasehold_fee = Column(Integer, nullable=True)
    land_area = Column(Float, nullable=True)
    formatted_land_area = Column(String, nullable=True)
    formatted_living_area = Column(String, nullable=True)
    formatted_supplemental_area = Column(String, nullable=True)
    supplemental_area = Column(Float, nullable=True)
    formatted_floor = Column(String, nullable=True)
    closest_water_distance_meters = Column(Integer, nullable=True)
    coastline_distance_meters = Column(Integer, nullable=True)
    raw_listing = Column(JSON, nullable=True)
    raw_apollo_state = Column(JSON, nullable=True)
    broker_raw = Column(JSON, nullable=True)
    broker_agency_raw = Column(JSON, nullable=True)
    main_image_url = Column(String, nullable=True)
    main_image_bytes = Column(LargeBinary, nullable=True)
    main_image_mime = Column(String, nullable=True)
    floorplan_image_url = Column(String, nullable=True)
    floorplan_image_bytes = Column(LargeBinary, nullable=True)
    floorplan_image_mime = Column(String, nullable=True)


class HemnetCompItem(DeclarativeBase):
    __tablename__ = "hemnet_comp_items"

    id = Column(BigInteger, primary_key=True, autoincrement=True)

    salda_id = Column(BigInteger, index=True)
    hemnet_id = Column(BigInteger, index=True)

    url = Column(String)

    lattitude = Column(Float, nullable=True)
    longitude = Column(Float, nullable=True)

    city = Column(String, nullable=True)
    postal_city = Column(String, nullable=True)
    district = Column(String, nullable=True)
    country = Column(String, nullable=True)
    region = Column(String, nullable=True)
    municipality = Column(String, nullable=True)
    street = Column(String, nullable=True)

    location = Column(String, nullable=True)
    main_location = Column(String, nullable=True)
    street_address = Column(String, nullable=True)

    offers_selling_price = Column(Boolean)
    living_area = Column(Float)
    rooms = Column(Float, nullable=True)

    broker_firm = Column(String, nullable=True)

    new_production = Column(Boolean)
    upcoming_open_houses = Column(Boolean, default=False)
    home_swapping = Column(Boolean, nullable=True)
    has_price_change = Column(Boolean, nullable=True)
    has_active_toplisting = Column(Boolean, nullable=True)

    status = Column(String, nullable=True)
    price = Column(Integer)
    cost_per_year = Column(Integer, nullable=True)
    monthly_fee = Column(Integer, nullable=True)
    publication_date = Column(Date, nullable=True)
    images_count = Column(Integer, nullable=True)
    item_type = Column(String, nullable=True)
    price_per_m2 = Column(Integer, nullable=True)

    collected_at = Column(Date, default=datetime.now())
