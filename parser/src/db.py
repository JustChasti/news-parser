import datetime
from time import sleep

from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import DateTime, Date
from sqlalchemy.engine.url import URL
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from loguru import logger

from config import base_user, base_pass, base_name, base_host, base_port

Base = declarative_base()


class News(Base):
    __tablename__ = 'news'
    id = Column(Integer, primary_key=True)
    link = Column(String(512), nullable=False)
    title = Column(String(), nullable=False)
    text = Column(String(), nullable=False)
    publish_date = Column(Date, nullable=True)
    parsed_date = Column(DateTime, default=datetime.datetime.now())


data = {
    'drivername': 'postgresql+psycopg2',
    'host': base_host,
    'port': base_port,
    'username': base_user,
    'password': base_pass,
    'database': base_name,
}


while True:
    try:
        engine = create_engine(URL(**data))
        engine.connect()
        Base.metadata.create_all(engine)
        Session = sessionmaker(bind=engine)
        break
    except Exception as e:
        print(e)
        sleep(5)


Session = sessionmaker(bind=engine)
