import datetime
import requests
import time
import json
from threading import Thread

from loguru import logger
from bs4 import BeautifulSoup
from newspaper import Article

import config
from src import db


class DuplicateNews(Exception):
    pass


def parse_page_custom(link, title=None, text=None, publish_date=None):
    session = db.Session()
    if session.query(db.News).filter(db.News.link == link).first():
        session.close()
        raise DuplicateNews('This link already in database')
    try:
        article = Article(link, language='ru')
        article.download()
        article.parse()
    except Exception as e:
        logger.warning('i cant download the article')
    _article = {
        "link": link,
        "title": title if title else article.title,
        "text": text if text else article.text,
        "publish_date": publish_date if publish_date else article.publish_date,
        "parsed_date": datetime.datetime.now(),
    }
    session.add(db.News(**_article))
    session.commit()
    session.close()
    logger.info('Page parsed')


def parse_msknews():
    try:
        page = requests.get('http://msk-news.net/').text
        soup = BeautifulSoup(page, "html.parser")
        sitehead = soup.find('div', {"id": "menu"})
        categories = sitehead.find_all('a')
        for category in categories:
            parse_msknews_category(category['href'])
        logger.info('Site parsed')
    except Exception as e:
        logger.exception(e)
        logger.warning(' Вероятно на сайте произошло обновление, или ваш ip был заблокирован')


def parse_msknews_category(url):
    deep_counter = 0
    page = requests.get(url).text
    page_count = 1
    soup = BeautifulSoup(page, "html.parser")
    column = soup.find('div', {"class": "col2"})

    while column:
        both_column_parsed = 0
        column = soup.find('div', {"class": "col2"})
        pages = column.find_all('div', {"class": "post_title"})
        for element in pages:
            page = element.find('a', {"class": "vh"})
            deep_counter += 1
            try:
                parse_page_custom(page['href'])
            except DuplicateNews as e:
                logger.warning(e)
                both_column_parsed += 1

        column = soup.find('div', {"class": "col2 col2b"})
        pages = column.find_all('div', {"class": "post_title"})
        for element in pages:
            page = element.find('a', {"class": "vh"})
            deep_counter += 1
            try:
                parse_page_custom(page['href'])
            except DuplicateNews as e:
                logger.warning(e)
                both_column_parsed += 1

        if both_column_parsed < 2 and page_count <= 100 and deep_counter < config.max_deep_cat:
            pass
        else:
            logger.info('Category parsed')
            break
        page_count += 1
        page = requests.get(url + '/' + str(count)).text
        soup = BeautifulSoup(page, "html.parser")


def parse_msknovosti():
    try:
        page = requests.get('https://msknovosti.ru/').text
        soup = BeautifulSoup(page, "html.parser")
        sitehead = soup.find('div', {"class": "menu-main-container"})
        categories = sitehead.find_all('a')
        for category in categories:
            parse_msknovosti_category(category['href'])
        logger.info('Site parsed')
    except Exception as e:
        logger.exception(e)
        logger.warning(' Вероятно на сайте произошло обновление, или ваш ip был заблокирован')


def parse_msknovosti_category(url):
    count = 1
    deep_counter = 0
    page = requests.get(url).text
    soup = BeautifulSoup(page, "html.parser")
    element = soup.find('a', {"class": "page-numbers"})
    maxcount = int(element.find_next_sibling("a").text)
    Parsing_flag = True
    while count <= maxcount and deep_counter < config.max_deep_cat and Parsing_flag:
        count += 1
        column = soup.find_all('div', {"class": "post-card post-card--vertical w-animate"})
        flag = 0
        for element in column:
            deep_counter += 1
            try:
                parse_page_custom(element.find('a')['href'])
            except DuplicateNews as e:
                logger.warning(e)
                Parsing_flag = False
                break
        page = requests.get(url + '/page/' + str(count)).text
        soup = BeautifulSoup(page, "html.parser")
    logger.info('Category parsed')


def parse_mskiregion():
    try:
        Parsing_flag = True
        page_num = 1
        deep_counter = 0
        while Parsing_flag and deep_counter < config.max_deep:
            if page_num == 1:
                page = requests.get('https://msk.inregiontoday.ru/?cat=1').text
            else:
                page = requests.get('https://msk.inregiontoday.ru/?cat=1&paged='
                                    + str(page_num)).text
            page_num += 1
            soup = BeautifulSoup(page, "html.parser")
            page_counter = 1
            titels = soup.find_all('h2', {"class": "entry-title"})
            if not titels:
                flag = 1
            else:
                for title in titels:
                    deep_counter += 1
                    link = title.find('a')
                    try:
                        parse_page_custom(link['href'])
                    except DuplicateNews as e:
                        logger.warning(e)
                        Parsing_flag = False
                        break
        logger.info('Site parsed')
    except Exception as e:
        logger.exception(e)
        logger.warning(' Вероятно на сайте произошло обновление, или ваш ip был заблокирован')


def convert_date(post_date):
    if ':' in post_date:
        date = datetime.datetime.now().date()
    elif 'Вчера' in post_date:
        date = datetime.date.today() - datetime.timedelta(days=1)
    else:
        date = ''
        for i in post_date:
            if i >= '0' and i <= '9':
                date = date + i
        date = datetime.datetime.strptime(date, "%d%m%Y").date()
    return date


def parse_molnet():
    try:
        Parsing_flag = True
        page_num = 1
        deep_counter = 0
        while Parsing_flag and deep_counter < config.max_deep:
            if page_num == 1:
                page = requests.get('https://www.molnet.ru/mos/ru/news').text
            else:
                page = requests.get('https://www.molnet.ru/mos/ru/news?page='
                                    + str(page_num)).text
            page_num += 1
            soup = BeautifulSoup(page, "html.parser")
            page_counter = 1
            column = soup.find('div', {"class": "l-col__inner"})
            active = column.find('div', {"class": "rubric-prelist news"})
            if not active:
                Parsing_flag = False
            else:
                links = []
                news = column.find_all('a', {"class": "link-wr"})
                for element in news:
                    post_date = element.find('span',
                                            {"class": "prelist-date"}).text
                    links.append(['https://www.molnet.ru' + element['href'],
                                 convert_date(post_date)])

                news = column.find_all('li', {"class": "itemlist__item"})
                for element in news:
                    link = element.find('a', {"class": "itemlist__link"})['href']
                    try:
                        post_date = element.find('span',
                                                {"class": "itemlist__date"}).text
                    except Exception as e:
                        break
                    links.append(['https://www.molnet.ru' + link,
                                 convert_date(post_date)])

                for link in links:
                    deep_counter += 1
                    try:
                        parse_page_custom(link[0], publish_date=link[1])
                    except DuplicateNews as e:
                        logger.warning(e)
                        Parsing_flag = False
                        break
        logger.info('Site parsed')
    except Exception as e:
        logger.exception(e)
        logger.warning(' Вероятно на сайте произошло обновление, или ваш ip был заблокирован')


def parse_moskvatyt():
    try:
        lower_st_date = '20100301'
        Parsing_flag = True
        now_date = datetime.date.today()
        deep_counter = 0
        page_num = now_date.strftime('%Y%m%d')
        while Parsing_flag and page_num != lower_st_date and deep_counter < config.max_deep:
            if now_date == datetime.datetime.now().date():
                page = requests.get('https://www.moskva-tyt.ru/news/').text
            else:
                page = requests.get('https://www.moskva-tyt.ru/news/'
                                    + str(page_num) + '.html').text
            now_date = now_date - datetime.timedelta(days=1)
            page_num = now_date.strftime('%Y%m%d')
            soup = BeautifulSoup(page, "html.parser")
            news = soup.find_all('div', {"class": "next"})
            if not news:
                logger.warning('Something wrong')
                flag = 1
            else:
                for element in news:
                    link = element.find('a')
                    deep_counter += 1
                    try:
                        moskvatytpage('https://www.moskva-tyt.ru/'+link['href'])
                    except DuplicateNews as e:
                        logger.warning(e)
                        Parsing_flag = False
                        break
        logger.info('Site parsed')
    except Exception as e:
        logger.exception(e)
        logger.warning(' Вероятно на сайте произошло обновление, или ваш ip был заблокирован')


def moskvatytpage(link):
    page = requests.get(link).text
    soup = BeautifulSoup(page, "html.parser")
    body = soup.find('div', {"class": "text"})
    text = ''
    elements = body.find_all('p')
    for element in elements:
        text += element.text
    session = db.Session()
    news = session.query(db.News).filter(db.News.link == link).first()
    date = link.strip('https://www.moskva-tyt.ru/news/')[:8]
    date = datetime.datetime.strptime(date, "%Y%m%d").date()
    parse_page_custom(link, text=text, publish_date=date)
    session.close()


def parse_mn():
    try:
        Parsing_flag = True
        deep_counter = 0
        count = 1
        while Parsing_flag and deep_counter < config.max_deep:
            link = 'https://www.mn.ru/api/v1/articles/more?page_size=5&page=' + str(count)
            page = requests.get(link).json()
            count += 1
            for news in page["data"]:
                deep_counter += 1
                date = news["attributes"]['published_at'][:10]
                publish_date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
                try:
                    news_link = 'https://www.mn.ru' + news['links']['self']
                    parse_page_custom(link=news_link,
                                      title=news["attributes"]['title'],
                                      text=news["attributes"]['description'],
                                      publish_date=publish_date)
                except DuplicateNews as e:
                    logger.warning(e)
                    Parsing_flag = False
                    break
        logger.info('Site parsed')
    except Exception as e:
        logger.exception(e)
        logger.warning(' Вероятно на сайте произошло обновление, или ваш ip был заблокирован')


if __name__ == "__main__":
    parser1 = Thread(target=parse_msknews)
    parser2 = Thread(target=parse_msknovosti)
    parser3 = Thread(target=parse_mskiregion)
    parser4 = Thread(target=parse_molnet)
    parser5 = Thread(target=parse_moskvatyt)
    parser6 = Thread(target=parse_mn)
    while True:
        parser1.start()
        parser2.start()
        parser3.start()
        parser4.start()
        parser5.start()
        parser6.start()
        logger.info('Потоки запущены')
        time.sleep(config.delay)
