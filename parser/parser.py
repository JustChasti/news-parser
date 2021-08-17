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


class DoesntExistence(Exception):
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
            try:
                parse_msknews_category(category['href'])
            except DuplicateNews as e:
                logger.info(e)
                logger.info('Category parsed')
            except DoesntExistence as e:
                logger.warning(e)
                logger.warning('Кончились страницы или блокировка ip')
                logger.info('Category parsed')
    except Exception as e:
        logger.exception(e)
        logger.error('Вероятно на сайте произошло обновление')


def parse_msknews_category(url):
    deep_counter = 0
    response = requests.get(url)
    if response.status_code != 200:
        raise DoesntExistence
    page = response.text
    page_count = 1
    soup = BeautifulSoup(page, "html.parser")
    column = soup.find('div', {"class": "col2"})

    while column:
        column = soup.find('div', {"class": "col2"})
        pages = column.find_all('div', {"class": "post_title"})
        for element in pages:
            page = element.find('a', {"class": "vh"})
            deep_counter += 1
            parse_page_custom(page['href'])

        column = soup.find('div', {"class": "col2 col2b"})
        pages = column.find_all('div', {"class": "post_title"})
        for element in pages:
            page = element.find('a', {"class": "vh"})
            deep_counter += 1
            parse_page_custom(page['href'])

        if page_count <= 100 and deep_counter < config.max_deep_cat:
            pass
        else:
            logger.info('Category parsed')
            break
        page_count += 1
        response = requests.get(url + '/' + str(count))
        if response.status_code != 200:
            raise DoesntExistence
        page = response.text
        soup = BeautifulSoup(page, "html.parser")


def parse_msknovosti():
    try:
        response = requests.get('https://msknovosti.ru/')
        if response.status_code != 200:
            raise DoesntExistence
        page = response.text
        soup = BeautifulSoup(page, "html.parser")
        sitehead = soup.find('div', {"class": "menu-main-container"})
        categories = sitehead.find_all('a')
        for category in categories:
            parse_msknovosti_category(category['href'])
        logger.info('Site parsed')
    except DoesntExistence as e:
        logger.warning(e)
        logger.warning('блокировка ip')
    except Exception as e:
        logger.exception(e)
        logger.warning(' Вероятно на сайте произошло обновление')


def parse_msknovosti_category(url):
    count = 1
    deep_counter = 0
    response = requests.get(url)
    if response.status_code != 200:
        raise DoesntExistence
    page = response.text
    soup = BeautifulSoup(page, "html.parser")
    element = soup.find('a', {"class": "page-numbers"})
    maxcount = int(element.find_next_sibling("a").text)
    try:
        while count <= maxcount and deep_counter < config.max_deep_cat:
            count += 1
            column = soup.find_all('div', {"class": "post-card post-card--vertical w-animate"})
            flag = 0
            for element in column:
                deep_counter += 1
                parse_page_custom(element.find('a')['href'])
            response = requests.get(url + '/page/' + str(count))
            if response.status_code != 200:
                raise DoesntExistence
            page = response.text
            soup = BeautifulSoup(page, "html.parser")
    except DuplicateNews as e:
        logger.info(e)
        logger.info('Category parsed')
    except DoesntExistence as e:
        logger.warning(e)
        logger.warning('Кончились страницы или блокировка ip')
        logger.info('Category parsed')
    except Exception as e:
        logger.exception(e)
        logger.error('Вероятно на сайте произошло обновление')
    


def parse_mskiregion():
    try:
        page_num = 1
        deep_counter = 0
        while deep_counter < config.max_deep:
            if page_num == 1:
                link = 'https://msk.inregiontoday.ru/?cat=1'
            else:
                link = f'https://msk.inregiontoday.ru/?cat=1&paged={page_num}'
            response = requests.get(link)
            if response.status_code != 200:
                raise DoesntExistence
            page = response.text
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
                    parse_page_custom(link['href'])
    
        logger.info('Site parsed')
    except DuplicateNews as e:
        logger.info(e)
        logger.info('Site parsed')
    except DoesntExistence as e:
        logger.warning(e)
        logger.warning('Кончились страницы или блокировка ip')
        logger.info('Site parsed')
    except Exception as e:
        logger.exception(e)
        logger.error('Вероятно на сайте произошло обновление')


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
        page_num = 1
        deep_counter = 0
        while deep_counter < config.max_deep:
            if page_num == 1:
                link = 'https://www.molnet.ru/mos/ru/news'
            else:
                link = f'https://www.molnet.ru/mos/ru/news?page={page_num}'
            response = requests.get(link)
            if response.status_code != 200:
                raise DoesntExistence
            page = response.text
            page_num += 1
            soup = BeautifulSoup(page, "html.parser")
            page_counter = 1
            column = soup.find('div', {"class": "l-col__inner"})
            active = column.find('div', {"class": "rubric-prelist news"})
            if not active:
                raise DoesntExistence
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
                    parse_page_custom(link[0], publish_date=link[1])

        logger.info('Site parsed')
    except DuplicateNews as e:
        logger.info(e)
        logger.info('Site parsed')
    except DoesntExistence as e:
        logger.warning(e)
        logger.warning('Кончились страницы или блокировка ip')
        logger.info('Site parsed')
    except Exception as e:
        logger.exception(e)
        logger.error('Вероятно на сайте произошло обновление')


def parse_moskvatyt():
    try:
        lower_st_date = '20100301'
        now_date = datetime.date.today()
        deep_counter = 0
        page_num = now_date.strftime('%Y%m%d')
        while page_num != lower_st_date and deep_counter < config.max_deep:
            if now_date == datetime.datetime.now().date():
                link = 'https://www.moskva-tyt.ru/news/'
            else:
                link = f'https://www.moskva-tyt.ru/news/{page_num}.html'

            response = requests.get(link)
            if response.status_code != 200:
                raise DoesntExistence(f'Page with page: {count} doesnt exist')

            page = response.text
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
                    moskvatytpage('https://www.moskva-tyt.ru/'+link['href'])
        logger.info('Site parsed')
    except DuplicateNews as e:
        logger.info(e)
        logger.info('Site parsed')
    except DoesntExistence as e:
        logger.warning(e)
        logger.warning('Кончились страницы или блокировка ip')
        logger.info('Site parsed')
    except Exception as e:
        logger.exception(e)
        logger.error('Вероятно на сайте произошло обновление')


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
        deep_counter = 0
        count = 1
        while deep_counter < config.max_deep:
            link = f'https://www.mn.ru/api/v1/articles/more?page_size=5&page={count}'
            response = requests.get(link)
            if response.status_code != 200:
                raise DoesntExistence(f'Page with page: {count} doesnt exist')
            page = response.json()
            count += 1
            for news in page["data"]:
                deep_counter += 1
                date = news["attributes"]['published_at'][:10]
                publish_date = datetime.datetime.strptime(date, "%Y-%m-%d").date()
                news_link = 'https://www.mn.ru' + news['links']['self']
                parse_page_custom(link=news_link,
                                  title=news["attributes"]['title'],
                                  text=news["attributes"]['description'],
                                  publish_date=publish_date)
        logger.info('Site parsed')
    except DuplicateNews as e:
        logger.info(e)
        logger.info('Site parsed')
    except DoesntExistence as e:
        logger.warning(e)
        logger.warning('Кончились страницы или блокировка ip')
        logger.info('Site parsed')
    except Exception as e:
        logger.exception(e)
        logger.error('Вероятно на сайте произошло обновление')


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
