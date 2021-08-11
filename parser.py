import datetime
import requests
import time

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

    article = Article(link, language='ru')
    article.download()
    article.parse()
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


def parse_page(link, category):
    article = Article(link, language='ru')
    article.download()
    article.parse()
    flag = 0
    if category == 1:
        if article.publish_date:
            session = db.Session()
            news = session.query(db.News).filter(db.News.link == link).first()
            if not news:
                session.add(db.News(link=link, title=article.title,
                            text=article.text,
                            publish_date=article.publish_date.date(),
                            parsed_date=datetime.datetime.now()))
                session.commit()
            else:
                flag = 1
            session.close()
            logger.info('Page parsed')
        else:
            logger.exception("Date Error")
    elif category == 2:
        page = requests.get(link).text
        soup = BeautifulSoup(page, "html.parser")
        body = soup.find('div', {"class": "text"})
        text = ''
        try:
            elements = body.find_all('p')
            for element in elements:
                text += element.text
            session = db.Session()
            news = session.query(db.News).filter(db.News.link == link).first()
            if not news:
                if article.publish_date:
                    session.add(db.News(link=link, title=article.title,
                                text=text,
                                publish_date=article.publish_date.date(),
                                parsed_date=datetime.datetime.now()))
                    session.commit()
                else:
                    date = link.strip('https://www.moskva-tyt.ru/news/')[:8]
                    date = datetime.datetime.strptime(date, "%Y%m%d").date()
                    session.add(db.News(link=link, title=article.title,
                                text=text,
                                publish_date=date,
                                parsed_date=datetime.datetime.now()))
                    session.commit()
            else:
                flag = 1
            session.close()
            logger.info('Page parsed')
        except Exception as e:
            logger.warning('Cant parse page')
    elif category == 3:
        print(article.title)
        print(article.text)
        print(article.publish_date.date())
    else:
        publish_date = category
        session = db.Session()
        news = session.query(db.News).filter(db.News.link == link).first()
        if not news:
            session.add(db.News(link=link, title=article.title,
                        text=article.text,
                        publish_date=publish_date,
                        parsed_date=datetime.datetime.now()))
            session.commit()
        else:
            flag = 1
        session.close()
        logger.info('Page parsed')
    return flag


def parse_msknews():
    page = requests.get('http://msk-news.net/').text
    soup = BeautifulSoup(page, "html.parser")
    sitehead = soup.find('div', {"id": "menu"})
    categories = sitehead.find_all('a')
    for category in categories:
        parse_msknews_category(category['href'])
    logger.info('Site parsed')


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
    page = requests.get('https://msknovosti.ru/').text
    soup = BeautifulSoup(page, "html.parser")
    sitehead = soup.find('div', {"class": "menu-main-container"})
    categories = sitehead.find_all('a')
    for category in categories:
        parse_msknovosti_category(category['href'], 1, 0, 0)
    logger.info('Site parsed')


def parse_msknovosti_category(url, count, maxcount, counter):
    if count == 1:
        page = requests.get(url).text
        soup = BeautifulSoup(page, "html.parser")
        element = soup.find('a', {"class": "page-numbers"})
        maxcount = int(element.find_next_sibling("a").text)
    else:
        page = requests.get(url + '/page/' + str(count)).text
        soup = BeautifulSoup(page, "html.parser")
    count += 1
    column = soup.find_all('div', {"class": "post-card post-card--vertical w-animate"})
    flag = 0
    for element in column:
        counter += 1
        result = parse_page(element.find('a')['href'], 1)
        if result == 1:
            flag = 1
    if flag == 0 and count <= maxcount and counter < config.max_deep_cat:
        parse_msknovosti_category(url, count, maxcount, counter)
    else:
        logger.info('Category parsed')


def parse_mskiregion():
    flag = 0
    page_num = 1
    counter = 0
    while flag == 0 and counter < config.max_deep:
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
                counter += 1
                link = title.find('a')
                result = parse_page(link['href'], 1)
                if result == 1:
                    flag = 1
    logger.info('Site parsed')


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
    flag = 0
    page_num = 1
    counter = 0
    while flag == 0 and counter < config.max_deep:
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
            flag = 1
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
                counter += 1
                try:
                    result = parse_page(link[0], link[1])
                    if result == 1:
                        flag = 1
                except:
                    logger.warning('Cant parse page')
    logger.info('Site parsed')


def parse_moskvatyt(): #с ним аккуратнее - блочит
    lower_st_date = '20100301'
    flag = 0
    now_date = datetime.date.today()
    counter = 0
    page_num = now_date.strftime('%Y%m%d')
    while flag == 0 and page_num != lower_st_date and counter < config.max_deep:
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
                counter += 1
                result = parse_page('https://www.moskva-tyt.ru/'+link['href'], 2)
                if result == 1:
                    flag = 1
    logger.info('Site parsed')


def parse_mn():
    page = requests.get('https://www.mn.ru/news').text
    soup = BeautifulSoup(page, "html.parser")
    news = soup.find_all('span', {"class": "article_socials-copy"})
    print(soup)
    logger.info('I cant parse it')


if __name__ == "__main__":
    pass
    # parse_msknews()

    # parse_msknovosti()
    # parse_mskiregion()
    # parse_molnet()
    # parse_moskvatyt()
    # logger.info('Parse loop ended')
    # time.sleep(config.delay)
