import telebot
from telebot import types

import config
from src import db

bot = telebot.TeleBot(config.token)


@bot.message_handler(content_types=['text'])
def start_chat(message):
    session = db.Session()
    news = session.query(db.News).all()
    session.close()
    bot.send_message(message.from_user.id, str(news[-1].id))


bot.polling(none_stop=True, interval=0)
