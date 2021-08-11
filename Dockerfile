FROM python:3
WORKDIR /parser
COPY requirements.txt /parser
RUN pip install --no-cache-dir -r requirements.txt
ENV TZ=Europe/Moscow
RUN ln -snf /usr/share/zoneinfo/$TZ /etc/localtime && echo $TZ > /etc/timezone
COPY . /parser
CMD ["python3", "parser.py"]
