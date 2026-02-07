FROM python:3.11-slim-bookworm

WORKDIR /app

# Установка tzdata для корректной работы с часовыми поясами
RUN apt-get update && apt-get install -y tzdata \
    && rm -rf /var/lib/apt/lists/*

# Установка часового пояса на московское время
ENV TZ=Europe/Moscow
RUN ln -sf /usr/share/zoneinfo/$TZ /etc/localtime && dpkg-reconfigure -f noninteractive tzdata

COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .

CMD ["python", "-m", "bot.main"]

