FROM python:3.12-slim

RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple

RUN echo "deb https://mirrors.ustc.edu.cn/debian/ bookworm main contrib non-free" > /etc/apt/sources.list && \
    echo "deb https://mirrors.ustc.edu.cn/debian/ bookworm-updates main contrib non-free" >> /etc/apt/sources.list && \
    echo "deb https://mirrors.ustc.edu.cn/debian-security bookworm-security main contrib non-free" >> /etc/apt/sources.list

RUN apt-get update && apt-get install -y rsync locales tzdata sshpass openssh-client

RUN sed -i '/zh_CN.UTF-8/s/^# //' /etc/locale.gen && locale-gen
ENV LANG=zh_CN.UTF-8

ENV TZ=Asia/Shanghai

RUN sed -i 's/#user_allow_other/user_allow_other/' /etc/fuse.conf 2>/dev/null || true

WORKDIR /app

ENV PIP_DEFAULT_TIMEOUT=100

COPY app/requirements.txt .
RUN pip install -r requirements.txt

COPY app/ .

STOPSIGNAL SIGTERM

CMD ["python", "app.py"]
