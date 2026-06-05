FROM python:3.12-slim

# 设置pip源为国内源
RUN pip config set global.index-url https://pypi.tuna.tsinghua.edu.cn/simple

# 设置apt源为国内源 (bookworm)
RUN echo "deb https://mirrors.ustc.edu.cn/debian/ bookworm main contrib non-free" > /etc/apt/sources.list && \
    echo "deb https://mirrors.ustc.edu.cn/debian/ bookworm-updates main contrib non-free" >> /etc/apt/sources.list && \
    echo "deb https://mirrors.ustc.edu.cn/debian-security bookworm-security main contrib non-free" >> /etc/apt/sources.list

# 安装rsync和必要工具（sshpass用于SSH密码认证）
RUN apt-get update && apt-get install -y rsync locales tzdata sshpass openssh-client

# 设置locale
RUN locale-gen zh_CN.UTF-8
ENV LANG zh_CN.UTF-8
ENV LANGUAGE zh_CN:zh
ENV LC_ALL zh_CN.UTF-8

# 设置默认时区为Asia/Shanghai
ENV TZ=Asia/Shanghai

# 启用FUSE的user_allow_other选项（保留兼容）
RUN sed -i 's/#user_allow_other/user_allow_other/' /etc/fuse.conf 2>/dev/null || true

WORKDIR /app

# 设置pip安装超时时间
ENV PIP_DEFAULT_TIMEOUT=100

COPY app/requirements.txt .
RUN pip install -r requirements.txt

COPY app/ .

# 创建启动脚本
RUN echo '#!/bin/bash\n\
# 获取实际的挂载点路径\n\
HOME_PATH=$(readlink -f /home)\n\
DATA_PATH=$(readlink -f /data)\n\
\n\
# 将挂载点信息写入配置文件\n\
echo "{\n\
  \"/home\": \"$HOME_PATH\",\n\
  \"/data\": \"$DATA_PATH\"\n\
}" > /app/mount_points.json\n\
\n\
# 启动应用 (exec 让 Python 成为 PID 1，SIGTERM 直达)\n\
exec python app.py' > /app/start.sh && \
chmod +x /app/start.sh

CMD ["/app/start.sh"] 