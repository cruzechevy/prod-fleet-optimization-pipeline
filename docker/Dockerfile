FROM python:3.10

WORKDIR /app

# Install default Java (works across Debian versions)
RUN apt-get update && apt-get install -y \
    default-jdk \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Set JAVA_HOME dynamically (important)
ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH=$JAVA_HOME/bin:$PATH

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

RUN pip install pyspark==3.5.1 delta-spark==3.1.0 notebook

EXPOSE 8888

CMD ["jupyter", "notebook", "--ip=0.0.0.0", "--port=8888", "--no-browser", "--allow-root"]