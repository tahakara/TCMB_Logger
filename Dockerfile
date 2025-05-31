# Python 3.11 slim image kullan
FROM python:3.12-apline

# Çalışma dizinini ayarla
WORKDIR /app

# Sistem paketlerini güncelle ve gerekli paketleri yükle
RUN apt-get update && apt-get install -y \
    gcc \
    default-libmysqlclient-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

# Python bağımlılıklarını kopyala ve yükle
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Uygulama kodunu kopyala
COPY . .

# Logs dizinini oluştur
RUN mkdir -p logs

# Environment dosyası için volume mount noktası
VOLUME ["/app/logs"]

# Port 8080'i expose et (gerekirse web interface için)
# EXPOSE 8080

# Non-root user oluştur ve kullan
RUN useradd -m -u 1000 tcmbuser && chown -R tcmbuser:tcmbuser /app
USER tcmbuser

# Uygulama başlatma komutu
CMD ["python", "app.py"]
