FROM python:3.10-slim-bookworm

ENV DEBIAN_FRONTEND=noninteractive

# تثبيت الاعتماديات الأساسية (curl, lsb-release, gpg)
RUN apt-get update && \
    apt-get install -y curl lsb-release gpg

# إضافة مفتاح GPG الخاص بـ Cloudflare WARP
RUN curl -fsSL https://pkg.cloudflareclient.com/pubkey.gpg | gpg --yes --dearmor --output /usr/share/keyrings/cloudflare-warp-archive-keyring.gpg

# إضافة مستودع Cloudflare WARP الصحيح
RUN echo "deb [signed-by=/usr/share/keyrings/cloudflare-warp-archive-keyring.gpg] https://pkg.cloudflareclient.com/ bookworm main" | tee /etc/apt/sources.list.d/cloudflare-client.list

# تثبيت حزمة cloudflare-warp وتنظيف الملفات المؤقتة
RUN apt-get update && \
    apt-get install -y cloudflare-warp && \
    apt-get clean

WORKDIR /app
COPY . .
RUN pip install --no-cache-dir -r requirements.txt

# ✅ الأمر النهائي: تمرير 'y' لقبول الشروط تلقائياً
CMD warp-svc --accept-tos & sleep 3 && printf 'y\n' | warp-cli registration new && sleep 2 && warp-cli connect && python app.py