FROM python:3.10-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Сначала копируем файл с зависимостями
COPY requirements.txt .

# Устанавливаем их
RUN pip install --no-cache-dir -r requirements.txt

# Копируем код
COPY generator.py .

# Запускаем
CMD ["sh", "-c", "python generator.py"]