FROM python:3.10-slim

# Устанавливаем рабочую директорию
WORKDIR /app

# Сначала копируем только файл с зависимостями (это ускоряет сборку)
COPY requirements.txt .

# Устанавливаем их
RUN pip install --no-cache-dir -r requirements.txt

# Теперь копируем сам код
COPY generator.py .

# Запускаем
CMD ["sh", "-c", "python generator.py"]