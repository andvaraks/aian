# AI-анализатор встреч PRO

Сервис распознаёт русскую речь и готовит конспект с решениями, задачами и открытыми вопросами.

## Запуск

Установите Python 3.12 и FFmpeg. Установите зависимости командой `python -m pip install -r requirements.txt`. Задайте `VSELLM_API_KEY`, `PUBLIC_ORIGIN=http://127.0.0.1:8006` и `COOKIE_SECURE=false` в окружении. Запустите `python -m uvicorn main:app --host 127.0.0.1 --port 8006`.

Тесты: `python -m unittest discover -s tests`.

PRO принимает публичные ссылки Dropbox и Google Drive на MP4 и WebM. Повтор ссылки открывает сохранённый результат.
Исполнитель и срок отображаются в отдельных полях.
