1. Добавьте в `.env` параметры
```
DB_URI='mongo connecting string'
CHAR_SET='ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz1234567890'
LENGTH_HASH=8
```
2. установите зависимости
`npm i`
3. Запустите в разных окнах консоли
```
ts-node generateCustomers.ts // для генерации данных
ts-node listener.ts // для слушателя
ts-node listener.ts --full-reindex // для полной синхронизации
```