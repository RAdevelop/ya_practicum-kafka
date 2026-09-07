## Развернуть окружение

- Создайте файл  `./build/.env_make`
- В файле укажите переменные:
  - `CA_PASS` - для пароля сертификатов
  - `CRT_ALT_NAMES_IP_2` - ip адрес вашей машины, если чтобы можно было подключаться к Кафка с этого внешнего адреса.
    - Например, при запуске go-app:
      - `go run ./cmd/server-api/main.go`
    - Или не указывайте, будет использован 127.0.0.1
- Пример:
```bash
cat > "./build/.env_make" << EOF
CA_PASS=kafka123
CRT_ALT_NAMES_IP_2=127.0.0.1 #ваш ip

EOF
```

```bash
make rebuild
```



## Пользователи Kafka

- `shop-api`
  - Отправка товаров
- `client-api`
  - Отправка запросов клиентов
- `admin` 
- `kafka-c-N` (N от 1 до 3) - контроллеры
- `kafka-b-N` (N от 1 до 3) - брокеры
- `schema-registry`
- `kafka-ui`

Итоговый набор пользователей для второго кластера
Для второго кластера тебе понадобятся сертификаты для:

- admin (управление кластером)
- mirror-maker (репликация)
- spark (аналитика)
- kafka-ui (мониторинг) — опционально.

## Топики

- `products` - для публикации товаров из файла
  - права пользователей
    - `shop-api`
      - Write
      - Describe
- `products_blocked` - список товаров, которые заблокированы, и не должны в итоге участвовать в обработке (аналитика и тп)
- `products_published` - список товаров, которые прошли фильтрацию заблокированных товаров, и должны в итоге участвовать в обработке (аналитика и тп)
- `recommendations`

Данные в файле можно увидеть так:
```bash
 docker exec -it kafka-connect cat /var/lib/kafka-connect-data/products_published.jsonl
```
пример:
```text
Struct{store_id=store_001,images=[Struct{alt=Наушники SoundMax Pro,url=https://example.com/images/product2.jpg}],description=Беспроводные наушники с активным шумоподавлением и высоким качеством звука.,created_at=2024-01-15T10:00:00Z,index=products,specifications=Struct{water_resistance=IPX4,weight=250g,battery_life=30 hours,dimensions=20cm x 18cm x 8cm},tags=[наушники, аудио, беспроводные],updated_at=2024-01-20T12:00:00Z,price=Struct{amount=8999.0,currency=RUB},product_id=p002,name=Наушники SoundMax Pro,category=Электроника,stock=Struct{reserved=15,available=80},sku=SM-P002,brand=SoundMax}
```