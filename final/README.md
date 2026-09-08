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
CRT_ALT_NAMES_IP_2=127.0.0.1 #или ваш ip хостовой машины

EOF
```

```bash
make rebuild
```

## Пользователи Kafka

- `shop-api` - отправка товаров в кластер
- `client-api` - отправка запросов клиентов
- `admin` - администрирование кластера
- `kafka-c-N` (N от 1 до 3) - контроллеры 1го кластера
- `kafka-b-N` (N от 1 до 3) - брокеры 1го кластера
- `schema-registry` - регистрация схем
- `kafka-ui` - для удобства проверки части результатов задания, можно смотреть в kafka-ui
- `mirror-maker` - дублирование данных на 2-й кластер
- `kafka-connect` - сохранение данных в файл, работа со spark, hdfs, аналитикой, коннекторами

## SuperUsers в кластерах

Честно, просто устал работать с ACL :)

Конечно, правильно, когда выдерживается подход с минимизацией прав для повышения уровня безопасности.

Список:
- кластер 1:
  - admin, kafka-b-1, kafka-b-2, kafka-b-3, kafka-c-1, kafka-c-2, kafka-c-3, mirror-maker, kafka-ui, schema-registry
- кластер 2:
  - admin, kafka2-b-1, kafka2-b-2, kafka2-b-3, kafka2-c-1,  kafka2-c-2, kafka2-c-3, mirror-maker, kafka-ui, schema-registry

## Топики

- `products` - для публикации товаров из файла
  - `data/shop-products.json` - файл с первыми 10-тью товарами
- `products_blocked` - список товаров, которые заблокированы, и не должны в итоге участвовать в обработке (аналитика и тп)
- `products_published` - список товаров, которые прошли фильтрацию заблокированных товаров, и должны в итоге участвовать в обработке (аналитика и тп)
- `recommendations`

## Скрипты для развертывания

- `/scripts/certs.sh` - создание сертификатов
  - для простоты, сертификаты на оба кластера и другие сервисы идентичные
  - `/mount_dir` - где будут созданы необходимые сертификаты
- `/scripts/topic.sh` - создание топиков
- `/scripts/acl.sh` - выдача прав
- `/scripts/schema-registry.sh` - регистрация JSON схем в сервисе schema-registry
- `/scripts/kafka-connect.sh` - регистрация коннектора для сохранения данных в файл


Данные в файле можно увидеть так:
```bash
 docker exec -it kafka-connect cat /var/lib/kafka-connect-data/products_published.jsonl
```
пример:
```text
Struct{store_id=store_001,images=[Struct{alt=Наушники SoundMax Pro,url=https://example.com/images/product2.jpg}],description=Беспроводные наушники с активным шумоподавлением и высоким качеством звука.,created_at=2024-01-15T10:00:00Z,index=products,specifications=Struct{water_resistance=IPX4,weight=250g,battery_life=30 hours,dimensions=20cm x 18cm x 8cm},tags=[наушники, аудио, беспроводные],updated_at=2024-01-20T12:00:00Z,price=Struct{amount=8999.0,currency=RUB},product_id=p002,name=Наушники SoundMax Pro,category=Электроника,stock=Struct{reserved=15,available=80},sku=SM-P002,brand=SoundMax}
```

## Проверка

### Публикация и дублирование данных между кластерами

- [Topics clusters kafka2-kraft](http://localhost:8080/ui/clusters/kafka2-kraft/all-topics?perPage=25)
  - можно увидеть, что данные в топиках дублируются
  - Это состояние топиков 2-го кластера после публикации первых 10 товаров, когда еще нет заблокированных:
    - ![состояние топиков 2-го кластера](./screens/1.png)

### Проверь данные в HDFS

```bash
docker exec hdfs-namenode hdfs dfs -ls -R /topics
```
Результат вида:
```text
rwxr-xr-x   - appuser supergroup          0 2026-09-08 10:09 /topics/+tmp
drwxr-xr-x   - appuser supergroup          0 2026-09-08 10:09 /topics/+tmp/products_published
drwxr-xr-x   - appuser supergroup          0 2026-09-08 10:09 /topics/+tmp/products_published/partition=0
drwxr-xr-x   - appuser supergroup          0 2026-09-08 10:11 /topics/+tmp/products_published/partition=1
drwxr-xr-x   - appuser supergroup          0 2026-09-08 10:09 /topics/+tmp/products_published/partition=2
drwxr-xr-x   - appuser supergroup          0 2026-09-08 10:09 /topics/products_published
drwxr-xr-x   - appuser supergroup          0 2026-09-08 10:09 /topics/products_published/partition=0
-rw-r--r--   1 appuser supergroup       2304 2026-09-08 10:09 /topics/products_published/partition=0/products_published+0+0000000000+0000000002.json
-rw-r--r--   1 appuser supergroup       2186 2026-09-08 10:09 /topics/products_published/partition=0/products_published+0+0000000003+0000000005.json
-rw-r--r--   1 appuser supergroup       2170 2026-09-08 10:09 /topics/products_published/partition=0/products_published+0+0000000006+0000000008.json
drwxr-xr-x   - appuser supergroup          0 2026-09-08 10:11 /topics/products_published/partition=1
-rw-r--r--   1 appuser supergroup       2289 2026-09-08 10:09 /topics/products_published/partition=1/products_published+1+0000000000+0000000002.json
-rw-r--r--   1 appuser supergroup        735 2026-09-08 10:11 /topics/products_published/partition=1/products_published+1+0000000003+0000000003.json
drwxr-xr-x   - appuser supergroup          0 2026-09-08 10:09 /topics/products_published/partition=2
-rw-r--r--   1 appuser supergroup       2212 2026-09-08 10:09 /topics/products_published/partition=2/products_published+2+0000000000+0000000002.json
-rw-r--r--   1 appuser supergroup       2212 2026-09-08 10:09 /topics/products_published/partition=2/products_published+2+0000000003+0000000005.json
```