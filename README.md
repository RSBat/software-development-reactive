# Реактивный веб сервис
## Ручки
* /user?currency={rub,usd,eur} - [PUT] добавляет пользователя с данной валютой, возвращает id нового пользователя
* /item?name=string&price=numeric - [PUT] добавляет товар с названием и ценой в рублях, возвращает id нового товара
* /listItems?userId=id - [GET] возвращает список товаров с ценами сконвертированными в валюту указанного пользователя

## Библиотеки
* RxNetty
* Rxified Vertx PostgreSQL client