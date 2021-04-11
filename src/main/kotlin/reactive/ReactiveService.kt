package reactive

import io.netty.buffer.ByteBuf
import io.netty.handler.codec.http.HttpMethod
import io.netty.handler.codec.http.HttpResponseStatus
import io.reactivex.netty.protocol.http.server.HttpServer
import io.reactivex.netty.protocol.http.server.HttpServerRequest
import io.reactivex.netty.protocol.http.server.HttpServerResponse
import io.vertx.pgclient.PgConnectOptions
import io.vertx.rxjava.pgclient.PgPool
import io.vertx.rxjava.sqlclient.Tuple
import io.vertx.sqlclient.data.Numeric
import rx.Observable
import java.lang.NumberFormatException
import java.math.BigDecimal
import kotlin.RuntimeException

data class Item(val name: String, val price: BigDecimal)

class ReactiveService {
    private val server: HttpServer<ByteBuf, ByteBuf>
    private val pool = PgPool.pool(CONNECTION_OPTIONS)

    init {
        server = HttpServer.newServer(8080)
            .start(::dispatchRequest)
    }

    private fun dispatchRequest(
        request: HttpServerRequest<ByteBuf>,
        response: HttpServerResponse<ByteBuf>
    ): Observable<Void> {
        return when (request.decodedPath) {
            "/user" -> processUser(request, response)
            "/item" -> processItem(request, response)
            "/list" -> listItems(request, response)
            else -> unknownPathFallback(request, response)
        }
    }

    private fun processUser(
        request: HttpServerRequest<ByteBuf>,
        response: HttpServerResponse<ByteBuf>
    ): Observable<Void> {
        if (request.httpMethod != HttpMethod.PUT) {
            response.status = HttpResponseStatus.METHOD_NOT_ALLOWED
            return response
        }

        val currency = request.queryParameters["currency"]?.firstOrNull()
        if (!EXCHANGE_RATES.containsKey(currency)) {
            response.status = HttpResponseStatus.BAD_REQUEST
            return response.writeString(Observable.just("Parameter 'currency' must be one of ${EXCHANGE_RATES.keys}"))
        }

        return pool.preparedQuery("insert into users(currency) values ($1) returning id")
            .rxExecute(Tuple.of(currency)).map { rowSet ->
                val resultRow = rowSet.iterator().next()
                resultRow.getInteger("id").toString()
            }.flatMapObservable { id ->
                response.writeString(Observable.just(id))
            }
    }

    private fun processItem(
        request: HttpServerRequest<ByteBuf>,
        response: HttpServerResponse<ByteBuf>
    ): Observable<Void> {
        if (request.httpMethod != HttpMethod.PUT) {
            response.status = HttpResponseStatus.METHOD_NOT_ALLOWED
            return response
        }

        val name = request.queryParameters["name"]?.firstOrNull()
        if (name == null) {
            response.status = HttpResponseStatus.BAD_REQUEST
            return response.writeString(Observable.just("Parameter 'name' must be present"))
        }

        val price = request.queryParameters["price"]?.firstOrNull()?.let {
            try {
                Numeric.parse(it)
            } catch (e: NumberFormatException) {
                null
            }
        }
        if (price == null) {
            response.status = HttpResponseStatus.BAD_REQUEST
            return response.writeString(Observable.just("Parameter 'price' must be present and be a valid decimal"))
        }

        return pool.preparedQuery("insert into items(name, price) values ($1, $2) returning id")
            .rxExecute(Tuple.of(name, price)).map { rowSet ->
                val resultRow = rowSet.iterator().next()
                resultRow.getInteger("id").toString()
            }.flatMapObservable { id ->
                response.writeString(Observable.just(id))
            }
    }

    private fun listItems(
        request: HttpServerRequest<ByteBuf>,
        response: HttpServerResponse<ByteBuf>
    ): Observable<Void> {
        if (request.httpMethod != HttpMethod.GET) {
            response.status = HttpResponseStatus.METHOD_NOT_ALLOWED
            return response
        }

        val userId = request.queryParameters["userId"]?.firstOrNull()?.toIntOrNull()
        if (userId == null) {
            response.status = HttpResponseStatus.BAD_REQUEST
            return response.writeString(Observable.just("Parameter 'userId' must be present and be an integer"))
        }

        return pool.preparedQuery("select currency from users where id = $1")
            .rxExecute(Tuple.of(userId)).map { rowSet ->
                if (rowSet.size() == 0) {
                    throw NoUserException()
                }
                val currency = rowSet.iterator().next().getString("currency")
                EXCHANGE_RATES[currency] ?: throw UnsupportedCurrencyException(currency)
            }.flatMap { exchangeRate ->
                pool.preparedQuery("select name, price from items").rxExecute().map { rowSet ->
                    rowSet.map {
                        Item(it.getString("name"),
                            exchangeRate * it.getNumeric("price").bigDecimalValue())
                    }.joinToString("\n") {
                        "${it.name}: ${it.price}"
                    }
                }
            }.flatMapObservable { id ->
                response.writeString(Observable.just(id))
            }.onErrorResumeNext { throwable ->
                when (throwable) {
                    is NoUserException -> {
                        response.status = HttpResponseStatus.NOT_FOUND
                        response.writeString(Observable.just("No such user"))
                    }
                    is UnsupportedCurrencyException -> {
                        response.status = HttpResponseStatus.NOT_IMPLEMENTED
                        response.writeString(Observable.just("Currency ${throwable.currency} is not supported"))
                    }
                    else -> {
                        response.status = HttpResponseStatus.INTERNAL_SERVER_ERROR
                        Observable.empty()
                    }
                }
            }
    }

    private fun unknownPathFallback(
        request: HttpServerRequest<ByteBuf>,
        response: HttpServerResponse<ByteBuf>
    ): Observable<Void> {
        response.status = HttpResponseStatus.NOT_FOUND
        return response
    }

    fun awaitShutdown() {
        server.awaitShutdown()
    }

    class NoUserException: RuntimeException()
    class UnsupportedCurrencyException(val currency: String): RuntimeException()

    companion object {
        private val CONNECTION_OPTIONS = PgConnectOptions()
            .setHost("localhost")
            .setPort(5432)
            .setDatabase("sd_reactive")
            .setUser("sd_lab")
            .setPassword("temp")

        private val EXCHANGE_RATES = mapOf(
            "rub" to BigDecimal.ONE,
            "usd" to BigDecimal.valueOf(70),
            "eur" to BigDecimal.valueOf(80),
        )
    }
}
