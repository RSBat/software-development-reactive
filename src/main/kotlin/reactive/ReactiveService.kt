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
import rx.Observable
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

        return pool.preparedQuery("insert into users(currency) values ($1) returning id")
            .rxExecute(Tuple.of("eur")).map { rowSet ->
                val resultRow = rowSet.iterator().next()
                resultRow.get(Integer::class.java, "id").toString()
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

        return pool.preparedQuery("insert into items(name, price) values ($1, $2) returning id")
            .rxExecute(Tuple.of("test", 1)).map { rowSet ->
                val resultRow = rowSet.iterator().next()
                resultRow.get(Integer::class.java, "id").toString()
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

        return pool.preparedQuery("select currency from users where id = $1")
            .rxExecute(Tuple.of(1)).map { rowSet ->
                if (rowSet.size() == 0) {
                    throw NoUserException()
                }
                val currency = rowSet.iterator().next()
                    .get(String::class.java, "currency")
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
