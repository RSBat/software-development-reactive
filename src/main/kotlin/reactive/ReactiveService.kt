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
import rx.Observer
import rx.Single
import rx.exceptions.Exceptions
import java.lang.RuntimeException

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

    companion object {
        private val CONNECTION_OPTIONS = PgConnectOptions()
            .setHost("localhost")
            .setPort(5432)
            .setDatabase("sd_reactive")
            .setUser("sd_lab")
            .setPassword("temp")
    }
}
