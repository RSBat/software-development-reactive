package reactive

import io.netty.buffer.ByteBuf
import io.netty.handler.codec.http.HttpResponseStatus
import io.reactivex.netty.protocol.http.server.HttpServer
import io.reactivex.netty.protocol.http.server.HttpServerRequest
import io.reactivex.netty.protocol.http.server.HttpServerResponse
import rx.Observable

class ReactiveService {
    private val server: HttpServer<ByteBuf, ByteBuf>

    init {
        server = HttpServer.newServer()
            .start(::dispatchRequest)
    }

    private fun dispatchRequest(
        request: HttpServerRequest<ByteBuf>,
        response: HttpServerResponse<ByteBuf>
    ): Observable<Void> {
        return when (request.decodedPath) {
            else -> unknownPathFallback(request, response)
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
}
