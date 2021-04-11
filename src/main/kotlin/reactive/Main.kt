package reactive

fun main() {
    val service = ReactiveService()
    service.awaitShutdown()
}
