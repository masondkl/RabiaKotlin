package me.mason.rabia

import com.github.exerosis.mynt.SocketProvider
import com.github.exerosis.mynt.base.Connection
import com.github.exerosis.mynt.base.Provider
import com.github.exerosis.mynt.base.Read
import com.github.exerosis.mynt.base.Write
import kotlinx.coroutines.*
import kotlinx.coroutines.channels.*
import kotlinx.coroutines.channels.Channel.Factory.UNLIMITED
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.lang.System.currentTimeMillis
import java.net.InetSocketAddress
import java.net.StandardSocketOptions
import java.nio.channels.AsynchronousChannelGroup
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.LockSupport
import kotlin.time.Duration.Companion.seconds

val REPLICA_TIMEOUT = 30.seconds
val CLIENT_TIMEOUT = 30.seconds

const val PHASE_COUNT = 8
const val LOG_COUNT = 256

const val N = 5
const val F = 2

const val REQUEST: Byte = 0
const val FORWARD: Byte = 1
const val PROPOSAL: Byte = 2
const val STATE: Byte = 3
const val VOTE: Byte = 4

fun id() = (ThreadLocalRandom.current().nextInt().toLong() shl 32) or (currentTimeMillis() and 0xFFFFFFFFL)
fun Long.time() = this and 0xFFFFFFFFL
fun Long.random() = this shr 32
fun Long.idStr() = "[${time()} ${random()}]"

class Request(
    val id: Long,
    var message: String
) {
    override fun equals(other: Any?): Boolean {
        if (other !is Request) return false
        return message == other.message && id == other.id
    }
    override fun toString(): String {
        val random = id.random()
        val time = id.time()
        return "[$time $random $message]"
    }
}

suspend fun Write.string(bytes: ByteArray) {
    int(bytes.size)
    bytes(bytes, bytes.size, 0)
}

suspend fun Read.string(): ByteArray {
    val length = int()
    return ByteArray(length).also { bytes(it, length, 0) }
}

suspend fun Write.uuid(uuid: UUID) {
    long(uuid.mostSignificantBits)
    long(uuid.leastSignificantBits)
}

suspend fun Read.uuid() = UUID(long(), long())

suspend fun Write.request(request: Request) {
    long(request.id)
    string(request.message.toByteArray())
}

suspend fun Read.request() = Request(long(), String(string()))

suspend fun Writes(dispatcher: CoroutineDispatcher, connection: Connection, channel: Channel<suspend Write.() -> (Unit)>) {
    CoroutineScope(dispatcher).launch {
        try {
            channel.consumeEach { connection.write.it() }
        } catch (_: Exception) {
        } finally {
            channel.close()
            connection.close()
        }
    }
}

fun Int.cardinality() = (0..<N).count { this and (1 shl it) > 0 }
fun Int.forEach(block: (Int) -> (Unit)) {
    (0..<N).forEach { if (this and (1 shl it) > 0) block(it) }
}
fun Int.count(block: (Int) -> (Boolean)): Int {
    var count = 0
    (0..<N).forEach { if (this and (1 shl it) > 0 && block(it)) count++ }
    return count
}
fun Int.binaryMask(): String {
    var str = ""
    (0..<N).forEach { if (this and (1 shl it) > 0) str += "1" else str += "0" }
    return str
}

class WaitGroup(private val n: Int) {
    private val channel = Channel<Unit>(UNLIMITED)
    val awaiting: AtomicBoolean = AtomicBoolean(false)
    val count: AtomicInteger = AtomicInteger(n)
    val mask: AtomicInteger = AtomicInteger(0)
    suspend fun await(): Int {
        if (!awaiting.compareAndSet(false, true)) error("already waiting")
        while (count.get() != 0) channel.receive()
        if (!awaiting.compareAndSet(true, false)) error("not waiting")
        var v = count.get()
        while (!count.compareAndSet(v, n)) v = count.get()
        v = mask.get()
        while (!mask.compareAndSet(v, 0)) v = mask.get()
        return v
    }
    suspend fun decrement(nodeId: Int) {
        if (count.get() == 0) return
        var value = mask.get()
        while (value.cardinality() < N - F && !mask.compareAndSet(value, value or (1 shl nodeId))) value = mask.get()
        count.decrementAndGet()
        if (awaiting.get()) channel.send(Unit)
    }
}

suspend fun Node(dispatcher: CoroutineDispatcher, provider: Provider, nodeId: Int): suspend () -> (Unit) {
    var slot = 0
    var phase = 0
    val entries = ConcurrentHashMap<Long, Request>()
    val logged = HashSet<Long>()
    val log = Array<Long?>(LOG_COUNT) { null }
    val queue = PriorityBlockingQueue<Request>(1) { a, b ->
        val at = a.id.time()
        val bt = b.id.time()
        val difference = (at - bt).toInt()
        if (difference == 0) {
            val ar = a.id.random()
            val br = b.id.random()
            if (ar > br) 1 else -1
        } else difference
    }
    val lock = Mutex()
    val proposalGroups = Array(LOG_COUNT) { WaitGroup(N - F) }
    val stateGroups = Array(LOG_COUNT * PHASE_COUNT) { WaitGroup(N - F) }
    val voteGroups = Array(LOG_COUNT * PHASE_COUNT) { WaitGroup(N - F) }
    val proposals = Array<Long?>(LOG_COUNT * N) { null }
    val states = Array<Int?>(LOG_COUNT * PHASE_COUNT * N) { null }
    val votes = Array<Int?>(LOG_COUNT * PHASE_COUNT * N) { null }
    val randoms = Array(LOG_COUNT) { Random(it.toLong()) }
    val replicas = HashSet<Channel<suspend Write.() -> (Unit)>>()
    println("N / 2 + 1: ${N / 2 + 1}")
    suspend fun commit(input: Request, output: Long?) {
        lock.withLock {
            val logSlot = slot
            slot = (slot + 1) % LOG_COUNT
            log[logSlot] = output
            log[slot] = null
            println("[$nodeId] Log[$logSlot] = ${output?.idStr()}")
        }
        if (output != input.id) {
            if (output != null) logged.add(output)
            queue.add(input)
        }
    }
    CoroutineScope(dispatcher).launch {
        while (true) {
            val slotSnapshot = slot
            val input = queue.poll()
            if (input == null) { LockSupport.parkNanos(1000); continue }
            if (logged.remove(input.id)) continue
            lock.withLock { phase = 0 }

//            println("Input: ${input}")

            replicas.forEach { channel ->
                channel.trySend {
                    byte(PROPOSAL)
                    int(slotSnapshot)
                    long(input.id)
                }
            }
            var mask = proposalGroups[slot].await()
//            println("[$nodeId] Proposals: ${mask.binaryMask()}")

            val distinctProposals = HashSet<Long>()
            lock.withLock {
                mask.forEach {
                    val proposal = proposals[slot * N + it] ?: return@forEach
                    distinctProposals.add(proposal)
                }
            }
            val countProposals = lock.withLock { distinctProposals.associateWith { proposal ->
                mask.count { proposals[slot * N + it] == proposal }
            } }
            var state = if ((countProposals[input.id] ?: 0) >= N / 2 + 1) 1 else 0
            var output: Long? = null
            lock.withLock { phase++ }
            while (true) {
                val phaseSnapshot = phase

                replicas.forEach { channel ->
                    channel.trySend {
                        byte(STATE)
                        int(slotSnapshot)
                        int(phaseSnapshot)
                        int(state)
                    }
                }
                mask = stateGroups[slot * PHASE_COUNT + phase].await()
//                println("[$nodeId] States: ${mask.binaryMask()}")

                val sZeros = lock.withLock { mask.count { states[slot * PHASE_COUNT * N + phase * N + it] == 0 } }
                val sOnes = lock.withLock { mask.count { states[slot * PHASE_COUNT * N + phase * N + it] == 1 } }
                val vote =
                    if (sZeros >= N / 2 + 1) 0
                    else if (sOnes >= N / 2 + 1) 1
                    else 2

                replicas.forEach { channel ->
                    channel.trySend {
                        byte(VOTE)
                        int(slotSnapshot)
                        int(phaseSnapshot)
                        int(vote)
                    }
                }
                mask = voteGroups[slot * PHASE_COUNT + phase].await()
//                println("[$nodeId] Votes: ${mask.binaryMask()}")

                val vZeros = lock.withLock { mask.count { votes[slot * PHASE_COUNT * N + phase * N + it] == 0 } }
                val vOnes = lock.withLock { mask.count { votes[slot * PHASE_COUNT * N + phase * N + it] == 1 } }
                if (vZeros >= F + 1 || vOnes >= F + 1) {
                    if (vZeros >= F + 1) break
                    output = distinctProposals.firstOrNull {
                        (countProposals[it] ?: 0) >= N / 2 + 1
                    }
                    break
                } else if (vZeros > 0 || vOnes > 0) {
                    state = if (vZeros > 0) 0 else 1
                } else {
                    state = if (randoms[slot].nextDouble() > 0.5) 1 else 0
                }
                lock.withLock { phase++ }
            }
            commit(input, output)
        }
    }
    CoroutineScope(dispatcher).launch {
        while (isActive && provider.isOpen) {
            val connection =
                try { provider.accept(InetSocketAddress("127.0.0.1", 9999 - nodeId)) }
                catch (e: Throwable) { continue }
            val channel = Channel<suspend Write.() -> (Unit)>(UNLIMITED)
            Writes(dispatcher, connection, channel)
            CoroutineScope(dispatcher).launch {
                try {
                    connection.read.apply {
                        val readNodeId = int()
                        while (isActive) {
                            when (byte()) {
                                REQUEST -> {
                                    val request = request()
                                    entries[request.id] = request
                                    queue.add(request)
                                    replicas.forEach { channel ->
                                        channel.trySend {
                                            byte(FORWARD)
                                            request(request)
                                        }
                                    }
                                }
                                FORWARD -> {
                                    val request = request()
                                    entries[request.id] = request
                                    queue.add(request)
                                }
                                PROPOSAL -> {
                                    val readSlot = int()
                                    val readId = long()
                                    lock.withLock {
                                        val group = proposalGroups[readSlot]
                                        if (readSlot >= slot) {
                                            proposals[readSlot * N + readNodeId] = readId
                                            group.decrement(readNodeId)
                                        }
                                    }
                                }
                                STATE -> {
                                    val readSlot = int()
                                    val readPhase = int()
                                    val readState = int()
                                    lock.withLock {
                                        val group = stateGroups[readSlot * PHASE_COUNT + readPhase]
                                        if ((readSlot > slot || (readSlot == slot && readPhase >= phase))) {
                                            states[readSlot * PHASE_COUNT * N + readPhase * N + readNodeId] = readState
                                            group.decrement(readNodeId)
                                        }
                                    }
                                }
                                VOTE -> {
                                    val readSlot = int()
                                    val readPhase = int()
                                    val readVote = int()
                                    lock.withLock {
                                        val group = voteGroups[readSlot * PHASE_COUNT + readPhase]
                                        if ((readSlot > slot || (readSlot == slot && readPhase >= phase))) {
                                            votes[readSlot * PHASE_COUNT * N + readPhase * N + readNodeId] = readVote
                                            group.decrement(readNodeId)
                                        }
                                    }
                                }
                            }
                        }
                    }
                } catch (_: Exception) {
                } finally {
                    channel.close()
                    connection.close()
                }
            }
        }
    }

    return {
        (0..<N).forEach {
            if (it == nodeId) return@forEach
            val connection =
                try { provider.connect(InetSocketAddress("127.0.0.1", 9999 - it)) }
                catch (e: Throwable) { return@forEach }
            val channel = Channel<suspend Write.() -> (Unit)>(UNLIMITED)
            Writes(dispatcher, connection, channel)
            replicas.add(channel)
            channel.trySend { int(nodeId) }
        }
    }
}

suspend fun main() {
    val service = Executors.newCachedThreadPool()
    val dispatcher = service.asCoroutineDispatcher()
    val group = withContext(Dispatchers.IO) {
        AsynchronousChannelGroup.withThreadPool(service)
    }
    val provider = SocketProvider(Short.MAX_VALUE.toInt(), group) {
        it.setOption(StandardSocketOptions.SO_KEEPALIVE, true)
    }
    CoroutineScope(dispatcher).launch {
        (0..<N).map { Node(dispatcher, provider, it) }.forEach { populate -> populate() }
    }
    delay(500)
    CoroutineScope(dispatcher).launch {
        // User client
        var node = 0
        val replicas = (0..<N).mapNotNull {
            val connection =
                try {
                    provider.connect(InetSocketAddress("127.0.0.1", 9999 - it))
                } catch (e: Throwable) {
                    return@mapNotNull null
                }
            Channel<suspend Write.() -> (Unit)>(UNLIMITED).also { channel ->
                Writes(dispatcher, connection, channel)
                channel.trySend { int(-1) }
            }
        }
        for (i in 0..<512) {
            //round robin
            val a = node++
            val id = id()
            replicas[a % N].trySend {
                byte(REQUEST)
                request(Request(id, "Hello $a"))
            }
        }
    }
}