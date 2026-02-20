import Foundation
import HaishinKit
import libsrt
import Logboard

protocol SRTSocketDelegate: AnyObject {
    func socket(_ socket: SRTSocket, status: SRT_SOCKSTATUS)
    func socket(_ socket: SRTSocket, incomingDataAvailabled data: Data, bytes: Int32)
    func socket(_ socket: SRTSocket, didAcceptSocket client: SRTSocket)
}

final class SRTSocket {

    static let defaultOptions: [SRTSocketOption: Any] = [:]
    static let payloadSize: Int = 1316

    var timeout: Int = 0
    var options: [SRTSocketOption: Any] = [:]
    weak var delegate: (any SRTSocketDelegate)?

    private(set) var mode: SRTMode = .caller

    // NOTE: perf est rempli par srt_bstats(). On le protège via srtQueue.
    private(set) var perf: CBytePerfMon = .init()

    private(set) var isRunning: HaishinKit.Atomic<Bool> = .init(false)

    // Empêche les sends/reads pendant close()
    private var isClosing: HaishinKit.Atomic<Bool> = .init(false)

    // ✅ Queue unique pour TOUS les appels libsrt
    private let srtQueue = DispatchQueue(
        label: "com.haishinkit.SRTHaishinKit.SRTSocket.srt",
        qos: .userInitiated
    )

    private(set) var socket: SRTSOCKET = SRT_INVALID_SOCK

    private(set) var status: SRT_SOCKSTATUS = SRTS_INIT {
        didSet {
            guard status != oldValue else { return }

            switch status {
            case SRTS_INIT:
                logger.trace("SRT Socket Init")
            case SRTS_OPENED:
                logger.info("SRT Socket opened")
            case SRTS_LISTENING:
                logger.trace("SRT Socket Listening")
            case SRTS_CONNECTING:
                logger.trace("SRT Socket Connecting")
            case SRTS_CONNECTED:
                logger.info("SRT Socket Connected")
            case SRTS_BROKEN:
                logger.warn("SRT Socket Broken")
                close()
            case SRTS_CLOSING:
                logger.trace("SRT Socket Closing")
            case SRTS_CLOSED:
                logger.info("SRT Socket Closed")
                stopRunning()
            case SRTS_NONEXIST:
                logger.warn("SRT Socket Not Exist")
            default:
                break
            }

            delegate?.socket(self, status: status)
        }
    }

    private var windowSizeC: Int32 = 1024 * 4

    // outgoingBuffer: accès UNIQUEMENT via outgoingQueue
    private var outgoingBuffer: [Data] = []
    private lazy var incomingBuffer: Data = .init(count: Int(windowSizeC))

    private let outgoingQueue = DispatchQueue(
        label: "com.haishinkit.SRTHaishinKit.SRTSocket.outgoing",
        qos: .userInitiated
    )

    private let incomingQueue = DispatchQueue(
        label: "com.haishinkit.SRTHaishinKit.SRTSocket.incoming",
        qos: .userInitiated
    )

    init() {}

    init(socket: SRTSOCKET) throws {
        self.socket = socket
        guard configure(.post) else {
            throw makeSocketError()
        }
        if incomingBuffer.count < windowSizeC {
            incomingBuffer = .init(count: Int(windowSizeC))
        }
        startRunning(name: nil)
    }

    // MARK: - Open

    func open(_ addr: sockaddr_in,
              mode: SRTMode,
              options: [SRTSocketOption: Any] = SRTSocket.defaultOptions) throws {

        // toute init/close socket = srtQueue
        try srtQueue.sync {
            guard socket == SRT_INVALID_SOCK else { return }

            self.mode = mode
            socket = srt_create_socket()

            guard socket != SRT_INVALID_SOCK else {
                throw makeSocketError()
            }

            self.options = options
            guard configure(.pre) else { throw makeSocketError() }

            var addr_cp = addr
            let stat = withUnsafePointer(to: &addr_cp) { ptr -> Int32 in
                let psa = UnsafeRawPointer(ptr).assumingMemoryBound(to: sockaddr.self)
                return mode.open(socket, psa, Int32(MemoryLayout.size(ofValue: addr)))
            }

            guard stat != SRT_ERROR else {
                throw makeSocketError()
            }

            switch mode {
            case .caller:
                guard configure(.post) else { throw makeSocketError() }
                if incomingBuffer.count < windowSizeC {
                    incomingBuffer = .init(count: Int(windowSizeC))
                }

            case .listener:
                let listenStat = srt_listen(socket, 1)
                guard listenStat != SRT_ERROR else {
                    srt_close(socket)
                    socket = SRT_INVALID_SOCK
                    throw makeSocketError()
                }
            }
        }

        startRunning(name: nil)
    }

    // MARK: - Output

    func doOutput(data: Data) {
        outgoingQueue.async { [weak self] in
            guard let self else { return }
            guard !self.isClosing.value else { return }

            // buffer géré uniquement ici
            self.outgoingBuffer.append(contentsOf: data.chunk(Self.payloadSize))

            while !self.outgoingBuffer.isEmpty {
                if self.isClosing.value { break }

                var chunk = self.outgoingBuffer[0]

                let sent: Int32 = self.srtQueue.sync {
                    guard self.socket != SRT_INVALID_SOCK else { return SRT_ERROR }
                    return self._sendmsg2(&chunk)
                }

                if sent == SRT_ERROR {
                    break
                }

                self.outgoingBuffer.removeFirst()
            }
        }
    }

    // MARK: - Input

    func doInput() {
        incomingQueue.async { [weak self] in
            guard let self else { return }

            while self.isRunning.value {
                if self.isClosing.value { break }

                let result: Int32 = self.srtQueue.sync {
                    guard self.socket != SRT_INVALID_SOCK else { return SRT_ERROR }
                    return self._recvmsg()
                }

                if result > 0 {
                    // Important: on copie le payload “utile”
                    let packet = Data(self.incomingBuffer.prefix(Int(result)))
                    self.delegate?.socket(self, incomingDataAvailabled: packet, bytes: result)
                } else {
                    usleep(5_000) // anti busy-loop
                }
            }
        }
    }

    // MARK: - Close

    func close() {
        // 1) stop nouveaux envois / lectures
        isClosing.mutate { $0 = true }
        stopRunning()

        // 2) attendre la fin des envois en cours + vider buffer
        outgoingQueue.sync {
            outgoingBuffer.removeAll()
        }

        // 3) fermer socket côté libsrt
        srtQueue.sync {
            guard socket != SRT_INVALID_SOCK else { return }
            srt_close(socket)
            socket = SRT_INVALID_SOCK
        }

        isClosing.mutate { $0 = false }
    }

    // MARK: - Options / Stats

    func configure(_ binding: SRTSocketOption.Binding) -> Bool {
        // configure touche libsrt => protéger
        return srtQueue.sync {
            let failures = SRTSocketOption.configure(socket, binding: binding, options: options)
            guard failures.isEmpty else {
                logger.error(failures)
                return false
            }
            return true
        }
    }

    func bstats() -> Int32 {
        return srtQueue.sync {
            guard socket != SRT_INVALID_SOCK else { return SRT_ERROR }
            return srt_bstats(socket, &perf, 1)
        }
    }

    private func accept() {
        // accept = libsrt => srtQueue
        let clientSock: SRTSOCKET = srtQueue.sync {
            guard socket != SRT_INVALID_SOCK else { return SRT_INVALID_SOCK }
            return srt_accept(socket, nil, nil)
        }
        guard clientSock != SRT_INVALID_SOCK else { return }

        do {
            delegate?.socket(self, didAcceptSocket: try SRTSocket(socket: clientSock))
        } catch {
            logger.error(error)
        }
    }

    private func makeSocketError() -> SRTError {
        let error_message = String(cString: srt_getlasterror_str())
        logger.error(error_message)
        return SRTError.illegalState(message: error_message)
    }

    // MARK: - libsrt wrappers (appelés UNIQUEMENT depuis srtQueue)

    @inline(__always)
    private func _sendmsg2(_ data: inout Data) -> Int32 {
        guard socket != SRT_INVALID_SOCK else { return SRT_ERROR }
        return data.withUnsafeBytes { pointer in
            guard let buffer = pointer.baseAddress?.assumingMemoryBound(to: CChar.self) else {
                return SRT_ERROR
            }
            return srt_sendmsg2(socket, buffer, Int32(data.count), nil)
        }
    }

    @inline(__always)
    private func _recvmsg() -> Int32 {
        guard socket != SRT_INVALID_SOCK else { return SRT_ERROR }
        return incomingBuffer.withUnsafeMutableBytes { pointer in
            guard let buffer = pointer.baseAddress?.assumingMemoryBound(to: CChar.self) else {
                return SRT_ERROR
            }
            return srt_recvmsg(socket, buffer, windowSizeC)
        }
    }
}

// MARK: - Running

extension SRTSocket: Running {

    func startRunning(name: String?) {
        guard !isRunning.value else { return }

        isRunning.mutate { $0 = true }

        DispatchQueue(label: "com.haishkinkit.SRTHaishinKit.SRTSocket.runloop")
            .async { [weak self] in
                guard let self else { return }

                while self.isRunning.value {
                    if self.isClosing.value { break }

                    let state: SRT_SOCKSTATUS = self.srtQueue.sync {
                        guard self.socket != SRT_INVALID_SOCK else { return SRTS_NONEXIST }
                        return srt_getsockstate(self.socket)
                    }

                    self.status = state

                    if self.mode == .listener, state == SRTS_LISTENING {
                        self.accept()
                    }

                    usleep(30_000)
                }
            }
    }

    func stopRunning() {
        guard isRunning.value else { return }
        isRunning.mutate { $0 = false }
    }
}