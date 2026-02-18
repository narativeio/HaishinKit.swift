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
    private(set) var perf: CBytePerfMon = .init()
    private(set) var isRunning: HaishinKit.Atomic<Bool> = .init(false)
    private(set) var socket: SRTSOCKET = SRT_INVALID_SOCK

    private let isClosing = HaishinKit.Atomic<Bool>(false)

    /// ✅ UNIQUE libsrt queue
    private let srtQueue = DispatchQueue(
        label: "com.haishinkit.SRTHaishinKit.SRTSocket.srt",
        qos: .userInitiated
    )

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

    // MARK: - Init

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
            guard configure(.post) else {
                throw makeSocketError()
            }
        case .listener:
            let listenStat = srt_listen(socket, 1)
            guard listenStat != SRT_ERROR else {
                srt_close(socket)
                throw makeSocketError()
            }
        }

        startRunning(name: nil)
    }

    // MARK: - Output

    func doOutput(data: Data) {
        outgoingQueue.async { [weak self] in
            guard let self else { return }
            guard !self.isClosing.value else { return }

            // ✅ outgoingBuffer TOUJOURS touché uniquement sur outgoingQueue
            self.outgoingBuffer.append(contentsOf: data.chunk(Self.payloadSize))

            while !self.outgoingBuffer.isEmpty {
                if self.isClosing.value { break }
                if self.socket == SRT_INVALID_SOCK { break }

                var chunk = self.outgoingBuffer[0]

                let sent: Int32 = self.srtQueue.sync {
                    guard self.socket != SRT_INVALID_SOCK else { return SRT_ERROR }
                    return chunk.withUnsafeBytes { ptr in
                        guard let buffer = ptr.baseAddress?.assumingMemoryBound(to: CChar.self) else {
                            return SRT_ERROR
                        }
                        return srt_sendmsg2(self.socket, buffer, Int32(chunk.count), nil)
                    }
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

                let result: Int32 = self.srtQueue.sync {
                    guard self.socket != SRT_INVALID_SOCK else { return SRT_ERROR }
                    return self.incomingBuffer.withUnsafeMutableBytes { ptr in
                        guard let buffer = ptr.baseAddress?.assumingMemoryBound(to: CChar.self) else {
                            return SRT_ERROR
                        }
                        return srt_recvmsg(self.socket, buffer, self.windowSizeC)
                    }
                }

                if result > 0 {
                    self.delegate?.socket(self,
                        incomingDataAvailabled: self.incomingBuffer,
                        bytes: result)
                } else {
                    usleep(5_000)
                }
            }
        }
    }

    // MARK: - Stats

    func bstats() -> Int32 {
        return srtQueue.sync {
            guard socket != SRT_INVALID_SOCK else { return SRT_ERROR }
            return srt_bstats(socket, &perf, 1)
        }
    }

    // MARK: - Close

    func close() {
        // ✅ empêche tout nouveau send
        isClosing.mutate { $0 = true }

        // ✅ attend que tous les doOutput en cours finissent
        outgoingQueue.sync {
            outgoingBuffer.removeAll()
        }

        stopRunning()

        srtQueue.sync {
            guard socket != SRT_INVALID_SOCK else { return }
            srt_close(socket)
            socket = SRT_INVALID_SOCK
        }

        isClosing.mutate { $0 = false }
    }

    // MARK: - Helpers

    func configure(_ binding: SRTSocketOption.Binding) -> Bool {
        let failures = SRTSocketOption.configure(socket, binding: binding, options: options)
        guard failures.isEmpty else {
            logger.error(failures)
            return false
        }
        return true
    }

    private func makeSocketError() -> SRTError {
        let error_message = String(cString: srt_getlasterror_str())
        logger.error(error_message)
        return SRTError.illegalState(message: error_message)
    }
}

// MARK: - Running

extension SRTSocket: Running {

    func startRunning(name: String?) {
        guard !isRunning.value else { return }

        isRunning.mutate { $0 = true }

        DispatchQueue(label: "com.haishkinkit.SRTHaishinKit.SRTSocket.runloop")
            .async {

                repeat {

                    let state = self.srtQueue.sync {
                        srt_getsockstate(self.socket)
                    }

                    self.status = state

                    switch self.mode {
                    case .listener:
                        // si tu utilises accept() garde-le ici
                        break
                    default:
                        break
                    }

                    usleep(30_000)

                } while self.isRunning.value
            }
    }

    func stopRunning() {
        guard isRunning.value else { return }
        isRunning.mutate { $0 = false }
    }
}