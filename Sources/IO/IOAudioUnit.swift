import AVFoundation
import SwiftUI

#if canImport(SwiftPMSupport)
import SwiftPMSupport
#endif

/// The IOAudioUnit  error domain codes.
public enum IOAudioUnitError: Swift.Error {
    /// The IOAudioUnit  failed to create the AVAudioConverter.
    case failedToCreate(from: AVAudioFormat?, to: AVAudioFormat?)
    /// The IOAudioUnit  faild to convert the an audio buffer.
    case failedToConvert(error: NSError)
}

protocol IOAudioUnitDelegate: AnyObject {
    func audioUnit(_ audioUnit: IOAudioUnit, errorOccurred error: IOAudioUnitError)
    func audioUnit(_ audioUnit: IOAudioUnit, didOutput audioBuffer: AVAudioPCMBuffer, when: AVAudioTime)
}

public final class IOAudioUnit: NSObject, IOUnit {
    typealias FormatDescription = AVAudioFormat

    let lockQueue = DispatchQueue(label: "com.haishinkit.HaishinKit.IOAudioUnit.lock")
    public var muted = false
    weak var mixer: IOMixer?
    var isMonitoringEnabled = false {
        didSet {
            if isMonitoringEnabled {
                monitor.startRunning()
            } else {
                monitor.stopRunning()
            }
        }
    }
    var settings: AudioCodecSettings = .default {
        didSet {
            codec.settings = settings
            resampler.settings = settings.makeAudioResamplerSettings()
        }
    }
    var isRunning: Atomic<Bool> {
        return codec.isRunning
    }
    private(set) var inputFormat: FormatDescription?
    var outputFormat: FormatDescription? {
        return codec.outputFormat
    }
    private lazy var codec: AudioCodec<IOMixer> = {
        var codec = AudioCodec<IOMixer>(lockQueue: lockQueue)
        codec.delegate = mixer
        return codec
    }()
    private lazy var resampler: IOAudioResampler<IOAudioUnit> = {
        var resampler = IOAudioResampler<IOAudioUnit>()
        resampler.delegate = self
        return resampler
    }()
    private var monitor: IOAudioMonitor = .init()
    #if os(tvOS)
    private var _capture: Any?
    @available(tvOS 17.0, *)
    private var capture: IOAudioCaptureUnit {
        if _capture == nil {
            _capture = IOAudioCaptureUnit()
        }
        return _capture as! IOAudioCaptureUnit
    }
    #elseif os(iOS) || os(macOS)
    private var capture: IOAudioCaptureUnit = .init()
    #endif

    #if os(iOS) || os(macOS) || os(tvOS)
    @available(tvOS 17.0, *)
    func attachAudio(_ device: AVCaptureDevice?, automaticallyConfiguresApplicationAudioSession: Bool) throws {
        try mixer?.session.configuration { session in
            guard let device else {
                try capture.attachDevice(nil, audioUnit: self)
                inputFormat = nil
                return
            }
            try capture.attachDevice(device, audioUnit: self)
            #if os(iOS)
            session.automaticallyConfiguresApplicationAudioSession = automaticallyConfiguresApplicationAudioSession
            #endif
        }
    }
    #endif

    func append(_ sampleBuffer: CMSampleBuffer, channel: UInt8 = 0) {
        switch sampleBuffer.formatDescription?.audioStreamBasicDescription?.mFormatID {
        case kAudioFormatLinearPCM:
            resampler.append(sampleBuffer.muted(muted))
        default:
            if codec.inputFormat?.formatDescription != sampleBuffer.formatDescription {
                if var asbd = sampleBuffer.formatDescription?.audioStreamBasicDescription {
                    codec.inputFormat = AVAudioFormat.init(streamDescription: &asbd)
                }
            }
            codec.append(sampleBuffer)
        }
    }

    func append(_ audioBuffer: AVAudioBuffer, when: AVAudioTime) {
        switch audioBuffer {
        case let audioBuffer as AVAudioPCMBuffer:
            resampler.append(audioBuffer, when: when)
        case let audioBuffer as AVAudioCompressedBuffer:
            codec.append(audioBuffer, when: when)
        default:
            break
        }
    }
}

#if os(iOS) || os(tvOS) || os(macOS)
@available(tvOS 17.0, *)
extension IOAudioUnit: AVCaptureAudioDataOutputSampleBufferDelegate {
    // MARK: AVCaptureAudioDataOutputSampleBufferDelegate
    public func captureOutput(_ output: AVCaptureOutput, didOutput sampleBuffer: CMSampleBuffer, from connection: AVCaptureConnection) {
        
        // TODO Create class Cleaner qui prend en input le sampleBuffer et retourne un sampleBuffer clean
        // Faire un passe plat par défaut, et ne faire des modifs que dans certaines conditions
        // If no zeros => [PTS suivant] = [PTS actuel] + [num samples actuel]
        // If zeros =>
        //      - [PTS suivant] = [PTS actuel] + [num samples actuel] - [nombre de zéros enlevés]
        //      - enlever les zéros du début du buffer
        
        resampler.append(sampleBuffer.muted(muted))
    }
}
#endif

extension IOAudioUnit: Running {
    // MARK: Running
    func startRunning(name: String? = nil) {
        codec.startRunning()
    }

    func stopRunning() {
        codec.stopRunning()
    }
}

extension IOAudioUnit: IOAudioResamplerDelegate {
    // MARK: IOAudioResamplerDelegate
    public func resampler(_ resampler: IOAudioResampler<IOAudioUnit>, errorOccurred error: IOAudioUnitError) {
        mixer?.audioUnit(self, errorOccurred: error)
    }

    public func resampler(_ resampler: IOAudioResampler<IOAudioUnit>, didOutput audioFormat: AVAudioFormat) {
        inputFormat = resampler.inputFormat
        codec.inputFormat = audioFormat
        monitor.inputFormat = audioFormat
    }

    public func resampler(_ resampler: IOAudioResampler<IOAudioUnit>, didOutput audioBuffer: AVAudioPCMBuffer, when: AVAudioTime) {
        mixer?.audioUnit(self, didOutput: audioBuffer, when: when)
        monitor.append(audioBuffer, when: when)
        codec.append(audioBuffer, when: when)
    }
}


public struct SampleDataPoint: Identifiable {
    public let date: Date = Date()
    public let value: Int

    public var id: Int { Int(date.timeIntervalSince1970) }

    public init(value: Int) {
        self.value = value
    }
}

@available(iOS 17.0, *)
@Observable public class SampleData {
    public static let shared = SampleData()
    public var values: [SampleDataPoint] = []
    public var audioStreamBasicDescription: AudioStreamBasicDescription? = .none
    public var commonFormat: AVAudioCommonFormat? = .none
    public var interleaved: Bool = false
    var tmpValues: [SampleDataPoint] = []
    var refreshCount = 0

    public var inter: String {
        var value: String = ""

        if interleaved {
            value = "true"
        } else {
            value = "false"
        }

        return value
    }

    public init() {}

    public init(values: [SampleDataPoint]) {
        self.values = values
    }

    func append(_ value: Int) {
        let point = SampleDataPoint(value: value)
        tmpValues.append(point)
        refreshCount += 1

        if tmpValues.count > 200 {
            tmpValues.removeFirst()
        }

        if self.refreshCount > 200 {
            self.refreshCount = 0
            self.values = []
            self.values = self.tmpValues
        }
    }
}
