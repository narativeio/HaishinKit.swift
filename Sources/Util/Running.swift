import Foundation

/// A type that methods for running.
public protocol Running: AnyObject {
    /// Indicates whether the receiver is running.
    public var isRunning: Atomic<Bool> { get }
    /// Tells the receiver to start running.
    public func startRunning(name: String?)
    /// Tells the receiver to stop running.
    public func stopRunning()
}
