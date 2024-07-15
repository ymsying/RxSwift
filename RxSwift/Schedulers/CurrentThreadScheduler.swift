//
//  CurrentThreadScheduler.swift
//  RxSwift
//
//  Created by Krunoslav Zaher on 8/30/15.
//  Copyright © 2015 Krunoslav Zaher. All rights reserved.
//

import Dispatch
import Foundation

#if os(Linux)
    fileprivate enum CurrentThreadSchedulerQueueKey {
        fileprivate static let instance = "RxSwift.CurrentThreadScheduler.Queue"
    }
#else
    private class CurrentThreadSchedulerQueueKey: NSObject, NSCopying {
        static let instance = CurrentThreadSchedulerQueueKey()
        private override init() {
            super.init()
        }

        override var hash: Int {
            return 0
        }

        public func copy(with zone: NSZone? = nil) -> Any {
            return self
        }
    }
#endif

// 获取当前进入的线程，查看排队任务，执行排队任务
/// Represents an object that schedules units of work on the current thread.
///
/// This is the default scheduler for operators that generate elements.
///
/// This scheduler is also sometimes called `trampoline scheduler`.
public class CurrentThreadScheduler : ImmediateSchedulerType {
    typealias ScheduleQueue = RxMutableBox<Queue<ScheduledItemType>>

    /// The singleton instance of the current thread scheduler.
    public static let instance = CurrentThreadScheduler()

    private static var isScheduleRequiredKey: pthread_key_t = { () -> pthread_key_t in
        let key = UnsafeMutablePointer<pthread_key_t>.allocate(capacity: 1)
        defer { key.deallocate() }
                                                               
        guard pthread_key_create(key, nil) == 0 else {
            rxFatalError("isScheduleRequired key creation failed")
        }

        return key.pointee
    }()

    private static var scheduleInProgressSentinel: UnsafeRawPointer = { () -> UnsafeRawPointer in
        return UnsafeRawPointer(UnsafeMutablePointer<Int>.allocate(capacity: 1))
    }()

    // 一个线程可以在多个队列中进行切换关联
    // 系统的Queue与此ScheduleQueue不用，ScheduleQueue可和平相处
    static var queue : ScheduleQueue? {
        get {
            return Thread.getThreadLocalStorageValueForKey(CurrentThreadSchedulerQueueKey.instance)
        }
        set {
            Thread.setThreadLocalStorageValue(newValue, forKey: CurrentThreadSchedulerQueueKey.instance)
        }
    }

    // 默认返回true
    /// Gets a value that indicates whether the caller must call a `schedule` method.
    public static private(set) var isScheduleRequired: Bool {
        get {
            return pthread_getspecific(CurrentThreadScheduler.isScheduleRequiredKey) == nil
        }
        set(isScheduleRequired) {
            if pthread_setspecific(CurrentThreadScheduler.isScheduleRequiredKey, isScheduleRequired ? nil : scheduleInProgressSentinel) != 0 {
                rxFatalError("pthread_setspecific failed")
            }
        }
    }

    /**
    Schedules an action to be executed as soon as possible on current thread.

    If this method is called on some thread that doesn't have `CurrentThreadScheduler` installed, scheduler will be
    automatically installed and uninstalled after all work is performed.

    - parameter state: State passed to the action to be executed.
    - parameter action: Action to be executed.
    - returns: The disposable object used to cancel the scheduled action (best effort).
    */
    public func schedule<StateType>(_ state: StateType, action: @escaping (StateType) -> Disposable) -> Disposable {
//        print("callStackSymbols: \n\(Thread.callStackSymbols.prefix(5))")
        
//        print("CurrentThreadScheduler --> schedule(...), \(mach_absolute_time())")
        // 主线程，可以新关联出主队列的队列吗？
        // !!!: 可以！！!
        // 在外部mainThread中使用subscribe(on：MainScheduler.instance)，内部就会建立自定义queue，
        
        // 如果当前线程关联的队列的人物已经全部执行完成，则执行if；如果还未全部执行完成，则不进入if
        
        if CurrentThreadScheduler.isScheduleRequired {
            CurrentThreadScheduler.isScheduleRequired = false
            
//            print("CurrentThreadScheduler --> action(...)")
            
            // 当前action执行期间，再次添加action，即可触发enqueue行为
            // 当前action内部触发subscribe
            // action内部调用异步队列任务，向当前线程插入异步
            let disposable = action(state)

            // 在队列任务出队完成后，将线程关联的对联解绑，设置为需要再次被调度
            defer {
//                print("CurrentThreadScheduler --> reset(...)")
                CurrentThreadScheduler.isScheduleRequired = true
                CurrentThreadScheduler.queue = nil
            }

            // ??: isScheduleRequired 设置为false，queue何时可以设置为有值
            // 要进行Queue的绑定，需要同时连续进入2+个任务，前一个isScheduleRequired=false，后面的进入Queue中
            guard let queue = CurrentThreadScheduler.queue else {
//                print("CurrentThreadScheduler --> no queue(...)")
                return disposable
            }

//            print("CurrentThreadScheduler --> while(...)")
            // 逐一将任务出队列，并在当前线程进行执行，
            while let latest = queue.value.dequeue() {
                if latest.isDisposed {
                    continue
                }
                latest.invoke()
            }

            return disposable
            // 执行线程与队列解绑操作
        }

        // 获取当前线程关联的队列
        let existingQueue = CurrentThreadScheduler.queue

        let queue: RxMutableBox<Queue<ScheduledItemType>>
        if let existingQueue = existingQueue {
            queue = existingQueue
        }
        else {
            // 队列不存在，则新建
            queue = RxMutableBox(Queue<ScheduledItemType>(capacity: 1))
            CurrentThreadScheduler.queue = queue
        }
        
//        print("CurrentThreadScheduler --> enqueue(...)")
        // 创建新的人物，并加入到队列中，等待在while循环中进行出队并执行
        let scheduledItem = ScheduledItem(action: action, state: state)
        queue.value.enqueue(scheduledItem)

        return scheduledItem
    }
}
