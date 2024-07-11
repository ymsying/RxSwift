//
//  ObserverBase.swift
//  RxSwift
//
//  Created by Krunoslav Zaher on 2/15/15.
//  Copyright © 2015 Krunoslav Zaher. All rights reserved.
//

class ObserverBase<Element> : Disposable, ObserverType {
    private let isStopped = AtomicInt(0)

    // sink.forwardOn调用，传入事件，
    func on(_ event: Event<Element>) {
        switch event {
        case .next:
            if load(self.isStopped) == 0 {
                // 调用具体子类的ocCore()
                self.onCore(event)
            }
        case .error, .completed:
            if fetchOr(self.isStopped, 1) == 0 {
                self.onCore(event)
            }
        }
    }

    func onCore(_ event: Event<Element>) {
        rxAbstractMethod()
    }

    func dispose() {
        fetchOr(self.isStopped, 1)
    }
}
