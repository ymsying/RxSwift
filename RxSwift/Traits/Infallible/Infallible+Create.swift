//
//  Infallible+Create.swift
//  RxSwift
//
//  Created by Shai Mishali on 27/08/2020.
//  Copyright © 2020 Krunoslav Zaher. All rights reserved.
//

import Foundation

public enum InfallibleEvent<Element> {
    /// Next element is produced.
    case next(Element)

    /// Sequence completed successfully.
    case completed
}

extension Infallible {
    public typealias InfallibleObserver = (InfallibleEvent<Element>) -> Void

    /**
     Creates an observable sequence from a specified subscribe method implementation.

     - seealso: [create operator on reactivex.io](http://reactivex.io/documentation/operators/create.html)

     - parameter subscribe: Implementation of the resulting observable sequence's `subscribe` method.
     - returns: The observable sequence with the specified implementation for the `subscribe` method.
     */
    public static func create(subscribe: @escaping (@escaping InfallibleObserver) -> Disposable) -> Infallible<Element> {
        
        // source为AnonymousObservable类型，
        // 返回的 observer 为AnyObserver(AnonymousObservableSink)
        let source = Observable<Element>.create { observer in
            subscribe { event in // 初始化函数中的调用会会传递回event
                switch event {
                case .next(let element):
                    observer.onNext(element) // 发给AnyObserver，
                    // AnyObserver的event被传递给AnonymousObservableSink的`.on`方法
                case .completed:
                    observer.onCompleted()
                }
            }
        }
        // 包装后返回
        return Infallible(source)
    }
}

extension InfallibleEvent: EventConvertible {
    public var event: Event<Element> {
        switch self {
        case let .next(element):
            return .next(element)
        case .completed:
            return .completed
        }
    }
}
