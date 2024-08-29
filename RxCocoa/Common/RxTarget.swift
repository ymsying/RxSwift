//
//  RxTarget.swift
//  RxCocoa
//
//  Created by Krunoslav Zaher on 7/12/15.
//  Copyright © 2015 Krunoslav Zaher. All rights reserved.
//

import Foundation

import RxSwift

class RxTarget : NSObject
               , Disposable // 遵从Disposable协议
{
    
    private var retainSelf: RxTarget?
    
    override init() {
        super.init()
        self.retainSelf = self // 自我引用，延长生命周期

#if TRACE_RESOURCES
        _ = Resources.incrementTotal()
#endif

#if DEBUG
        MainScheduler.ensureRunningOnMainThread()
#endif
    }
    
    func dispose() {
#if DEBUG
        MainScheduler.ensureRunningOnMainThread()
#endif
        self.retainSelf = nil // 手动触发dispose，释放对象
    }

#if TRACE_RESOURCES
    deinit {
        _ = Resources.decrementTotal()
    }
#endif
}
