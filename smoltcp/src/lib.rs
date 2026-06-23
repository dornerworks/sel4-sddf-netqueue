//
// Copyright 2023, Colias Group, LLC
// Copyright 2026, DornerWorks, LLC
//
// SPDX-License-Identifier: BSD-2-Clause
//

#![no_std]

use core::u8;
use lock_api::{Mutex, RawMutex};
use one_shot_mutex::unsync::RawOneShotMutex;

use smoltcp::phy::{self, Device, DeviceCapabilities};
use smoltcp::time::Instant;

use sel4_abstract_rc::{AbstractRcT, RcT};
use sel4_sddf_netqueue::NetQueueHandle;
use sel4_shared_memory::SharedMemoryRef;

pub struct Inner {
    rx_dma_region: SharedMemoryRef<'static, [u8]>,
    tx_dma_region: SharedMemoryRef<'static, [u8]>,
    rx_ring_buffers: NetQueueHandle<'static>,
    tx_ring_buffers: NetQueueHandle<'static>,
    caps: DeviceCapabilities,
    tx_notify: fn(),
}

impl Inner {
    pub fn new(
        rx_dma_region: SharedMemoryRef<'static, [u8]>,
        tx_dma_region: SharedMemoryRef<'static, [u8]>,
        rx_ring_buffers: NetQueueHandle<'static>,
        tx_ring_buffers: NetQueueHandle<'static>,
        caps: DeviceCapabilities,
        tx_notify: fn(),
    ) -> Self {
        Self {
            rx_dma_region,
            tx_dma_region,
            rx_ring_buffers,
            tx_ring_buffers,
            caps,
            tx_notify,
        }
    }

    pub fn tx_notify(&self) {
        (self.tx_notify)();
    }
}

pub struct DeviceImpl<R: RawMutex = RawOneShotMutex, P: AbstractRcT = RcT> {
    inner: P::Rc<Mutex<R, Inner>>,
}

impl<R: RawMutex, P: AbstractRcT> Clone for DeviceImpl<R, P> {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

impl<R: RawMutex, P: AbstractRcT> DeviceImpl<R, P> {
    pub fn new(
        raw_mutex: R,
        rx_dma_region: SharedMemoryRef<'static, [u8]>,
        tx_dma_region: SharedMemoryRef<'static, [u8]>,
        rx_ring_buffers: NetQueueHandle<'static>,
        tx_ring_buffers: NetQueueHandle<'static>,
        caps: DeviceCapabilities,
        tx_notify: fn(),
    ) -> Self {
        Self {
            inner: P::Rc::from(Mutex::from_raw(
                raw_mutex,
                Inner::new(
                    rx_dma_region,
                    tx_dma_region,
                    rx_ring_buffers,
                    tx_ring_buffers,
                    caps,
                    tx_notify,
                ),
            )),
        }
    }

    fn inner(&self) -> &P::Rc<Mutex<R, Inner>> {
        &self.inner
    }

    pub fn rx_available(&self) -> bool {
        !self.inner().lock().rx_ring_buffers.active.is_empty()
    }

    pub fn tx_available(&self) -> bool {
        !self.inner().lock().tx_ring_buffers.free.is_empty()
    }

    pub fn consume_rx<T, F>(&mut self, f: F) -> T
    where
        F: FnOnce(&mut [u8]) -> T,
    {
        let mut desc = self
            .inner()
            .lock()
            .rx_ring_buffers
            .active
            .dequeue()
            .unwrap();
        let start_index = desc.io_or_offset() as usize;
        let end_index: usize = desc.io_or_offset() as usize + desc.len() as usize;
        let mut packet = self
            .inner()
            .lock()
            .rx_dma_region
            .as_mut_ptr()
            .index(start_index..end_index)
            .as_raw_ptr();

        let result = f(unsafe { packet.as_mut() });

        desc.set_len(0);
        if self
            .inner()
            .lock()
            .rx_ring_buffers
            .free
            .enqueue(desc)
            .is_ok()
        {
            self.inner().lock().rx_ring_buffers.active.request_signal();

            if self.inner().lock().rx_ring_buffers.free.require_signal() {
                self.inner().lock().tx_ring_buffers.free.cancel_signal();
            }
        }

        result
    }

    pub fn consume_tx<T, F>(&mut self, len: usize, f: F) -> T
    where
        F: FnOnce(&mut [u8]) -> T,
    {
        let mut desc = self.inner().lock().tx_ring_buffers.free.dequeue().unwrap();
        let start_index = desc.io_or_offset() as usize;
        let end_index: usize = desc.io_or_offset() as usize + len;
        let mut buf = self
            .inner()
            .lock()
            .tx_dma_region
            .as_mut_ptr()
            .index(start_index..end_index)
            .as_raw_ptr();
        desc.set_len(len as u16);

        let result = f(unsafe { buf.as_mut() });

        if self
            .inner()
            .lock()
            .tx_ring_buffers
            .active
            .enqueue(desc)
            .is_ok()
        {
            if self.inner().lock().tx_ring_buffers.active.require_signal() {
                self.inner().lock().tx_ring_buffers.active.cancel_signal();
                self.inner().lock().tx_notify();
            }
        }
        result
    }
}

impl<R: RawMutex, P: AbstractRcT> Device for DeviceImpl<R, P> {
    type RxToken<'a>
        = RxToken<R, P>
    where
        R: 'a,
        P: 'a;
    type TxToken<'a>
        = TxToken<R, P>
    where
        R: 'a,
        P: 'a;

    fn capabilities(&self) -> DeviceCapabilities {
        self.inner().lock().caps.clone()
    }

    fn receive(&mut self, _timestamp: Instant) -> Option<(Self::RxToken<'_>, Self::TxToken<'_>)> {
        if self.tx_available() && self.rx_available() {
            let rx = RxToken {
                shared: self.clone(),
            };

            let tx = TxToken {
                shared: self.clone(),
            };
            Some((rx, tx))
        } else {
            None
        }
    }

    fn transmit(&mut self, _timestamp: Instant) -> Option<Self::TxToken<'_>> {
        if self.tx_available() {
            Some(TxToken {
                shared: self.clone(),
            })
        } else {
            None
        }
    }
}

pub struct RxToken<R: RawMutex, P: AbstractRcT> {
    shared: DeviceImpl<R, P>,
}

impl<R: RawMutex, P: AbstractRcT> phy::RxToken for RxToken<R, P> {
    fn consume<T, F>(mut self, f: F) -> T
    where
        F: FnOnce(&mut [u8]) -> T,
    {
        self.shared.consume_rx(f)
    }
}

pub struct TxToken<R: RawMutex, P: AbstractRcT> {
    shared: DeviceImpl<R, P>,
}

impl<R: RawMutex, P: AbstractRcT> phy::TxToken for TxToken<R, P> {
    fn consume<T, F>(mut self, len: usize, f: F) -> T
    where
        F: FnOnce(&mut [u8]) -> T,
    {
        self.shared.consume_tx(len, f)
    }
}
