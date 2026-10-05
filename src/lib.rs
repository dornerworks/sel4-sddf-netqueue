#![no_std]

mod descriptor;

pub use descriptor::NetBuffDesc;
use sel4_shared_memory::{SharedMemoryPtr, SharedMemoryRef, map_field};

#[repr(C)]
#[derive(Debug)]
pub struct RawNetQueue {
    pub tail: u16,
    pub head: u16,
    pub consumer_signalled: u32,
    /* This is actually an array, but we do not know the size at compile time. */
    pub buffers: NetBuffDesc,
}

pub struct NetQueue<'a> {
    inner: SharedMemoryRef<'a, RawNetQueue>,
    capacity: u32,
}

impl<'a> NetQueue<'a> {
    pub fn new(ptr: SharedMemoryRef<'a, RawNetQueue>, capacity: u32) -> Self {
        Self {
            inner: ptr,
            capacity: capacity,
        }
    }

    pub fn get_head(&self) -> u16 {
        let ptr = self.inner.as_ptr();
        map_field!(ptr.head).read()
    }

    pub fn set_head(&mut self, value: u16) {
        let ptr = self.inner.as_mut_ptr();
        map_field!(ptr.head).write(value);
    }

    pub fn get_tail(&self) -> u16 {
        let ptr = self.inner.as_ptr();
        map_field!(ptr.tail).read()
    }

    pub fn set_tail(&mut self, value: u16) {
        let ptr = self.inner.as_mut_ptr();
        map_field!(ptr.tail).write(value);
    }

    pub fn get_consumer_signalled(&self) -> u32 {
        let ptr = self.inner.as_ptr();
        map_field!(ptr.consumer_signalled).read()
    }

    pub fn set_consumer_signalled(&mut self, value: u32) {
        let ptr = self.inner.as_mut_ptr();
        map_field!(ptr.consumer_signalled).write(value);
    }

    pub fn request_signal(&mut self) {
        let ptr = self.inner.as_mut_ptr();
        map_field!(ptr.consumer_signalled).write(0);
    }

    pub fn cancel_signal(&mut self) {
        let ptr = self.inner.as_mut_ptr();
        map_field!(ptr.consumer_signalled).write(1);
    }

    pub fn require_signal(&self) -> bool {
        let ptr = self.inner.as_ptr();
        map_field!(ptr.consumer_signalled).read() == 0
    }

    fn length(&self) -> u16 {
        let ptr = self.inner.as_ptr();
        map_field!(ptr.tail).read() - map_field!(ptr.head).read()
    }

    pub fn is_empty(&self) -> bool {
        self.length() == 0
    }

    pub fn is_full(&self) -> bool {
        u32::from(self.length()) == self.capacity
    }

    fn get_mut_buffer(&mut self, index: u16) -> SharedMemoryPtr<'_, NetBuffDesc> {
        let index = ((index as u32) % self.capacity) as usize;
        let ptr = self.inner.as_mut_ptr();

        unsafe {
            map_field!(ptr.buffers)
                .map(|arr| core::ptr::NonNull::new(arr.as_ptr().add(index)).unwrap())
        }
    }

    pub fn enqueue(&mut self, buffer: NetBuffDesc) -> Result<(), ()> {
        if self.is_full() {
            Err(())
        } else {
            let tail = self.get_tail();
            let buf = self.get_mut_buffer(tail);
            buf.write(buffer);
            self.set_tail(tail + 1);
            Ok(())
        }
    }

    pub fn dequeue(&mut self) -> Option<NetBuffDesc> {
        if self.is_empty() {
            None
        } else {
            let head = self.get_head();
            let buf = self.get_mut_buffer(head);
            let buffer = buf.read();
            self.set_head(head + 1);
            Some(buffer)
        }
    }
}

pub struct NetQueueHandle<'a> {
    pub free: NetQueue<'a>,
    pub active: NetQueue<'a>,
    pub capacity: u32,
}

impl<'a> NetQueueHandle<'a> {
    pub fn new(free: NetQueue<'a>, active: NetQueue<'a>, capacity: u32) -> Self {
        Self {
            free,
            active,
            capacity,
        }
    }

    pub fn from_ptrs(
        free: SharedMemoryRef<'a, RawNetQueue>,
        active: SharedMemoryRef<'a, RawNetQueue>,
        capacity: u32,
        buffer_size: Option<u64>,
    ) -> Self {
        let mut free = NetQueue::new(free, capacity);
        let active = NetQueue::new(active, capacity);
        if let Some(size) = buffer_size {
            for i in 0..capacity {
                let desc: NetBuffDesc = NetBuffDesc::new((i as u64) * size, 0);
                if free.enqueue(desc).is_err() {
                    panic!("Not enough room to fill free queue");
                };
            }
        }
        Self {
            free,
            active,
            capacity,
        }
    }
}
