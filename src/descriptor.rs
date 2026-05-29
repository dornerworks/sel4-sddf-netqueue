use zerocopy::{FromBytes, IntoBytes};

#[repr(C)]
#[derive(Debug, Copy, Clone, IntoBytes, FromBytes)]
pub struct NetBuffDesc {
    pub io_or_offset: u64,
    pub len: u16,
    _padding: [u8; 6],
}

impl NetBuffDesc {
    pub fn new(io_or_offset: u64, len: u16) -> Self {
        Self { io_or_offset, len, _padding: [0; 6] }
    }

    pub fn io_or_offset(&self) -> u64 {
        self.io_or_offset
    }

    pub fn set_io_or_offset(&mut self, io_or_offset: u64) {
        self.io_or_offset = io_or_offset;
    }

    pub fn len(&self) -> u16 {
        self.len
    }

    pub fn set_len(&mut self, len: u16) {
        self.len = len;
    }
}
