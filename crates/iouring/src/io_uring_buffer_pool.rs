// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use crate::io_uring_selector::{
    Command, Controller, DriverGroup, multishot::pool::Group, provided_buffers::WorkerBuffers,
};
use core_scheduler::*;
use std::{
    io,
    sync::{
        Arc, Weak,
        atomic::{AtomicUsize, Ordering},
    },
};

pub(crate) const RECEIVE_METADATA: usize = 16 + std::mem::size_of::<libc::sockaddr_storage>();

/// A shared payload budget, or explicitly provisioned worker-owned storage. Ordinary pools use demand-driven monitor receives. Worker pools reserve a contiguous slab and provided-buffer ring for every worker at construction; each completed lease returns to its original slab from any thread.
#[derive(Clone)]
pub struct IoUringBufferPool(pub(crate) Arc<Pool>);

pub(crate) struct Pool {
    pub(crate) options: BufferPoolOptions,
    pub(crate) selector: Weak<DriverGroup>,
    storage: Storage,
    next: AtomicUsize,
    capacity: usize,
}

enum Storage {
    Shared(HeapBufferPool),
    Workers(Box<[Arc<WorkerBuffers>]>),
}

impl IoUringBufferPool {
    pub(crate) fn new(group: &Arc<DriverGroup>, options: BufferPoolOptions) -> io::Result<Self> {
        let heap = HeapBufferPool::with_options(options)?;
        Ok(Self(Arc::new(Pool {
            options,
            selector: Arc::downgrade(group),
            storage: Storage::Shared(heap),
            next: AtomicUsize::new(0),
            capacity: options.buffers,
        })))
    }

    pub(crate) fn per_worker(
        group: &Arc<DriverGroup>,
        controllers: &[Arc<Controller>],
        options: BufferPoolOptions,
    ) -> io::Result<Self> {
        let capacity = options.buffers.checked_mul(controllers.len());
        if options.buffers == 0
            || options.buffers > 32768
            || options.buffer_size == 0
            || !options.alignment.is_power_of_two()
            || capacity.is_none()
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "worker pools require 1..=32768 buffers per worker, a nonzero size, power-of-two alignment, and representable total capacity",
            ));
        }
        // recvmsg metadata precedes the usable payload. Round up its prefix so completed payloads preserve the requested alignment.
        let prefix = RECEIVE_METADATA
            .checked_add(options.alignment - 1)
            .map(|value| value & !(options.alignment - 1));
        let size = prefix
            .and_then(|prefix| options.buffer_size.checked_add(prefix))
            .filter(|size| {
                *size <= i32::MAX as usize
                    && size
                        .checked_add(options.alignment - 1)
                        .and_then(|stride| stride.checked_mul(options.buffers))
                        .is_some_and(|allocation| allocation <= isize::MAX as usize)
            })
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "receive metadata and payload must fit the kernel ABI",
                )
            })?;
        let identifier = Arc::new(Group::new(&controllers[0])?);
        // Allocate payload slabs and provided-ring storage before publishing anything to workers. Neither needs first-use allocation or a pool-to-worker directory.
        let workers = controllers
            .iter()
            .map(|controller| {
                let heap = HeapBufferPool::with_options(BufferPoolOptions {
                    buffer_size: size,
                    ..options
                })
                .expect("validated worker storage fits the heap pool's size and alignment limits");
                WorkerBuffers::new(heap, options, Arc::clone(&identifier), controller)
            })
            .collect::<io::Result<Box<[_]>>>()?;
        let pool = Self(Arc::new(Pool {
            options,
            selector: Arc::downgrade(group),
            storage: Storage::Workers(workers),
            next: AtomicUsize::new(0),
            capacity: capacity.unwrap(),
        }));
        let Storage::Workers(workers) = &pool.0.storage else {
            unreachable!("the worker-pool constructor creates worker-owned storage");
        };
        for (controller, worker) in controllers.iter().zip(workers) {
            controller
                .send(Command::PoolReady(Arc::clone(worker)))
                .map_err(|_| {
                    io::Error::new(
                        io::ErrorKind::BrokenPipe,
                        "worker closed during pool initialization",
                    )
                })?;
        }
        Ok(pool)
    }

    pub(crate) fn is_provided(&self) -> bool {
        matches!(self.0.storage, Storage::Workers(_))
    }

    pub(crate) fn worker(&self, controller: &Controller) -> Option<&Arc<WorkerBuffers>> {
        match &self.0.storage {
            Storage::Shared(_) => None,
            Storage::Workers(workers) => Some(&workers[controller.worker]),
        }
    }

    fn heap(&self) -> &HeapBufferPool {
        match &self.0.storage {
            Storage::Shared(heap) => heap,
            Storage::Workers(workers) => {
                let index = self
                    .0
                    .selector
                    .upgrade()
                    .and_then(|group| crate::io_uring_worker::route_index(&group))
                    .unwrap_or_else(|| self.0.next.fetch_add(1, Ordering::Relaxed) % workers.len());
                &workers[index].heap
            }
        }
    }
}

impl Drop for Pool {
    fn drop(&mut self) {
        if let Storage::Workers(workers) = &self.storage {
            for worker in workers {
                worker.release();
            }
        }
    }
}

impl BufferPool for IoUringBufferPool {
    type Buffer = crate::IoUringBuffer;
    fn capacity(&self) -> usize {
        self.0.capacity
    }
    fn buffer_size(&self) -> usize {
        self.0.options.buffer_size
    }
    fn alignment(&self) -> usize {
        self.0.options.alignment
    }
    fn try_acquire(&self) -> Option<Self::Buffer> {
        self.heap()
            .try_acquire()
            .map(|buffer| crate::IoUringBuffer::new(buffer, 0, self.buffer_size()))
    }
    async fn acquire(&self, cancellation: &Cancellation) -> Result<Self::Buffer, Cancelled> {
        self.heap()
            .acquire(cancellation)
            .await
            .map(|buffer| crate::IoUringBuffer::new(buffer, 0, self.buffer_size()))
    }
}
