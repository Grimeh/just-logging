#![feature(result_option_map_or_default)]

use crossbeam_queue::SegQueue;
use log::{error, Level, LevelFilter, Log, Metadata, Record};
use std::fs::File;
use std::io::Write;
use std::path::Path;
use std::sync::atomic::{AtomicU32, AtomicU8, AtomicUsize, Ordering};
use std::sync::{LazyLock, Mutex, RwLock};
use std::{fs, io, panic, thread};
use std::backtrace::{Backtrace, BacktraceStatus};

#[macro_export]
macro_rules! clog {
	($verb:ident, $condition:expr, $($next:expr),+) => {
		if $condition {
			::log::$verb!($($next),+);
		}
	};
}

/// Conditional trace log macro
///
/// Logs at trace level if the supplied condition is true
#[macro_export]
macro_rules! ctrace {
	($condition:expr, $next:tt) => {
		$crate::clog!(trace, $condition, $next);
	};
}

/// Conditional debug log macro
///
/// Logs at debug level if the supplied condition is true
#[macro_export]
macro_rules! cdebug {
	($condition:expr, $next:tt) => {
		$crate::clog!(debug, $condition, $next);
	};
}

/// Conditional info log macro
///
/// Logs at info level if the supplied condition is true
#[macro_export]
macro_rules! cinfo {
	($condition:expr, $next:tt) => {
		$crate::clog!(debug, $condition, $next);
	};
}

/// Conditional warning macro
///
/// Logs a warning if the supplied condition is true
#[macro_export]
macro_rules! cwarn {
	($condition:expr, $next:tt) => {
		$crate::clog!(warn, $condition, $next);
	};
}

/// Conditional error macro
///
/// Logs an error if the supplied condition is true
#[macro_export]
macro_rules! cerror {
	($condition:expr, $($next:expr),+) => {
		$crate::clog!(error, $condition, $($next),+);
	};
}

const MODULE_BLACKLIST: &'static [(&'static str, LevelFilter)] = &[
	("ignore::", LevelFilter::Warn),
	("globset", LevelFilter::Warn),
	("notify::", LevelFilter::Warn),
];

const LOG_PREV_SUFFIX: &str = "prev";

pub static JUSTLOG: LazyLock<JustLog> = LazyLock::new(|| JustLog {
	enabled: AtomicU8::new(0),
	max_level: AtomicUsize::new(LevelFilter::Trace as usize),
	msg_count: AtomicU32::new(0),
	queue: SegQueue::new(),
	file: Mutex::new(None),
	module_levels: RwLock::new(Vec::new()),
	timestamps_enabled: true,
	additional_sinks: Mutex::new(Vec::new()),
});

pub struct LogEntry {
	pub module: String,
	pub level: Level,
	pub timestamp: Option<String>,

	pub filename: String,
	pub line: u32,

	pub msg: String,

	pub backtrace: Option<Backtrace>,
}

#[repr(u8)]
enum EnabledState {
	Disabled = 0,
	Initialising = 1,
	Enabled = 2,
	ShuttingDown = 3,
}

struct ModFilter {
	module: String,
	level: LevelFilter,
}

#[derive(Copy, Clone, Debug)]
pub enum LogError {
	CachePoisoned,
}

pub type FnSink = dyn FnMut(&LogEntry) + Send + Sync;

pub struct JustLog {
	enabled: AtomicU8,
	max_level: AtomicUsize,

	msg_count: AtomicU32,
	queue: SegQueue<LogEntry>,
	file: Mutex<Option<File>>,

	module_levels: RwLock<Vec<ModFilter>>,
	timestamps_enabled: bool,

	additional_sinks: Mutex<Vec<Box<FnSink>>>,
}

impl JustLog {
	pub fn spawn_log_thread(log_path: Option<&Path>, default_level: LevelFilter) -> io::Result<thread::JoinHandle<()>> {
		JUSTLOG.init(log_path);
		assert_eq!(JUSTLOG.enabled.load(Ordering::Relaxed), EnabledState::Enabled as u8);

		JUSTLOG.max_level.store(default_level as usize, Ordering::Relaxed);
		// log::set_max_level(default_level);
		log::set_logger(&*JUSTLOG).unwrap();

		// redirect panics to logger
		log_panics::init();

		thread::Builder::new()
			.name("ah_logger".to_string())
			.spawn(|| {
				match panic::catch_unwind(|| {
					let logger = &*JUSTLOG;
					loop {
						if logger.enabled.load(Ordering::Relaxed) == EnabledState::ShuttingDown as u8 {
							break;
						}

						atomic_wait::wait(&logger.msg_count, 0);
						logger.flush();
					}
				}) {
					Ok(()) => {},
					Err(err) => {
						error!("log thread panicked! {:?}", err);
					}
				}
			})
	}

	pub fn set_module_log_level(module: &str, level: LevelFilter) {
		let this = &*JUSTLOG;
		assert_eq!(this.enabled.load(Ordering::Relaxed), EnabledState::Enabled as u8);

		let mut levels = this.module_levels.write().unwrap();
		for filter in levels.iter_mut() {
			if module.starts_with(&filter.module) {
				filter.level = level;
				return;
			}
		}

		// didn't find, add
		levels.push(ModFilter {
			module: module.to_string(),
			level,
		});
	}

	pub fn add_sink(sink: Box<FnSink>) {
		let this = &*JUSTLOG;
		let mut sinks = this.additional_sinks.lock().unwrap();
		sinks.push(sink);
	}

	pub fn shutdown() {
		let this = &*JUSTLOG;
		this.enabled.store(EnabledState::ShuttingDown as u8, Ordering::Relaxed);
		atomic_wait::wake_all(&this.msg_count);
	}

	fn init(&self, log_path: Option<&Path>) {
		self.enabled.compare_exchange(
			EnabledState::Disabled as u8,
			EnabledState::Initialising as u8,
			Ordering::Acquire,
			Ordering::Relaxed
		).expect("invalid JustLog init state");

		{
			let mut bl = self.module_levels.write().unwrap();
			for (name, level) in MODULE_BLACKLIST {
				bl.push(ModFilter {
					module: name.to_string(),
					level: *level,
				});
			}
		}

		if let Some(path) = log_path {
			let mut file = self.file.lock().unwrap();
			*file = open_log(path);
		}

		self.enabled.compare_exchange(
			EnabledState::Initialising as u8,
			EnabledState::Enabled as u8,
			Ordering::Relaxed,
			Ordering::Relaxed
		).expect("JustLog init race detected");
	}
}

impl Log for JustLog {
	fn enabled(&self, metadata: &Metadata) -> bool {
		metadata.level() <= Level::Trace
	}

	fn log(&self, record: &Record) {
		if record.target() == "panic" {
			// immediately print to stderr for convenience
			eprintln!("{}", record.args());
		}

		let module = record.module_path().unwrap_or("NONE");

		// check against module filter rules
		let mut passed = false;
		let levels = self.module_levels.read().unwrap();
		for filter in levels.iter() {
			if module.starts_with(&filter.module) {
				if record.level() > filter.level {
					// failing any rules discards the record
					return;
				}
				passed = true;
			}
		}

		// fallback to global max level if no module filter rules were passed
		if !passed {
			if record.level() as usize > self.max_level.load(Ordering::Relaxed) {
				return;
			}
		}

		let timestamp = if self.timestamps_enabled {
			let now = chrono::Local::now();
			let now = now.format("%y%m%d-%H:%M:%S.%3f").to_string();
			Some(now)
		} else {
			None
		};

		let filename = record.file().map_or_default(|f| f.to_string());
		let line = record.line().unwrap_or_default();

		let backtrace = if cfg!(feature = "backtrace") {
			if record.level() == Level::Error {
				let bt = Backtrace::force_capture();
				if bt.status() == BacktraceStatus::Captured {
					Some(bt)
				} else {
					None
				}
			} else {
				None
			}
		} else {
			None
		};

		self.queue.push(LogEntry {
			module: module.to_string(),
			level: record.level(),
			timestamp,
			filename,
			line,
			msg: record.args().to_string(),
			backtrace,
		});

		self.msg_count.fetch_add(1, Ordering::Relaxed);
		atomic_wait::wake_one(&self.msg_count);
	}

	fn flush(&self) {
		while self.msg_count.load(Ordering::Relaxed) > 0 {
			let mut f = self.file.lock().unwrap();
			let mut additional = self.additional_sinks.lock().unwrap();

			while let Some(entry) = self.queue.pop() {
				self.msg_count.fetch_sub(1, Ordering::Relaxed);

				for s in &mut *additional {
					s(&entry);
				}

				let msg = match entry.timestamp {
					Some(ts) => {
						match entry.backtrace {
							Some(bt) => {
								format!("{} [{}] {} - {}\n{}", ts, entry.module, entry.level, entry.msg, bt)
							}
							None => format!("{} [{}] {} - {}", ts, entry.module, entry.level, entry.msg),
						}
					}
					None => {
						match entry.backtrace {
							Some(bt) => format!("[{}] {} - {}\n{}", entry.module, entry.level, entry.msg, bt),
							None => format!("[{}] {} - {}", entry.module, entry.level, entry.msg),
						}

					}
				};

				if entry.level > Level::Error {
					println!("{}", msg);
				} else {
					eprintln!("{}", msg);
				}

				match &mut *f {
					Some(f) => {
						write!(f, "{}\n", msg).unwrap();
					}
					None => {}
				}
			}
		}
	}
}

fn open_log(path: &Path) -> Option<File> {
	let path = {
		if path.is_absolute() {
			path.to_owned()
		} else {
			let cwd = std::env::current_dir().ok()?;
			cwd.join(path)
		}
	};

	if let Some(parent) = path.parent() {
		if !parent.exists() {
			fs::create_dir_all(parent).ok()?;
		} else {
			// roll log if it exists
			roll_log(&path);
		}
	}

	// `create` will truncate if the file exists
	File::create(path).ok()
}

fn roll_log(path: &Path) -> Option<()> {
	if fs::exists(path).ok()? {
		let prev_path = path.with_added_extension(LOG_PREV_SUFFIX);
		if fs::exists(&prev_path).ok()? {
			fs::remove_file(&prev_path).ok()?;
		}
		fs::rename(path, prev_path).ok()?;
	}

	Some(())
}
