// bblite - daemonless, user-space checkpoint staging via LD_PRELOAD (v0).
//
// Absorb declared checkpoint files on a node-local staging tier (RAM tmpfs
// by default), drain them asynchronously to the shared store with
// commit-by-rename, pace with a staging watermark, and guarantee that
// everything is durable on the store before the process exits (the
// drain-at-exit commit barrier - the semantic anchor of the whole design).
//
// Semantics contract (v0):
//   * Files under BBLITE_STORE whose basename matches BBLITE_PATTERN are
//     transparently redirected to BBLITE_STAGING_DIR; everything else is a
//     pure passthrough. Default (BBLITE unset) = 100% passthrough.
//   * close() returns after the local spill; the drain runs on worker lanes.
//   * fsync()/fdatasync() are a real durability barrier for that file: they
//     block until the bytes are on the store (fdatasync'd, renamed into
//     place, directory fsync'd).
//   * reads of a staged file (open O_RDONLY / stat) are served from staging
//     and see the latest bytes even before the drain.
//   * unlink()/rename() of staged files are tracked (tombstone / redirect).
//   * Process exit drains everything outstanding (destructor commit barrier).
//   * NOT staged (passthrough + warning once): O_DIRECT, O_TMPFILE,
//     dirfd-relative opens, rename onto a path outside the store.
//   * After fork() the child disables staging (drain workers do not exist).
//
// Threading model (v0): one global mutex over the fd/file tables; per-fd
// serialized writers (the MPI-rank case). Multi-threaded writers of the same
// fd are not the target and only need to not crash.
//
// LD_PRELOAD INIT TRAP (learned the hard way by v0-alpha): a
// __attribute__((constructor)) runs BEFORE this translation unit's ordinary
// dynamic initializers - any file-scope std::mutex/map/thread/Config object
// is still unconstructed when the constructor executes, and its later
// dynamic init CLOBBERS everything. Hence: one POD global pointer (G), all
// non-trivial state allocated inside a first-use function-local static
// (magic static = runtime init, thread-safe, immune to init-order).
//
// SPDX-License-Identifier: MIT
#define _GNU_SOURCE
#include <dlfcn.h>
#include <fcntl.h>
#include <fnmatch.h>
#include <limits.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <unordered_map>
#include <vector>

namespace {

// ------------------------------------------------------------------ libc ---
struct RealApi {
    int (*open)(const char*, int, mode_t);
    int (*open64)(const char*, int, mode_t);
    int (*openat)(int, const char*, int, mode_t);
    int (*openat64)(int, const char*, int, mode_t);
    int (*creat)(const char*, mode_t);
    ssize_t (*write)(int, const void*, size_t);
    ssize_t (*pwrite)(int, const void*, size_t, off_t);
    ssize_t (*read)(int, void*, size_t);
    ssize_t (*pread)(int, void*, size_t, off_t);
    off_t (*lseek)(int, off_t, int);
    int (*close)(int);
    int (*dup)(int);
    int (*fsync)(int);
    int (*fdatasync)(int);
    int (*unlink)(const char*);
    int (*rename)(const char*, const char*);
    int (*mkdir)(const char*, mode_t);
    int (*stat)(const char*, struct stat*);
    int (*lstat)(const char*, struct stat*);
    int (*fstat)(int, struct stat*);
    FILE* (*fopen)(const char*, const char*);
    FILE* (*fopen64)(const char*, const char*);
    int (*fclose)(FILE*);
};
RealApi* R = nullptr; // POD pointer; calloc'd at first use (no dynamic init)

// ------------------------------------------------------------------ state ---
struct Config {
    bool enabled = false;
    std::string store;                    // absolute prefix to stage under
    std::string stageDir;                 // node-local staging root
    std::vector<std::string> patterns;    // basename glob(s), space-separated
    int watermarkPct = 80;                // backpressure threshold on stage fs
    int lanes = 4;                        // drain worker threads
    bool verbose = false;
};

struct Entry {
    std::string finalPath;                // absolute path on the store
    std::string stagePath;                // current path on the staging tier
    int stageFd = -1;                     // private fd, survives app renames
    int refs = 0;                         // app-visible fds
    uint64_t copied = 0;                  // bytes durably renamed to store
    bool queued = false;
    bool tombstone = false;               // unlinked before drain completed
};
using EntryPtr = std::shared_ptr<Entry>;

struct State {
    Config cfg;
    std::mutex mtx;                       // guards everything below
    std::condition_variable cv;
    std::deque<EntryPtr> queue;
    std::unordered_map<int, EntryPtr> fdMap;   // app fd -> entry
    std::unordered_map<std::string, EntryPtr> byFinal;
    std::unordered_map<FILE*, int> ourFile;    // fdopen'd FILE* -> app fd
    std::atomic<bool> stop{false};
    std::atomic<int> live{0};             // workers currently inside a drain
    std::atomic<uint64_t> seq{0};
    std::vector<std::thread> workers;
    std::atomic<bool> wmWarned{false};
    std::atomic<bool> directWarned{false};
    std::atomic<bool> escapeWarned{false};
};
State* G = nullptr; // POD: zero-initialized, no dynamic init required

// -------------------------------------------------------------- utilities ---
void logMsg(const char* fmt, ...)
{
    if (G == nullptr || !G->cfg.verbose)
        return;
    fprintf(stderr, "bblite[%d]: ", (int)getpid());
    va_list ap;
    va_start(ap, fmt);
    vfprintf(stderr, fmt, ap);
    va_end(ap);
}

std::string normalize(const std::string& p)
{
    std::string out;
    bool prevSlash = false;
    for (char ch : p)
    {
        if (ch == '/')
        {
            if (prevSlash)
                continue;
            prevSlash = true;
        }
        else
            prevSlash = false;
        out += ch;
    }
    if (out.size() > 1 && out.back() == '/')
        out.pop_back();
    return out;
}

// v0: absolute or cwd-relative paths only; true dirfd-relative -> passthrough.
std::string absPath(int dirfd, const char* path)
{
    if (path == nullptr || path[0] == 0)
        return "";
    if (path[0] == '/')
        return normalize(path);
    if (dirfd != AT_FDCWD)
        return "";
    char cwd[PATH_MAX];
    if (getcwd(cwd, sizeof cwd) == nullptr)
        return "";
    return normalize(std::string(cwd) + "/" + path);
}

bool underStore(const std::string& abs)
{
    return !G->cfg.store.empty() && abs.size() > G->cfg.store.size() + 1 &&
           abs.compare(0, G->cfg.store.size(), G->cfg.store) == 0 &&
           abs[G->cfg.store.size()] == '/';
}

bool basenameMatches(const std::string& abs)
{
    const std::string base = abs.substr(abs.rfind('/') + 1);
    for (const auto& p : G->cfg.patterns)
        if (fnmatch(p.c_str(), base.c_str(), 0) == 0)
            return true;
    return false;
}

int mkdirP(const std::string& dir)
{
    size_t pos = 1;
    for (;;)
    {
        size_t slash = dir.find('/', pos);
        std::string sub = (slash == std::string::npos) ? dir : dir.substr(0, slash);
        if (R->mkdir(sub.c_str(), 0777) != 0 && errno != EEXIST)
            return -1;
        if (slash == std::string::npos)
            return 0;
        pos = slash + 1;
    }
}

void fsyncDirOf(const std::string& path)
{
    size_t slash = path.rfind('/');
    if (slash == std::string::npos || slash == 0)
        return;
    int fd = R->open(path.substr(0, slash).c_str(), O_RDONLY, 0);
    if (fd >= 0)
    {
        R->fsync(fd);
        R->close(fd);
    }
}

// Blocks the *app writer* while staging sits at/above the watermark and
// drains are pending: the backpressure edge of the envelope. Bounded
// (worst case ~30 s, then proceed): bblite must never deadlock an
// application no matter how sick the store is. Never touches G->mtx while
// sleeping.
void watermarkWait()
{
    if (G->cfg.watermarkPct >= 100)
        return;
    for (int spin = 0; spin < 300 && !G->stop.load(); ++spin)
    {
        struct statvfs vfs;
        if (statvfs(G->cfg.stageDir.c_str(), &vfs) != 0)
            return;
        const unsigned long long total = (unsigned long long)vfs.f_blocks * vfs.f_frsize;
        const unsigned long long avail = (unsigned long long)vfs.f_bavail * vfs.f_frsize;
        const unsigned long long pct = total ? 100ull * (total - avail) / total : 0;
        if ((int)pct < G->cfg.watermarkPct)
            return;
        bool busy;
        {
            std::lock_guard<std::mutex> lk(G->mtx);
            busy = !G->queue.empty() || G->live.load() > 0;
        }
        if (!busy)
        {
            if (!G->wmWarned.exchange(true))
                fprintf(stderr,
                        "bblite[%d]: WARNING staging %s at %llu%% >= watermark %d%%, nothing "
                        "drainable pending; allowing growth (open files must close)\n",
                        (int)getpid(), G->cfg.stageDir.c_str(), pct, G->cfg.watermarkPct);
            return;
        }
        logMsg("watermark backpressure: staging at %llu%%, waiting for drain\n", pct);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

// ------------------------------------------------------------------- drain ---
// Caller holds G->mtx. Copy staged bytes [from, size) to the store under a
// temp name, fdatasync, rename into place, fsync dir: the metadata-friendly
// atomic publish. Updates e.copied on success.
bool copyRange(Entry& e, uint64_t from, uint64_t size)
{
    char tmp[PATH_MAX];
    snprintf(tmp, sizeof tmp, "%s.bblite-tmp.%d.%llu", e.finalPath.c_str(), (int)getpid(),
             (unsigned long long)G->seq.fetch_add(1));
    const size_t slash = e.finalPath.rfind('/');
    if (slash != std::string::npos && mkdirP(e.finalPath.substr(0, slash)) != 0)
    {
        fprintf(stderr, "bblite[%d]: cannot create store dir for %s (%s)\n", (int)getpid(),
                e.finalPath.c_str(), strerror(errno));
        return false;
    }
    int out = R->open(tmp, O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (out < 0)
    {
        fprintf(stderr, "bblite[%d]: cannot open %s (%s)\n", (int)getpid(), tmp, strerror(errno));
        return false;
    }
    static const size_t CHUNK = 8u << 20;
    std::vector<char> buf(CHUNK);
    uint64_t off = from;
    bool ok = true;
    while (off < size)
    {
        const size_t want = (size_t)std::min<uint64_t>(CHUNK, size - off);
        ssize_t got = R->pread(e.stageFd, buf.data(), want, (off_t)off);
        if (got <= 0)
        {
            ok = false;
            break;
        }
        ssize_t put = 0;
        while (put < got)
        {
            ssize_t w = R->pwrite(out, buf.data() + put, (size_t)(got - put), (off_t)(off + put));
            if (w <= 0)
                break;
            put += w;
        }
        if (put < got)
        {
            ok = false;
            break;
        }
        off += (uint64_t)got;
    }
    if (ok && R->fdatasync(out) != 0)
        ok = false;
    R->close(out);
    if (ok && R->rename(tmp, e.finalPath.c_str()) != 0)
        ok = false;
    if (ok)
        fsyncDirOf(e.finalPath);
    if (!ok)
    {
        R->unlink(tmp);
        fprintf(stderr, "bblite[%d]: drain of %s failed (%s)\n", (int)getpid(),
                e.finalPath.c_str(), strerror(errno));
        return false;
    }
    e.copied = size;
    return true;
}

bool drainEntry(Entry& e)
{
    for (;;)
    {
        struct stat st;
        if (e.stageFd < 0 || R->fstat(e.stageFd, &st) != 0)
            return false;
        if (e.copied >= (uint64_t)st.st_size)
            return true;
        if (!copyRange(e, e.copied, (uint64_t)st.st_size))
            return false;
    }
}

void enqueueLocked(const EntryPtr& e)
{
    if (e->queued || e->tombstone || e->stageFd < 0)
        return;
    G->queue.push_back(e);
    e->queued = true;
    G->cv.notify_one();
}

void workerLoop()
{
    for (;;)
    {
        EntryPtr e;
        {
            std::unique_lock<std::mutex> lk(G->mtx);
            G->cv.wait(lk, [] { return G->stop.load() || !G->queue.empty(); });
            if (G->queue.empty())
                return; // stop requested and queue drained
            e = G->queue.front();
            G->queue.pop_front();
            e->queued = false;
            G->live.fetch_add(1);
        }
        {
            std::lock_guard<std::mutex> lk(G->mtx);
            if (!e->tombstone)
                drainEntry(*e);
        }
        {
            std::lock_guard<std::mutex> lk(G->mtx);
            G->live.fetch_sub(1);
            struct stat st;
            const uint64_t sz =
                (e->stageFd >= 0 && R->fstat(e->stageFd, &st) == 0) ? (uint64_t)st.st_size : 0;
            if (e->refs == 0 && !e->tombstone && e->stageFd >= 0 && e->copied >= sz)
            {
                R->close(e->stageFd);
                e->stageFd = -1;
                R->unlink(e->stagePath.c_str());
                auto it = G->byFinal.find(e->finalPath);
                if (it != G->byFinal.end() && it->second == e)
                    G->byFinal.erase(it);
                logMsg("drained + freed %s (%llu bytes)\n", e->finalPath.c_str(),
                       (unsigned long long)sz);
            }
            G->cv.notify_all();
        }
    }
}

// ---------------------------------------------------------------- open path ---
EntryPtr findEntry(int fd)
{
    std::lock_guard<std::mutex> lk(G->mtx);
    auto it = G->fdMap.find(fd);
    return it == G->fdMap.end() ? nullptr : it->second;
}

// Returns fd >= 0 when staged (a dup of the entry's private staging fd),
// -1 when the caller must pass through to the real libc open.
int openCommon(int dirfd, const char* path, int flags, mode_t mode)
{
    if (!G->cfg.enabled || path == nullptr)
        return -1;
    const std::string abs = absPath(dirfd, path);
    if (abs.empty() || !underStore(abs) || !basenameMatches(abs))
        return -1;
    if (flags & (O_DIRECT | O_TMPFILE))
    {
        if (!G->directWarned.exchange(true))
            fprintf(stderr, "bblite[%d]: WARNING %s uses O_DIRECT/O_TMPFILE -> passthrough\n",
                    (int)getpid(), abs.c_str());
        return -1;
    }

    // read-only open of a not-yet-drained staged file: serve from staging.
    if ((flags & O_ACCMODE) == O_RDONLY)
    {
        std::lock_guard<std::mutex> lk(G->mtx);
        auto it = G->byFinal.find(abs);
        if (it == G->byFinal.end() || it->second->stageFd < 0)
            return -1; // nothing staged: the real open (or ENOENT) is correct
        int fd = R->open(it->second->stagePath.c_str(), O_RDONLY, 0);
        if (fd < 0)
            return -1;
        G->fdMap[fd] = it->second;
        it->second->refs++;
        return fd;
    }

    if (flags & O_CREAT)
        watermarkWait(); // admission control for a new checkpoint

    std::lock_guard<std::mutex> lk(G->mtx);
    auto it = G->byFinal.find(abs);
    if (it != G->byFinal.end())
    {
        EntryPtr e = it->second;
        if ((flags & O_TRUNC) && e->refs == 0)
        {
            // checkpoint slot reused: finish the old version first, then reset
            drainEntry(*e);
            if (e->stageFd >= 0)
            {
                R->close(e->stageFd);
                e->stageFd = -1;
            }
            R->unlink(e->stagePath.c_str());
            G->byFinal.erase(it);
        }
        else if (e->stageFd >= 0 && !e->tombstone)
        {
            int fd = R->dup(e->stageFd);
            if (fd >= 0)
            {
                G->fdMap[fd] = e;
                e->refs++;
                return fd;
            }
            return -1;
        }
        else
            return -1;
    }

    // fresh staging: mirror the store-relative layout under stageDir
    const std::string stagePath = G->cfg.stageDir + abs.substr(G->cfg.store.size());
    const size_t slash = stagePath.rfind('/');
    if (slash != std::string::npos && mkdirP(stagePath.substr(0, slash)) != 0)
    {
        fprintf(stderr, "bblite[%d]: staging dir mkdir failed for %s (%s) -> passthrough\n",
                (int)getpid(), abs.c_str(), strerror(errno));
        return -1;
    }
    // The private staging fd is OUR handle: always O_RDWR so the drain can
    // pread from it even when the app opened write-only.
    int sflags = O_RDWR | O_CREAT | ((flags & O_TRUNC) ? O_TRUNC : 0) |
                 ((flags & O_APPEND) ? O_APPEND : 0);
    int sfd = R->open(stagePath.c_str(), sflags, mode ? mode : 0644);
    if (sfd < 0)
        return -1;
    int fd = R->dup(sfd);
    if (fd < 0)
    {
        R->close(sfd);
        return -1;
    }
    EntryPtr e = std::make_shared<Entry>();
    e->finalPath = abs;
    e->stagePath = stagePath;
    e->stageFd = sfd;
    e->refs = 1;
    G->fdMap[fd] = e;
    G->byFinal[abs] = e;
    logMsg("staged %s -> %s\n", abs.c_str(), stagePath.c_str());
    return fd;
}

// --------------------------------------------------------------- close path ---
// Returns 1 if handled (fd was staged; its real close already happened when
// skipRealClose==false), 0 for passthrough.
int closeCommon(int fd, bool skipRealClose = false)
{
    if (!G->cfg.enabled)
        return 0;
    std::lock_guard<std::mutex> lk(G->mtx);
    auto it = G->fdMap.find(fd);
    if (it == G->fdMap.end())
        return 0;
    EntryPtr e = it->second;
    G->fdMap.erase(it);
    e->refs--;
    if (e->refs <= 0)
    {
        if (e->tombstone)
        {
            if (e->stageFd >= 0)
            {
                R->close(e->stageFd);
                e->stageFd = -1;
            }
            auto b = G->byFinal.find(e->finalPath);
            if (b != G->byFinal.end() && b->second == e)
                G->byFinal.erase(b);
        }
        else
            enqueueLocked(e); // the async point: spill done, drain scheduled
    }
    if (!skipRealClose)
        R->close(fd);
    return 1;
}

bool syncEntry(int fd, int* status)
{
    EntryPtr e = findEntry(fd);
    if (!e)
        return false;
    std::lock_guard<std::mutex> lk(G->mtx);
    const bool ok = drainEntry(*e);
    *status = ok ? 0 : -1;
    if (!ok)
        errno = EIO;
    return true;
}

// ------------------------------------------------------------------ stdio ---
FILE* fopenCommon(const char* path, const char* mode)
{
    if (!G->cfg.enabled || path == nullptr || mode == nullptr)
        return nullptr;
    const char c0 = mode[0];
    if (c0 != 'w' && c0 != 'a')
        return nullptr; // 'r' variants: no entry -> passthrough (see DESIGN)
    int flags = (strchr(mode, '+') ? O_RDWR : O_WRONLY) | O_CREAT |
                (c0 == 'w' ? O_TRUNC : O_APPEND);
    int fd = openCommon(AT_FDCWD, path, flags, 0666);
    if (fd < 0)
        return nullptr;
    FILE* f = fdopen(fd, mode);
    if (f == nullptr)
    {
        closeCommon(fd);
        return nullptr;
    }
    std::lock_guard<std::mutex> lk(G->mtx);
    G->ourFile[f] = fd;
    return f;
}

int fcloseCommon(FILE* f, int* rc)
{
    if (!G->cfg.enabled || f == nullptr)
        return 0;
    int fd = -1;
    {
        std::lock_guard<std::mutex> lk(G->mtx);
        auto it = G->ourFile.find(f);
        if (it == G->ourFile.end())
            return 0;
        fd = it->second;
        G->ourFile.erase(it);
    }
    *rc = R->fclose(f); // flushes the stdio buffer onto the staged fd, closes the fd
    closeCommon(fd, true);
    return 1;
}

// -------------------------------------------------------------------- init ---
void childAfterFork()
{
    // Drain threads do not exist in the child: disable staging so the child
    // is always correct (pure passthrough) rather than silently undrained.
    if (G != nullptr)
    {
        G->stop.store(true);
        G->cfg.enabled = false;
    }
}

void initOnce()
{
    static State* s = [] {
        R = (RealApi*)calloc(1, sizeof(RealApi));
#define SYM(name) *(void**)&R->name = dlsym(RTLD_NEXT, #name)
        SYM(open);
        SYM(open64);
        SYM(openat);
        SYM(openat64);
        SYM(creat);
        SYM(write);
        SYM(pwrite);
        SYM(read);
        SYM(pread);
        SYM(lseek);
        SYM(close);
        SYM(dup);
        SYM(fsync);
        SYM(fdatasync);
        SYM(unlink);
        SYM(rename);
        SYM(mkdir);
        SYM(stat);
        SYM(lstat);
        SYM(fstat);
        SYM(fopen);
        SYM(fopen64);
        SYM(fclose);
#undef SYM

        State* p = new State();
        // Publish G BEFORE any worker can observe it: threads started below
        // dereference G immediately (the static's return-assignment is too late).
        G = p;
        if (!R->open || !R->write || !R->close || !R->dup)
        {
            fprintf(stderr, "bblite: FATAL: cannot resolve libc entry points -> disabled\n");
            return p;
        }
        Config& cfg = p->cfg;
        if (const char* e = getenv("BBLITE"))
            cfg.enabled = atoi(e) != 0;
        if (const char* v = getenv("BBLITE_STORE"))
            cfg.store = normalize(v);
        if (const char* v = getenv("BBLITE_STAGING_DIR"))
            cfg.stageDir = normalize(v);
        if (const char* v = getenv("BBLITE_PATTERN"))
        {
            std::string pat = v;
            size_t pos = 0;
            while (pos < pat.size())
            {
                size_t sp = pat.find(' ', pos);
                if (sp == std::string::npos)
                    sp = pat.size();
                if (sp > pos)
                    cfg.patterns.push_back(pat.substr(pos, sp - pos));
                pos = sp + 1;
            }
        }
        if (const char* v = getenv("BBLITE_WATERMARK_PCT"))
            cfg.watermarkPct = atoi(v);
        if (const char* v = getenv("BBLITE_DRAIN_LANES"))
            cfg.lanes = atoi(v);
        if (const char* v = getenv("BBLITE_VERBOSE"))
            cfg.verbose = atoi(v) != 0;
        if (cfg.lanes < 1)
            cfg.lanes = 1;
        if (cfg.enabled && (cfg.store.empty() || cfg.stageDir.empty() || cfg.patterns.empty()))
        {
            fprintf(stderr, "bblite: BBLITE=1 but STORE/PATTERN/STAGING_DIR incomplete -> disabled\n");
            cfg.enabled = false;
        }
        pthread_atfork(nullptr, nullptr, childAfterFork);
        if (cfg.enabled)
        {
            for (int i = 0; i < cfg.lanes; ++i)
                p->workers.emplace_back(workerLoop);
            if (cfg.verbose)
                fprintf(stderr, "bblite[%d]: ON store=%s stage=%s patterns=%zu lanes=%d wm=%d%%\n",
                        (int)getpid(), cfg.store.c_str(), cfg.stageDir.c_str(),
                        cfg.patterns.size(), cfg.lanes, cfg.watermarkPct);
        }
        return p;
    }();
    (void)s;
}

__attribute__((constructor)) void bbliteCtor()
{
    initOnce();
}

// The commit barrier: job-step boundary == process boundary here (v0).
// Everything still staged is drained before the process can disappear -
// this is what makes restart correctness hold without any daemon.
__attribute__((destructor)) void bbliteDtor()
{
    if (G == nullptr || !G->cfg.enabled)
        return;
    {
        std::lock_guard<std::mutex> lk(G->mtx);
        for (auto& kv : G->byFinal)
            enqueueLocked(kv.second);
    }
    for (int spin = 0; spin < 36000; ++spin) // up to 1 h: never exit undrained
    {
        bool idle;
        {
            std::lock_guard<std::mutex> lk(G->mtx);
            idle = G->queue.empty() && G->live.load() == 0;
        }
        if (idle)
            break;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    G->stop.store(true);
    G->cv.notify_all();
    for (auto& t : G->workers)
        if (t.joinable())
            t.join();
}

} // namespace

// =============================================================== interposed ==
extern "C" {

int open(const char* path, int flags, ...)
{
    initOnce();
    mode_t mode = 0;
    if (flags & O_CREAT)
    {
        va_list ap;
        va_start(ap, flags);
        mode = (mode_t)va_arg(ap, int);
        va_end(ap);
    }
    const int fd = openCommon(AT_FDCWD, path, flags, mode);
    return fd >= 0 ? fd : R->open(path, flags, mode);
}

int open64(const char* path, int flags, ...)
{
    initOnce();
    mode_t mode = 0;
    if (flags & O_CREAT)
    {
        va_list ap;
        va_start(ap, flags);
        mode = (mode_t)va_arg(ap, int);
        va_end(ap);
    }
    const int fd = openCommon(AT_FDCWD, path, flags, mode);
    return fd >= 0 ? fd : R->open64(path, flags, mode);
}

int openat(int dirfd, const char* path, int flags, ...)
{
    initOnce();
    mode_t mode = 0;
    if (flags & O_CREAT)
    {
        va_list ap;
        va_start(ap, flags);
        mode = (mode_t)va_arg(ap, int);
        va_end(ap);
    }
    const int fd = openCommon(dirfd, path, flags, mode);
    return fd >= 0 ? fd : R->openat(dirfd, path, flags, mode);
}

int openat64(int dirfd, const char* path, int flags, ...)
{
    initOnce();
    mode_t mode = 0;
    if (flags & O_CREAT)
    {
        va_list ap;
        va_start(ap, flags);
        mode = (mode_t)va_arg(ap, int);
        va_end(ap);
    }
    const int fd = openCommon(dirfd, path, flags, mode);
    return fd >= 0 ? fd : R->openat64(dirfd, path, flags, mode);
}

int creat(const char* path, mode_t mode)
{
    initOnce();
    const int fd = openCommon(AT_FDCWD, path, O_CREAT | O_WRONLY | O_TRUNC, mode);
    return fd >= 0 ? fd : R->creat(path, mode);
}

ssize_t write(int fd, const void* buf, size_t n)
{
    initOnce();
    EntryPtr e = findEntry(fd);
    return R->write(e ? e->stageFd : fd, buf, n);
}

ssize_t pwrite(int fd, const void* buf, size_t n, off_t off)
{
    initOnce();
    EntryPtr e = findEntry(fd);
    return R->pwrite(e ? e->stageFd : fd, buf, n, off);
}

ssize_t read(int fd, void* buf, size_t n)
{
    initOnce();
    EntryPtr e = findEntry(fd);
    return R->read(e ? e->stageFd : fd, buf, n);
}

ssize_t pread(int fd, void* buf, size_t n, off_t off)
{
    initOnce();
    EntryPtr e = findEntry(fd);
    return R->pread(e ? e->stageFd : fd, buf, n, off);
}

off_t lseek(int fd, off_t off, int whence)
{
    initOnce();
    EntryPtr e = findEntry(fd);
    return R->lseek(e ? e->stageFd : fd, off, whence);
}

int close(int fd)
{
    initOnce();
    if (closeCommon(fd))
        return 0;
    return R->close(fd);
}

int fsync(int fd)
{
    initOnce();
    int status;
    if (syncEntry(fd, &status))
        return status;
    return R->fsync(fd);
}

int fdatasync(int fd)
{
    initOnce();
    int status;
    if (syncEntry(fd, &status))
        return status;
    return R->fdatasync(fd);
}

int unlink(const char* path)
{
    initOnce();
    const std::string abs = G->cfg.enabled ? absPath(AT_FDCWD, path) : std::string();
    if (underStore(abs))
    {
        std::lock_guard<std::mutex> lk(G->mtx);
        auto it = G->byFinal.find(abs);
        if (it != G->byFinal.end())
        {
            EntryPtr e = it->second;
            e->tombstone = true;
            if (e->stageFd >= 0)
                R->unlink(e->stagePath.c_str());
            if (R->unlink(abs.c_str()) != 0 && errno != ENOENT)
                return -1;
            if (e->refs == 0)
            {
                if (e->stageFd >= 0)
                {
                    R->close(e->stageFd);
                    e->stageFd = -1;
                }
                G->byFinal.erase(it);
                G->cv.notify_all();
            }
            return 0;
        }
    }
    return R->unlink(path);
}

int rename(const char* from, const char* to)
{
    initOnce();
    const std::string absF = G->cfg.enabled ? absPath(AT_FDCWD, from) : std::string();
    if (underStore(absF))
    {
        const std::string absT = absPath(AT_FDCWD, to);
        if (!underStore(absT))
        {
            if (!G->escapeWarned.exchange(true))
                fprintf(stderr,
                        "bblite[%d]: WARNING rename of staged %s outside the store -> "
                        "passthrough (may fail or lose undrained data)\n",
                        (int)getpid(), absF.c_str());
        }
        else
        {
            std::lock_guard<std::mutex> lk(G->mtx);
            auto it = G->byFinal.find(absF);
            if (it != G->byFinal.end())
            {
                EntryPtr e = it->second;
                std::string newStage = G->cfg.stageDir + absT.substr(G->cfg.store.size());
                const size_t slash = newStage.rfind('/');
                if (slash != std::string::npos && mkdirP(newStage.substr(0, slash)) != 0)
                    return -1;
                if (R->rename(e->stagePath.c_str(), newStage.c_str()) != 0 && errno != ENOENT)
                    return -1;
                if (e->copied > 0 && R->rename(absF.c_str(), absT.c_str()) != 0 && errno != ENOENT)
                    return -1;
                e->stagePath = newStage;
                e->finalPath = absT;
                G->byFinal.erase(it);
                G->byFinal[absT] = e;
                logMsg("renamed staged %s -> %s\n", absF.c_str(), absT.c_str());
                return 0;
            }
        }
    }
    return R->rename(from, to);
}

int stat(const char* path, struct stat* stbuf)
{
    initOnce();
    const std::string abs = G->cfg.enabled ? absPath(AT_FDCWD, path) : std::string();
    if (underStore(abs))
    {
        std::lock_guard<std::mutex> lk(G->mtx);
        auto it = G->byFinal.find(abs);
        if (it != G->byFinal.end() && it->second->stageFd >= 0)
            return R->fstat(it->second->stageFd, stbuf);
    }
    return R->stat(path, stbuf);
}

int lstat(const char* path, struct stat* stbuf)
{
    return stat(path, stbuf); // v0: staged files are regular files, no symlink magic
}

FILE* fopen(const char* path, const char* mode)
{
    initOnce();
    FILE* f = fopenCommon(path, mode);
    return f ? f : R->fopen(path, mode);
}

FILE* fopen64(const char* path, const char* mode)
{
    initOnce();
    FILE* f = fopenCommon(path, mode);
    return f ? f : R->fopen64(path, mode);
}

int fclose(FILE* f)
{
    initOnce();
    int rc = 0;
    if (fcloseCommon(f, &rc))
        return rc;
    return R->fclose(f);
}

} // extern "C"
