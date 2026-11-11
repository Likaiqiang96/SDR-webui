// sdrwebd.cpp — RSP1 网页看板后端（C++，替换原 Python 版本）
// 单文件：HTTP + WebSocket + SoapySDR 采集 + FFT 频谱 + 解调 + Opus(ffmpeg 子进程)
//
// 用法: ./sdrwebd [--port 8095] [--static DIR] [--bookmarks FILE]

#include <SoapySDR/Device.hpp>
#include <SoapySDR/Formats.hpp>
#include <fftw3.h>
#include <nlohmann/json.hpp>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <cmath>
#include <complex>
#include <csignal>
#include <climits>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <cstdarg>
#include <deque>
#include <fstream>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include <arpa/inet.h>
#include <dirent.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/epoll.h>
#include <poll.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>

using json = nlohmann::json;
using Clock = std::chrono::steady_clock;
using cfloat = std::complex<float>;

// ------------------------------------------------------------------ 基础
static std::atomic<bool> g_run{true};
static int g_port = 8095;
static std::string g_static;
static std::string g_bookmark_file;

static double now_s() {
    static const auto t0 = Clock::now();
    return std::chrono::duration<double>(Clock::now() - t0).count();
}

static void logf_(const char* fmt, ...) {
    char buf[512];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    time_t t = time(nullptr);
    struct tm tmv;
    localtime_r(&t, &tmv);
    char ts[32];
    strftime(ts, sizeof(ts), "%H:%M:%S", &tmv);
    fprintf(stderr, "%s %s\n", ts, buf);
}

// ------------------------------------------------------------------ 参数
struct Params {
    double freq = 100e6;
    double rate = 2e6;
    double offset = 0;
    double bw = 180000;
    double squelch = -100;
    double gain = 21;
    std::string mode = "WFM";
    int bits = 12;
    bool paused = true;
};

static std::mutex g_mtx;
static Params g_p;
static std::atomic<bool> g_dirty{true};

// ------------------------------------------------------------------ 全局状态
static std::atomic<int> g_clients{0};
static std::atomic<bool> g_scan_req{false};
static std::atomic<double> g_iq_mbps{0}, g_down_kbps{0}, g_audio_kbps{0};
static std::atomic<double> g_snr{0};
static std::atomic<bool> g_running{false};
static std::string g_error;

static const int SPEC_BINS = 1024;
static std::mutex g_spec_mtx;
static uint8_t g_spec[SPEC_BINS];

// ------------------------------------------------------------------ 直接发送
static void send_all(int fd, const char* data, size_t n, bool drop_if_busy) {
    size_t off = 0;
    while (off < n) {
        ssize_t w = ::send(fd, data + off, n - off, MSG_NOSIGNAL | MSG_DONTWAIT);
        if (w > 0) { off += (size_t)w; continue; }
        if (w < 0 && (errno == EAGAIN || errno == EWOULDBLOCK) && !drop_if_busy) {
            struct pollfd pfd{fd, POLLOUT, 0};
            if (poll(&pfd, 1, 200) <= 0) return;
            continue;
        }
        return;   // 阻塞/出错：丢这一帧
    }
}

// ------------------------------------------------------------------ WS 帧 / HTTP
static std::string b64(const uint8_t* d, size_t n) {
    static const char* T = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string o;
    o.reserve((n + 2) / 3 * 4);
    for (size_t i = 0; i < n; i += 3) {
        uint32_t v = d[i] << 16;
        if (i + 1 < n) v |= d[i + 1] << 8;
        if (i + 2 < n) v |= d[i + 2];
        o += T[(v >> 18) & 63];
        o += T[(v >> 12) & 63];
        o += (i + 1 < n) ? T[(v >> 6) & 63] : '=';
        o += (i + 2 < n) ? T[v & 63] : '=';
    }
    return o;
}

// SHA1（仅用于 WS 握手）
static void sha1(const uint8_t* d, size_t n, uint8_t out[20]) {
    uint32_t h[5] = {0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476, 0xC3D2E1F0};
    uint64_t bits = (uint64_t)n * 8;
    std::vector<uint8_t> m(d, d + n);
    m.push_back(0x80);
    while (m.size() % 64 != 56) m.push_back(0);
    for (int i = 7; i >= 0; i--) m.push_back((bits >> (i * 8)) & 0xff);
    for (size_t off = 0; off < m.size(); off += 64) {
        uint32_t w[80];
        for (int i = 0; i < 16; i++)
            w[i] = (m[off + i * 4] << 24) | (m[off + i * 4 + 1] << 16) |
                   (m[off + i * 4 + 2] << 8) | m[off + i * 4 + 3];
        for (int i = 16; i < 80; i++) {
            uint32_t v = w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16];
            w[i] = (v << 1) | (v >> 31);
        }
        uint32_t a = h[0], b = h[1], c = h[2], d0 = h[3], e = h[4];
        for (int i = 0; i < 80; i++) {
            uint32_t f, k;
            if (i < 20) { f = (b & c) | (~b & d0); k = 0x5A827999; }
            else if (i < 40) { f = b ^ c ^ d0; k = 0x6ED9EBA1; }
            else if (i < 60) { f = (b & c) | (b & d0) | (c & d0); k = 0x8F1BBCDC; }
            else { f = b ^ c ^ d0; k = 0xCA62C1D6; }
            uint32_t t = ((a << 5) | (a >> 27)) + f + e + k + w[i];
            e = d0; d0 = c; c = (b << 30) | (b >> 2); b = a; a = t;
        }
        h[0] += a; h[1] += b; h[2] += c; h[3] += d0; h[4] += e;
    }
    for (int i = 0; i < 5; i++) {
        out[i * 4] = h[i] >> 24; out[i * 4 + 1] = h[i] >> 16;
        out[i * 4 + 2] = h[i] >> 8; out[i * 4 + 3] = h[i];
    }
}

static std::string ws_accept_key(const std::string& key) {
    static const char* GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
    std::string s = key + GUID;
    uint8_t h[20];
    sha1((const uint8_t*)s.data(), s.size(), h);
    return b64(h, 20);
}

// 客户端 -> 服务器 帧解析（需要 unmask）
struct WsParser {
    std::string buf;
    // 返回 false 表示需要更多数据
    bool next(std::string& payload, int& opcode) {
        if (buf.size() < 2) return false;
        uint8_t b0 = buf[0], b1 = buf[1];
        int op = b0 & 0x0f;
        size_t len = b1 & 0x7f;
        size_t pos = 2;
        if (len == 126) {
            if (buf.size() < 4) return false;
            len = ((uint8_t)buf[2] << 8) | (uint8_t)buf[3];
            pos = 4;
        } else if (len == 127) {
            if (buf.size() < 10) return false;
            len = 0;
            for (int i = 0; i < 8; i++) len = (len << 8) | (uint8_t)buf[2 + i];
            pos = 10;
        }
        bool masked = b1 & 0x80;
        size_t mpos = pos;
        if (masked) {
            if (buf.size() < pos + 4) return false;
            pos += 4;
        }
        if (buf.size() < pos + len) return false;
        payload.assign(buf, pos, len);
        if (masked) {
            uint8_t mk[4] = {(uint8_t)buf[mpos], (uint8_t)buf[mpos + 1],
                             (uint8_t)buf[mpos + 2], (uint8_t)buf[mpos + 3]};
            for (size_t i = 0; i < len; i++) payload[i] ^= mk[i & 3];
        }
        opcode = op;
        buf.erase(0, pos + len);
        return true;
    }
};

static void ws_frame(int fd, uint8_t opcode, const void* data, size_t n) {
    std::string h;
    h.push_back((char)(0x80 | opcode));
    if (n < 126) {
        h.push_back((char)n);
    } else if (n < 65536) {
        h.push_back(126);
        h.push_back((char)((n >> 8) & 0xff));
        h.push_back((char)(n & 0xff));
    } else {
        h.push_back(127);
        for (int i = 7; i >= 0; i--) h.push_back((char)((n >> (i * 8)) & 0xff));
    }
    std::string msg = h;
    msg.append((const char*)data, n);
    send_all(fd, msg.data(), msg.size(), true);
}

static void ws_text(int fd, const std::string& s) { ws_frame(fd, 0x1, s.data(), s.size()); }
struct Conn;
static std::mutex g_conn_mtx;
static std::map<int, std::shared_ptr<Conn>> g_conns;
static void broadcast_bin(const char* d, size_t n, bool skip_opus);
static void broadcast_text(const std::string& s);
static void ws_bin(int fd, const std::string& s) { ws_frame(fd, 0x2, s.data(), s.size()); }

// ------------------------------------------------------------------ 书签
static json bookmarks_load() {
    std::ifstream f(g_bookmark_file);
    if (f) {
        try {
            json j;
            f >> j;
            if (j.contains("lists")) return j;
        } catch (...) {}
    }
    return json{{"selectedList", "General"},
                {"bookmarkDisplayMode", 1},
                {"lists", {{"General", {{"showOnWaterfall", true}, {"bookmarks", json::object()}}}}}};
}

static void bookmarks_save(const json& j) {
    std::string tmp = g_bookmark_file + ".tmp";
    {
        std::ofstream f(tmp);
        if (!f) return;
        f << j.dump(4);
    }
    rename(tmp.c_str(), g_bookmark_file.c_str());
}

// ------------------------------------------------------------------ Opus 编码器（ffmpeg 子进程）
class OpusEnc {
  public:
    OpusEnc(const std::function<void(const char*, size_t)>& sink) : sink_(sink) {}
    ~OpusEnc() { close(); }

    bool start(double rate) {
        int ip[2], op[2];
        if (pipe(ip) || pipe(op)) return false;
        pid_ = fork();
        if (pid_ == 0) {
            dup2(ip[0], 0);
            dup2(op[1], 1);
            ::close(ip[0]); ::close(ip[1]); ::close(op[0]); ::close(op[1]);
            execlp("ffmpeg", "ffmpeg", "-hide_banner", "-loglevel", "error",
                   "-f", "s16le", "-ar", std::to_string((int)rate).c_str(), "-ac", "1", "-i", "pipe:0",
                   "-c:a", "libopus", "-b:a", "40000", "-vbr", "on",
                   "-application", "audio", "-frame_duration", "10",
                   "-cluster_time_limit", "60", "-cluster_size_limit", "0",
                   "-f", "webm", "pipe:1", (char*)nullptr);
            _exit(127);
        }
        ::close(ip[0]);
        ::close(op[1]);
        in_ = ip[1];
        out_ = op[0];
        reader_ = std::thread([this] {
            char buf[4096];
            while (true) {
                ssize_t n = read(out_, buf, sizeof(buf));
                if (n <= 0) break;
                if (closed_) break;
                sink_(buf, (size_t)n);
            }
        });
        return true;
    }

    void write(const int16_t* pcm, size_t n) {
        if (in_ < 0 || closed_) return;
        size_t bytes = n * 2;
        const char* p = (const char*)pcm;
        while (bytes) {
            ssize_t w = ::write(in_, p, bytes);
            if (w <= 0) break;
            p += w;
            bytes -= (size_t)w;
        }
    }

    void close() {
        if (closed_.exchange(true)) return;
        if (in_ >= 0) { ::close(in_); in_ = -1; }
        if (reader_.joinable()) reader_.join();
        if (out_ >= 0) { ::close(out_); out_ = -1; }
        if (pid_ > 0) { int st; waitpid(pid_, &st, WNOHANG); pid_ = -1; }
    }

  private:
    std::function<void(const char*, size_t)> sink_;
    pid_t pid_ = -1;
    int in_ = -1, out_ = -1;
    std::atomic<bool> closed_{false};
    std::thread reader_;
};


static std::atomic<double> g_opus_bytes{0};
static std::atomic<int> g_enc_count{0};
static std::mutex g_enc_mtx;
static std::map<int, std::shared_ptr<OpusEnc>> g_encs;

// ------------------------------------------------------------------ DSP 工具
static void design_lowpass(std::vector<float>& h, int taps, double cutoff_norm) {
    h.resize(taps);
    double sum = 0;
    int M = (taps - 1) / 2;
    for (int i = 0; i < taps; i++) {
        double x = i - M;
        double sinc = (x == 0) ? 2 * cutoff_norm : sin(2 * M_PI * cutoff_norm * x) / (M_PI * x);
        double w = 0.54 - 0.46 * cos(2 * M_PI * i / (taps - 1));   // Hamming
        h[i] = (float)(sinc * w);
        sum += h[i];
    }
    for (auto& v : h) v /= (float)sum;
}

struct Fir {
    std::vector<float> h;
    std::vector<float> z;
    size_t pos = 0;
    void init(const std::vector<float>& hh) {
        h = hh;
        z.assign(h.size(), 0.0f);
        pos = 0;
    }
    inline float step(float x) {
        z[pos] = x;
        float y = 0;
        size_t idx = pos;
        for (size_t i = 0; i < h.size(); i++) {
            y += h[i] * z[idx];
            idx = idx ? idx - 1 : h.size() - 1;
        }
        pos = (pos + 1) % h.size();
        return y;
    }
    // 多相抽取：只算输出点
    void decimate(const float* x, int n, int D, std::vector<float>& out) {
        out.clear();
        out.reserve(n / D + 2);
        const size_t H = h.size();
        for (int i = 0; i < n; i++) {
            z[pos] = x[i];
            if (i % D == 0) {
                float y = 0;
                size_t idx = pos;
                for (size_t k = 0; k < H; k++) {
                    y += h[k] * z[idx];
                    idx = idx ? idx - 1 : H - 1;
                }
                out.push_back(y);
            }
            pos = (pos + 1) % H;
        }
    }
};

// ------------------------------------------------------------------ SDR
class Sdr {
  public:
    Sdr();
    ~Sdr();
    void run();
    void apply(const json& kw);
    json params_json();
    void push_status();
    void request_scan() { scan_req_ = true; }
    double audio_rate() { return audio_rate_; }

  private:
    bool ensure_dev();
    void close_dev();
    void reset_dsp(double rate);
    void read_block();
    void demod(const cfloat* z, int n);
    void push_spectrum();
    void do_scan();
    void setup_filters();

    SoapySDR::Device* dev_ = nullptr;
    SoapySDR::Stream* stream_ = nullptr;
    std::vector<int16_t> buf_;
    std::atomic<bool> scan_req_{false};
    std::atomic<bool> rate_dirty_{false};
    bool scanning_ = false;

    static const int READ_N = 16384;

    // DSP 状态
    double audio_rate_ = 48000;
    int dem1_ = 1, dec2_ = 1;
    std::vector<cfloat> spec_acc_;
    std::vector<float> audio_acc_;
    cfloat last8_{0, 0};
    double mix_phase8_ = 0;
    float dc_state_ = 0;
    float dc_coef_ = 0.99f;
    float deemph_a_ = 0, deemph_state_ = 0;
    Fir dec_fir_, audio_fir_, deemph_fir_;
    bool audio_lp_on_ = false;
    double squelch_hold_ = 0;
    fftwf_plan fft_plan_ = nullptr;
    std::vector<cfloat> fft_in_;
    std::vector<cfloat> fft_out_;
    std::vector<float> win_;
    int fft_n_ = 16384;

    // 统计
    double iq_bytes_ = 0, pcm_bytes_ = 0, opus_bytes_ = 0, spec_bytes_ = 0;
    double mark_ = 0, iq_rate_ = 0, audio_kbps_ = 0, down_kbps_ = 0, snr_ = 0;
    double rate_mark_ = 0, rate_iq_ = 0, rate_spec_ = 0, rate_pcm_ = 0, rate_opus_ = 0;
    std::vector<cfloat> z_buf_;
    std::vector<float> z8r_, z8i_;
    std::vector<int16_t> pcm_buf_ = std::vector<int16_t>(1024, 0);
    float last_re_ = 0, last_im_ = 0;
    double mix_phase8_ssb_ = 0;
    std::vector<float> a_buf_, d2_buf_;
    double iq_mark_ = 0, pcm_mark_ = 0, opus_mark_ = 0, spec_mark_ = 0;
    double status_mark_ = 0, spec_mark_t_ = 0;
};

static Sdr* g_sdr = nullptr;

Sdr::Sdr() {
    buf_.resize(READ_N * 2);
    fft_in_.resize(fft_n_);
    fft_out_.resize(fft_n_);
    win_.resize(fft_n_);
    for (int i = 0; i < fft_n_; i++) win_[i] = 0.5f - 0.5f * cosf(2 * M_PI * i / (fft_n_ - 1));
    fft_plan_ = fftwf_plan_dft_1d(fft_n_, (fftwf_complex*)fft_in_.data(),
                                  (fftwf_complex*)fft_out_.data(), FFTW_FORWARD, FFTW_ESTIMATE);
    mark_ = now_s();
    reset_dsp(g_p.rate);
}

Sdr::~Sdr() {
    close_dev();
    if (fft_plan_) fftwf_destroy_plan(fft_plan_);
}

void Sdr::setup_filters() {
    std::vector<float> h;
    design_lowpass(h, 65, 0.8 / dec2_);
    dec_fir_.init(h);
    double cutoff = std::min(15000.0, audio_rate_ * 0.45);
    design_lowpass(h, 129, cutoff / (audio_rate_ / 2));
    audio_fir_.init(h);
    float tau = 50e-6f;
    float dt = (float)(1.0 / audio_rate_);
    deemph_a_ = dt / (tau + dt);
    deemph_fir_.init({deemph_a_});
    dc_coef_ = 1.0f - 2.0f * (float)M_PI * 20.0f / (float)audio_rate_;
}

void Sdr::reset_dsp(double rate) {
    int d1 = (int)lround(rate / 1e6);
    if (d1 < 1) d1 = 1;
    if (d1 > 8) d1 = 8;
    dem1_ = d1;
    dec2_ = std::max(1, (int)lround((rate / d1) / 48000.0));
    audio_rate_ = rate / d1 / dec2_;
    last8_ = cfloat(0, 0);
    mix_phase8_ = 0;
    dc_state_ = 0;
    deemph_state_ = 0;
    audio_acc_.clear();
    spec_acc_.clear();
    squelch_hold_ = 0;
    setup_filters();
}

json Sdr::params_json() {
    std::lock_guard<std::mutex> lk(g_mtx);
    return json{{"freq", g_p.freq}, {"rate", g_p.rate}, {"offset", g_p.offset},
                {"bw", g_p.bw}, {"squelch", g_p.squelch}, {"gain", g_p.gain},
                {"mode", g_p.mode}, {"bits", g_p.bits}, {"paused", g_p.paused}};
}

void Sdr::apply(const json& kw) {
    bool rate_changed = false;
    {
        std::lock_guard<std::mutex> lk(g_mtx);
        auto setd = [&](const char* k, double& dst) {
            if (kw.contains(k) && kw[k].is_number()) {
                double v = kw[k].get<double>();
                if (std::abs(v - dst) > 0.5) {
                    if (std::string(k) == "rate") rate_changed = true;
                    dst = v;
                }
            }
        };
        setd("freq", g_p.freq);
        setd("rate", g_p.rate);
        setd("offset", g_p.offset);
        setd("bw", g_p.bw);
        setd("squelch", g_p.squelch);
        setd("gain", g_p.gain);
        if (kw.contains("mode") && kw["mode"].is_string()) g_p.mode = kw["mode"].get<std::string>();
        if (kw.contains("bits") && kw["bits"].is_number()) g_p.bits = kw["bits"].get<int>();
        if (kw.contains("paused") && kw["paused"].is_boolean()) g_p.paused = kw["paused"].get<bool>();
    }
    if (rate_changed) rate_dirty_ = true;
    g_dirty = true;
}

bool Sdr::ensure_dev() {
    if (dev_) return true;
    try {
        SoapySDR::Kwargs args{{"driver", "sdrplay"}};
        dev_ = SoapySDR::Device::make(args);
        if (!dev_) return false;
        Params p;
        {
            std::lock_guard<std::mutex> lk(g_mtx);
            p = g_p;
        }
        dev_->setSampleRate(SOAPY_SDR_RX, 0, p.rate);
        dev_->setFrequency(SOAPY_SDR_RX, 0, p.freq);
        try { dev_->setGainMode(SOAPY_SDR_RX, 0, false); } catch (...) {}
        dev_->setGain(SOAPY_SDR_RX, 0, 42.0 - p.gain);
        stream_ = dev_->setupStream(SOAPY_SDR_RX, SOAPY_SDR_CS16);
        if (dev_->activateStream(stream_) != 0) {
            logf_("activateStream 失败");
            throw std::runtime_error("activateStream failed");
        }
        {
            std::lock_guard<std::mutex> lk(g_mtx);
            reset_dsp(p.rate);
        }
        g_running = true;
        logf_("SDR 已打开: %.3f MHz, %.2f MSps", p.freq / 1e6, p.rate / 1e6);
        return true;
    } catch (std::exception& e) {
        logf_("SDR 打开失败: %s", e.what());
        close_dev();
        return false;
    }
}

void Sdr::close_dev() {
    if (!dev_) return;
    try {
        if (stream_) {
            dev_->deactivateStream(stream_);
            dev_->closeStream(stream_);
            stream_ = nullptr;
        }
        SoapySDR::Device::unmake(dev_);
    } catch (...) {}
    dev_ = nullptr;
    g_running = false;
    logf_("SDR 已释放");
}

void Sdr::read_block() {
    void* bufs[1] = {buf_.data()};
    int flags = 0;
    long long timeNs = 0;
    int ret = dev_->readStream(stream_, bufs, READ_N, flags, timeNs, 200000);
    if (ret <= 0) return;
    int n = ret;

    if ((int)z_buf_.size() < n) z_buf_.resize(n);
    const float scale = 1.0f / 32768.0f;
    for (int i = 0; i < n; i++)
        z_buf_[i] = cfloat(buf_[2 * i] * scale, buf_[2 * i + 1] * scale);
    cfloat* z = z_buf_.data();

    iq_bytes_ += (double)n * 4;
    if (now_s() - rate_mark_ >= 1.0) {
        double dt = now_s() - rate_mark_;
        rate_mark_ = now_s();
        g_iq_mbps = (iq_bytes_ - rate_iq_) / dt / 1e6;
        g_down_kbps = ((spec_bytes_ - rate_spec_) + (pcm_bytes_ - rate_pcm_)) / dt / 1024.0;
        {
            double o = g_opus_bytes.load();
            g_audio_kbps = (g_enc_count.load() > 0) ? ((o - rate_opus_) * 8 / dt / 1000.0)
                                                    : ((pcm_bytes_ - rate_pcm_) * 8 / dt / 1000.0);
            rate_opus_ = o;
        }
        rate_iq_ = iq_bytes_; rate_spec_ = spec_bytes_; rate_pcm_ = pcm_bytes_;
    }

    // 频谱累积
    spec_acc_.insert(spec_acc_.end(), z, z + n);
    if ((int)spec_acc_.size() > fft_n_ * 4)
        spec_acc_.erase(spec_acc_.begin(), spec_acc_.end() - fft_n_ * 2);
    if (now_s() - spec_mark_t_ >= 0.08 && !scanning_) {
        spec_mark_t_ = now_s();
        push_spectrum();
    }

    demod(z, n);
}

void Sdr::push_spectrum() {
    if ((int)spec_acc_.size() < fft_n_) return;
    const cfloat* src = spec_acc_.data() + (spec_acc_.size() - fft_n_);
    for (int i = 0; i < fft_n_; i++) fft_in_[i] = src[i] * win_[i];
    fftwf_execute(fft_plan_);
    int half = fft_n_ / 2, k = fft_n_ / SPEC_BINS;
    float norm = 20.0f * log10f((float)fft_n_ / 4.0f);
    uint8_t out[SPEC_BINS];
    for (int b = 0; b < SPEC_BINS; b++) {
        float mx = -1e30f;
        for (int j = 0; j < k; j++) {
            int idx = (half + b * k + j) % fft_n_;   // fftshift 后
            cfloat v = fft_out_[idx];
            float p = v.real() * v.real() + v.imag() * v.imag();
            mx = std::max(mx, p);
        }
        float db = 10.0f * log10f(mx + 1e-12f) - norm;
        int q = (int)lroundf(db + 140.0f);
        out[b] = (uint8_t)std::min(255, std::max(0, q));
    }
    {
        std::lock_guard<std::mutex> lk(g_spec_mtx);
        memcpy(g_spec, out, SPEC_BINS);
        // SNR：中间 3/4 的峰值 - 中位数
        std::vector<float> mid;
        mid.reserve(SPEC_BINS * 3 / 4);
        for (int i = SPEC_BINS / 8; i < SPEC_BINS * 7 / 8; i++) mid.push_back((float)out[i] - 140.0f);
        std::vector<float> sorted2 = mid;
        std::sort(sorted2.begin(), sorted2.end());
        float med = sorted2[sorted2.size() / 2];
        float pk = *std::max_element(mid.begin(), mid.end());
        snr_ = std::max(0.0f, pk - med);
        g_snr = snr_;
    }
    std::string frame;
    frame.push_back('S');
    frame.push_back(0);
    frame.push_back((char)(SPEC_BINS & 0xff));
    frame.push_back((char)((SPEC_BINS >> 8) & 0xff));
    frame.append((const char*)out, SPEC_BINS);
    spec_bytes_ += (double)frame.size();
    broadcast_bin(frame.data(), frame.size(), false);
}

void Sdr::demod(const cfloat* zin, int n) {
    Params p;
    {
        std::lock_guard<std::mutex> lk(g_mtx);
        p = g_p;
    }
    const double fs = p.rate;
    const int D = dem1_;
    const double fsd = fs / D;
    int nd = n / D;
    if (nd < 8) return;

    const double off = p.offset;
    int k = 0;
    double f_res = 0;
    float c_re[8], c_im[8];
    bool use_coarse = false;
    if (std::abs(off) > 1.0) {
        k = (int)llround(off / fsd);
        f_res = off - k * fsd;
        int kk = ((k % D) + D) % D;
        if (kk) {
            use_coarse = true;
            for (int i = 0; i < D; i++) {
                double ang = -2.0 * M_PI * kk * i / D;
                c_re[i] = (float)cos(ang);
                c_im[i] = (float)sin(ang);
            }
        }
    }

    if ((int)z8r_.size() < nd) { z8r_.resize(nd); z8i_.resize(nd); }
    float* zr = z8r_.data();
    float* zi = z8i_.data();
    const float invD = 1.0f / (float)D;
    // 抽取到 ~1MHz（裸 float 运算）
    for (int i = 0; i < nd; i++) {
        const cfloat* src = zin + (size_t)i * D;
        float ar = 0, ai = 0;
        if (use_coarse) {
            for (int j = 0; j < D; j++) {
                float vr = src[j].real(), vi = src[j].imag();
                ar += vr * c_re[j] - vi * c_im[j];
                ai += vr * c_im[j] + vi * c_re[j];
            }
        } else {
            for (int j = 0; j < D; j++) {
                ar += src[j].real();
                ai += src[j].imag();
            }
        }
        zr[i] = ar * invD;
        zi[i] = ai * invD;
    }
    if (std::abs(f_res) > 1.0) {
        double dphi = -2.0 * M_PI * f_res / fsd;
        double ph = dphi * mix_phase8_;
        for (int i = 0; i < nd; i++) {
            float cr = (float)cos(ph), ci = (float)sin(ph);
            float vr = zr[i], vi = zi[i];
            zr[i] = vr * cr - vi * ci;
            zi[i] = vr * ci + vi * cr;
            ph += dphi;
        }
    }
    mix_phase8_ += nd;

    if ((int)a_buf_.size() < nd) a_buf_.resize(nd);
    float* a = a_buf_.data();
    const double bw = p.bw;
    if (p.mode == "WFM" || p.mode == "NFM") {
        float lr = last_re_, li = last_im_;
        float g = (p.mode == "NFM") ? 0.7f : 1.0f;
        for (int i = 0; i < nd; i++) {
            float cr = zr[i], ci = zi[i];
            float dr = cr * lr + ci * li;
            float di = ci * lr - cr * li;
            lr = cr; li = ci;
            float m = sqrtf(dr * dr + di * di) + 1e-9f;
            a[i] = di / m * g;
        }
        last_re_ = lr; last_im_ = li;
    } else if (p.mode == "AM" || p.mode == "DSB") {
        float mean = 0;
        for (int i = 0; i < nd; i++) {
            a[i] = sqrtf(zr[i] * zr[i] + zi[i] * zi[i]);
            mean += a[i];
        }
        mean /= (float)nd;
        for (int i = 0; i < nd; i++) a[i] -= mean;
    } else if (p.mode == "USB" || p.mode == "LSB" || p.mode == "CW") {
        double f = (p.mode == "USB") ? bw / 2 : (p.mode == "LSB" ? -bw / 2 : 700.0);
        double dphi = -2.0 * M_PI * f / fsd;
        double ph = dphi * mix_phase8_ssb_;
        for (int i = 0; i < nd; i++) {
            float cr = (float)cos(ph), ci = (float)sin(ph);
            a[i] = zr[i] * cr - zi[i] * ci;
            ph += dphi;
        }
        mix_phase8_ssb_ += nd;
    } else {
        for (int i = 0; i < nd; i++) a[i] = zr[i];
    }

    auto& d2 = d2_buf_;
    dec_fir_.decimate(a, nd, dec2_, d2);

    double cutoff = std::min((p.mode == "WFM") ? 15000.0 : bw / 2.0, audio_rate_ * 0.45);
    bool need_lp = cutoff < audio_rate_ * 0.45;
    for (size_t i = 0; i < d2.size(); i++) {
        float v = d2[i];
        if (need_lp) v = audio_fir_.step(v);
        if (p.mode == "WFM" || p.mode == "NFM") {
            deemph_state_ = deemph_state_ + deemph_a_ * (v - deemph_state_);
            v = deemph_state_;
        }
        dc_state_ = dc_coef_ * dc_state_ + (1 - dc_coef_) * v;
        v -= dc_state_;
        audio_acc_.push_back(v);
    }
    bool mute = false;
    if (p.squelch > -99) {
        double sum = 0;
        for (float v : audio_acc_) sum += v * v;
        double lvl = 10.0 * log10(std::max(1e-12, sum / std::max<size_t>(1, audio_acc_.size())));
        if (lvl >= p.squelch) squelch_hold_ = now_s();
        if (now_s() - squelch_hold_ > 0.25) mute = true;
    }

    const size_t CHUNK = 1024;
    while (audio_acc_.size() >= CHUNK) {
        for (size_t i = 0; i < CHUNK; i++) {
            float v = mute ? 0.0f : audio_acc_[i] * 6.0f;
            v = std::min(1.0f, std::max(-1.0f, v));
            pcm_buf_[i] = (int16_t)lrintf(v * 32767.0f);
        }
        audio_acc_.erase(audio_acc_.begin(), audio_acc_.begin() + CHUNK);

        std::string frame;
        frame.reserve(4 + CHUNK * 2);
        frame.push_back('A');
        frame.push_back(0);
        frame.push_back((char)(CHUNK & 0xff));
        frame.push_back((char)((CHUNK >> 8) & 0xff));
        frame.append((const char*)pcm_buf_.data(), CHUNK * 2);
        pcm_bytes_ += (double)frame.size();
        broadcast_bin(frame.data(), frame.size(), true);
        {
            std::lock_guard<std::mutex> lk(g_enc_mtx);
            for (auto& kv : g_encs) kv.second->write(pcm_buf_.data(), CHUNK);
        }
    }
}

void Sdr::push_status() {
    Params p;
    {
        std::lock_guard<std::mutex> lk(g_mtx);
        p = g_p;
    }
    json j{{"type", "status"}, {"freq", p.freq}, {"offset", p.offset}, {"rate", p.rate},
           {"mode", p.mode}, {"bits", p.bits}, {"bw", p.bw}, {"squelch", p.squelch},
           {"gain", p.gain}, {"running", (bool)g_running}, {"paused", p.paused},
           {"iq_mbps", (double)g_iq_mbps}, {"down_kbps", (double)g_down_kbps},
           {"audio_kbps", (double)g_audio_kbps}, {"codec", "opus"},
           {"clients", (int)g_clients}, {"snr", (double)g_snr},
           {"audio_rate", audio_rate_}, {"error", g_error}};
    broadcast_text(j.dump());
}

void Sdr::do_scan() {
    scanning_ = true;
    Params p;
    {
        std::lock_guard<std::mutex> lk(g_mtx);
        p = g_p;
    }
    const double f0 = 87.5e6, f1 = 108.0e6;
    const double rate = p.rate;
    const double span = rate * 0.8, step = span * 0.9;
    const int nfft = 4096;
    const double bin_hz = rate / nfft;
    std::vector<double> freqs(nfft);
    for (int i = 0; i < nfft; i++) freqs[i] = (i - nfft / 2) * bin_hz;
    std::vector<float> win(nfft);
    for (int i = 0; i < nfft; i++) win[i] = 0.5f - 0.5f * cosf(2 * M_PI * i / (nfft - 1));
    std::vector<cfloat> fin(nfft), fout(nfft);
    fftwf_plan plan = fftwf_plan_dft_1d(nfft, (fftwf_complex*)fin.data(),
                                        (fftwf_complex*)fout.data(), FFTW_FORWARD, FFTW_ESTIMATE);
    logf_("开始扫描 %.1f-%.1f MHz", f0 / 1e6, f1 / 1e6);
    std::vector<std::pair<double, double>> res;   // freq, db
    double old_freq = p.freq;
    for (double f = f0; f <= f1 + 1; f += step) {
        try {
            dev_->setFrequency(SOAPY_SDR_RX, 0, f);
            usleep(120000);
            void* bufs[1] = {buf_.data()};
            int flags = 0;
            long long tns = 0;
            for (int i = 0; i < 4; i++) dev_->readStream(stream_, bufs, READ_N, flags, tns, 100000);
            std::vector<cfloat> acc;
            acc.reserve(READ_N * 6);
            for (int r = 0; r < 6; r++) {
                int ret = dev_->readStream(stream_, bufs, READ_N, flags, tns, 200000);
                if (ret > 0)
                    for (int i = 0; i < ret; i++)
                        acc.push_back(cfloat(buf_[2 * i] / 32768.0f, buf_[2 * i + 1] / 32768.0f));
            }
            if ((int)acc.size() < nfft) continue;
            std::vector<float> pw(nfft, 0.0f);
            int nfr = std::min(4, (int)acc.size() / nfft);
            for (int fr = 0; fr < nfr; fr++) {
                const cfloat* seg = acc.data() + acc.size() - nfft * (fr + 1);
                for (int i = 0; i < nfft; i++) fin[i] = seg[i] * win[i];
                fftwf_execute(plan);
                for (int i = 0; i < nfft; i++) {
                    int idx = (i + nfft / 2) % nfft;
                    cfloat v = fout[idx];
                    pw[i] += v.real() * v.real() + v.imag() * v.imag();
                }
            }
            for (auto& v : pw) v /= (float)nfr;
            float norm = 20.0f * log10f((float)nfft / 4.0f);
            std::vector<float> db(nfft);
            for (int i = 0; i < nfft; i++) db[i] = 10.0f * log10f(pw[i] + 1e-12f) - norm;

            // 只在扫描范围内分析
            std::vector<int> sel;
            for (int i = 0; i < nfft; i++) {
                double fr = f + freqs[i];
                if (std::abs(freqs[i]) < span / 2 && fr >= f0 && fr <= f1) sel.push_back(i);
            }
            if (sel.size() < 64) continue;
            std::vector<float> dbv(sel.size()), fvv(sel.size());
            for (size_t i = 0; i < sel.size(); i++) { dbv[i] = db[sel[i]]; fvv[i] = freqs[sel[i]]; }
            std::vector<float> sorted = dbv;
            std::sort(sorted.begin(), sorted.end());
            float floor = sorted[sorted.size() / 5];
            float thr = floor + 6.0f;

            // 平滑后找连续段
            int k = std::max(3, (int)lround(6000.0 / bin_hz));
            if (k % 2 == 0) k++;
            std::vector<float> sm(dbv.size(), 0);
            for (size_t i = 0; i < dbv.size(); i++) {
                float s = 0; int c = 0;
                for (int j = -k / 2; j <= k / 2; j++) {
                    int idx = (int)i + j;
                    if (idx >= 0 && idx < (int)dbv.size()) { s += dbv[idx]; c++; }
                }
                sm[i] = s / c;
            }
            size_t i = 0;
            while (i < sm.size()) {
                if (sm[i] <= thr) { i++; continue; }
                size_t j = i;
                while (j + 1 < sm.size() && sm[j + 1] > thr) j++;
                // 合并 25kHz 内的碎片
                while (j + 1 < sm.size()) {
                    size_t gap_end = j + 1;
                    while (gap_end < sm.size() && sm[gap_end] <= thr) gap_end++;
                    if (gap_end < sm.size() && (gap_end - j - 1) * bin_hz < 25000) j = gap_end;
                    else break;
                    while (j + 1 < sm.size() && sm[j + 1] > thr) j++;
                }
                double width = (j - i + 1) * bin_hz;
                int pi = (int)i;
                for (size_t q = i; q <= j; q++) if (dbv[q] > dbv[pi]) pi = (int)q;
                float pk = dbv[pi];
                if (width >= 55e3 && pk >= -85 && pk - floor >= 14.0f) {
                    double sum = 0, wsum = 0;
                    for (size_t q = i; q <= j; q++) {
                        float w = powf(10.0f, std::min(0.0f, dbv[q] - pk) / 10.0f);
                        sum += w; wsum += w * fvv[q];
                    }
                    res.push_back({f + wsum / sum, pk});
                }
                i = j + 1;
            }
        } catch (...) {}
    }
    try { dev_->setFrequency(SOAPY_SDR_RX, 0, old_freq); } catch (...) {}
    fftwf_destroy_plan(plan);

    // 合并 + 0.1 MHz 网格
    std::sort(res.begin(), res.end(), [](auto& a, auto& b) { return a.first < b.first; });
    std::vector<std::pair<double, double>> merged;
    for (auto& r : res) {
        if (!merged.empty() && std::abs(r.first - merged.back().first) < 120e3) {
            if (r.second > merged.back().second) merged.back() = r;
        } else merged.push_back(r);
    }
    std::map<long long, std::pair<double, double>> grid;
    for (auto& r : merged) {
        long long q = llround(r.first / 100e3);
        auto it = grid.find(q);
        if (it == grid.end() || r.second > it->second.second) grid[q] = {q * 100000.0, r.second};
    }
    json arr = json::array();
    for (auto& kv : grid) {
        double f = kv.second.first;
        if (f < f0 || f > f1) continue;
        arr.push_back({{"freq", f}, {"db", std::round(kv.second.second * 10) / 10}});
    }
    scanning_ = false;
    logf_("扫描完成，%d 个台", (int)arr.size());
    json msg{{"type", "scan"}, {"stations", arr}};
    broadcast_text(msg.dump());
}

void Sdr::run() {
    while (g_run) {
        bool active, paused;
        {
            std::lock_guard<std::mutex> lk(g_mtx);
            paused = g_p.paused;
            active = g_clients > 0;
        }
        if (!active || paused) {
            if (dev_) close_dev();
            if (now_s() - status_mark_ > 0.5) { status_mark_ = now_s(); push_status(); }
            usleep(150000);
            continue;
        }
        if (rate_dirty_.exchange(false)) {
            close_dev();
            std::lock_guard<std::mutex> lk(g_mtx);
            reset_dsp(g_p.rate);
        }
        if (!ensure_dev()) { usleep(500000); continue; }

        // 应用参数
        if (g_dirty) {
            g_dirty = false;
            Params p;
            {
                std::lock_guard<std::mutex> lk(g_mtx);
                p = g_p;
            }
            try {
                dev_->setFrequency(SOAPY_SDR_RX, 0, p.freq);
                try { dev_->setGainMode(SOAPY_SDR_RX, 0, false); } catch (...) {}
        dev_->setGain(SOAPY_SDR_RX, 0, 42.0 - p.gain);
            } catch (...) {}
        }
        if (scan_req_.exchange(false)) {
            try { do_scan(); } catch (std::exception& e) { logf_("扫描异常: %s", e.what()); scanning_ = false; }
            continue;
        }
        if (now_s() - status_mark_ > 0.5) { status_mark_ = now_s(); push_status(); }
        try {
            read_block();
        } catch (std::exception& e) {
            logf_("处理异常: %s", e.what());
            close_dev();
            usleep(300000);
        }
    }
}

// ------------------------------------------------------------------ HTTP 服务
struct Conn {
    int fd;
    std::string in;
    WsParser ws;
    bool is_ws = false;
    bool closing = false;
    std::string codec = "pcm";
    double last_rx = 0;
    double last_ping = 0;
};


static void broadcast_bin(const char* d, size_t n, bool skip_opus) {
    std::vector<int> fds;
    {
        std::lock_guard<std::mutex> lk(g_conn_mtx);
        for (auto& kv : g_conns) {
            if (!kv.second->is_ws) continue;
            if (skip_opus && kv.second->codec == "opus") continue;
            fds.push_back(kv.first);
        }
    }
    for (int fd : fds) ws_frame(fd, 0x2, d, n);
}

static void broadcast_text(const std::string& s) {
    std::vector<int> fds;
    {
        std::lock_guard<std::mutex> lk(g_conn_mtx);
        for (auto& kv : g_conns)
            if (kv.second->is_ws) fds.push_back(kv.first);
    }
    for (int fd : fds) ws_text(fd, s);
}

static const char* mime_of(const std::string& p) {
    auto ends = [&](const char* s) {
        size_t n = strlen(s);
        return p.size() >= n && p.compare(p.size() - n, n, s) == 0;
    };
    if (ends(".html")) return "text/html; charset=utf-8";
    if (ends(".json")) return "application/json; charset=utf-8";
    if (ends(".js")) return "application/javascript";
    if (ends(".css")) return "text/css";
    if (ends(".png")) return "image/png";
    if (ends(".svg")) return "image/svg+xml";
    return "application/octet-stream";
}

static void http_send(int fd, int code, const char* mime, const std::string& body,
                      const std::string& extra = "") {
    std::string h = "HTTP/1.1 " + std::to_string(code) + " OK\r\n";
    h += std::string("Content-Type: ") + mime + "\r\n";
    h += "Content-Length: " + std::to_string(body.size()) + "\r\n";
    h += "Cache-Control: no-store\r\n";
    h += "Connection: close\r\n";
    h += extra;
    h += "\r\n";
    h += body;
    send_all(fd, h.data(), h.size(), false);
}

static std::string url_decode(const std::string& s) {
    std::string o;
    for (size_t i = 0; i < s.size(); i++) {
        if (s[i] == '%' && i + 2 < s.size()) {
            o += (char)strtol(s.substr(i + 1, 2).c_str(), nullptr, 16);
            i += 2;
        } else if (s[i] == '+') o += ' ';
        else o += s[i];
    }
    return o;
}

static void handle_api(int fd, const std::string& path, const std::string& body) {
    std::string p = path;
    auto qpos = p.find('?');
    std::string query = qpos == std::string::npos ? "" : p.substr(qpos + 1);
    if (qpos != std::string::npos) p = p.substr(0, qpos);
    std::string list = "General";
    {
        auto lp = query.find("list=");
        if (lp != std::string::npos) list = url_decode(query.substr(lp + 5));
    }

    if (p == "/api/res") {
        json out{{"bandplans", json::array()}, {"colormaps", json::array()}};
        DIR* d = opendir((g_static + "/../res/bandplans").c_str());
        if (d) {
            struct dirent* e;
            while ((e = readdir(d))) {
                std::string n = e->d_name;
                if (n.size() < 6 || n.substr(n.size() - 5) != ".json") continue;
                try {
                    std::ifstream f(g_static + "/../res/bandplans/" + n);
                    json j; f >> j;
                    out["bandplans"].push_back({{"file", n}, {"name", j.value("name", n)},
                                                {"country", j.value("country_name", "")}});
                } catch (...) {}
            }
            closedir(d);
        }
        d = opendir((g_static + "/../res/colormaps").c_str());
        if (d) {
            struct dirent* e;
            while ((e = readdir(d))) {
                std::string n = e->d_name;
                if (n.size() < 6 || n.substr(n.size() - 5) != ".json") continue;
                try {
                    std::ifstream f(g_static + "/../res/colormaps/" + n);
                    json j; f >> j;
                    out["colormaps"].push_back({{"file", n}, {"name", j.value("name", n)},
                                                {"map", j.value("map", json::array())}});
                } catch (...) {}
            }
            closedir(d);
        }
        http_send(fd, 200, "application/json", out.dump());
        return;
    }
    if (p == "/api/bookmarks") {
        if (body.empty()) {
            http_send(fd, 200, "application/json", bookmarks_load().dump());
        } else {
            try {
                json in = json::parse(body);
                json cur = bookmarks_load();
                if (in.contains("lists")) {
                    bookmarks_save(in);
                } else {
                    std::string l = in.value("list", cur.value("selectedList", "General"));
                    if (!cur["lists"].contains(l))
                        cur["lists"][l] = {{"showOnWaterfall", true}, {"bookmarks", json::object()}};
                    for (auto& [k, v] : in.value("bookmarks", json::object()).items())
                        cur["lists"][l]["bookmarks"][k] = v;
                    bookmarks_save(cur);
                }
                http_send(fd, 200, "application/json", "{\"ok\":true}");
            } catch (...) { http_send(fd, 400, "application/json", "{\"ok\":false}"); }
        }
        return;
    }
    if (p == "/api/bookmarks/export") {
        json cur = bookmarks_load();
        json bms = json::object();
        if (cur["lists"].contains(list)) bms = cur["lists"][list].value("bookmarks", json::object());
        json out{{"bookmarks", bms}};
        http_send(fd, 200, "application/json", out.dump(), "");
        return;
    }
    if (p == "/api/bookmarks/import") {
        try {
            json in = json::parse(body);
            json cur = bookmarks_load();
            int n = 0;
            if (in.contains("lists")) {
                for (auto& [ln, l] : in["lists"].items()) {
                    if (!cur["lists"].contains(ln))
                        cur["lists"][ln] = {{"showOnWaterfall", true}, {"bookmarks", json::object()}};
                    json lb = l.value("bookmarks", json::object());
                    for (auto& [k, v] : lb.items()) {
                        cur["lists"][ln]["bookmarks"][k] = v;
                        n++;
                    }
                }
            } else {
                if (!cur["lists"].contains(list))
                    cur["lists"][list] = {{"showOnWaterfall", true}, {"bookmarks", json::object()}};
                json ib = in.value("bookmarks", json::object());
                for (auto& [k, v] : ib.items()) {
                    cur["lists"][list]["bookmarks"][k] = v;
                    n++;
                }
            }
            bookmarks_save(cur);
            http_send(fd, 200, "application/json", json{{"ok", true}, {"imported", n}}.dump());
        } catch (...) { http_send(fd, 400, "application/json", "{\"ok\":false}"); }
        return;
    }
    if (p == "/api/bookmarks/scan") {
        try {
            json in = json::parse(body);
            std::string lst = in.value("list", "Scan");
            json cur = bookmarks_load();
            if (!cur["lists"].contains(lst))
                cur["lists"][lst] = {{"showOnWaterfall", true}, {"bookmarks", json::object()}};
            json bms = json::object();
            std::string mode = in.value("mode", "WFM");
            double bw = in.value("bandwidth", 180000.0);
            int n = 0;
            for (auto& st : in.value("stations", json::array())) {
                double f = st.value("freq", 0.0);
                char name[64];
                snprintf(name, sizeof(name), "%s %.3f", mode.c_str(), f / 1e6);
                bms[name] = {{"frequency", f}, {"bandwidth", bw}, {"mode", mode}};
                n++;
            }
            cur["lists"][lst]["bookmarks"] = bms;
            cur["selectedList"] = lst;
            bookmarks_save(cur);
            http_send(fd, 200, "application/json", json{{"ok", true}, {"count", n}}.dump());
        } catch (...) { http_send(fd, 400, "application/json", "{\"ok\":false}"); }
        return;
    }
    http_send(fd, 404, "text/plain", "not found");
}

static void handle_cmd(int fd, const json& m);

static void on_ws_text(int fd, const std::string& s) {
    try {
        json m = json::parse(s);
        handle_cmd(fd, m);
    } catch (...) {}
}

static void handle_cmd(int fd, const json& m) {
    std::string cmd = m.value("cmd", "");
    if (cmd == "set") {
        json kw = m;
        kw.erase("cmd");
        std::string codec = m.value("codec", "");
        if (!codec.empty()) {
            {
                std::lock_guard<std::mutex> lk(g_conn_mtx);
                auto it = g_conns.find(fd);
                if (it != g_conns.end()) it->second->codec = codec;
            }
            std::lock_guard<std::mutex> lk(g_enc_mtx);
            auto& enc = g_encs[fd];
            if (codec == "opus") {
                if (!enc) {
                    enc = std::make_shared<OpusEnc>([fd](const char* d, size_t n) {
                        std::string f;
                        f.push_back('O');
                        f.push_back(0);
                        f.push_back((char)(n & 0xff));
                        f.push_back((char)((n >> 8) & 0xff));
                        f.append(d, n);
                        g_opus_bytes.store(g_opus_bytes.load() + (double)n);
                        ws_frame(fd, 0x2, f.data(), f.size());
                    });
                    if (enc->start(g_sdr->audio_rate())) g_enc_count++;
                }
            } else {
                if (enc) { enc->close(); enc.reset(); g_enc_count--; }
            }
        }
        g_sdr->apply(kw);
        json pj = g_sdr->params_json();
        broadcast_text(json{{"type", "params"}, {"params", pj}}.dump());
    } else if (cmd == "scan") {
        {
            std::lock_guard<std::mutex> lk(g_mtx);
            g_p.paused = false;
        }
        g_dirty = true;
        g_sdr->request_scan();
        broadcast_text(json{{"type", "scan_started"}}.dump());
    } else if (cmd == "status") {
        g_sdr->push_status();
    }
}

static void ws_handshake(int fd, const std::string& req) {
    auto get = [&](const std::string& name) -> std::string {
        std::string key = name + ":";
        size_t pos = 0;
        while ((pos = req.find(key, pos)) != std::string::npos) {
            if (pos == 0 || req[pos - 1] == '\n') {
                size_t e = req.find("\r\n", pos);
                std::string v = req.substr(pos + key.size(), e - pos - key.size());
                size_t a = v.find_first_not_of(" \t");
                return a == std::string::npos ? "" : v.substr(a);
            }
            pos += key.size();
        }
        return "";
    };
    std::string key = get("Sec-WebSocket-Key");
    std::string acc = ws_accept_key(key);
    std::string resp = "HTTP/1.1 101 Switching Protocols\r\n"
                       "Upgrade: websocket\r\n"
                       "Connection: Upgrade\r\n"
                       "Sec-WebSocket-Accept: " + acc + "\r\n\r\n";
    send_all(fd, resp.data(), resp.size(), false);
    {
        std::lock_guard<std::mutex> lk(g_conn_mtx);
        auto it = g_conns.find(fd);
        if (it != g_conns.end()) {
            it->second->is_ws = true;
            it->second->last_rx = now_s();
            it->second->last_ping = now_s();
        }
    }
    g_clients++;
    g_sdr->push_status();
    ws_text(fd, json{{"type", "hello"}, {"audio_rate", 48000}}.dump());
    logf_("浏览器接入 (%d)", (int)g_clients);
}

static void handle_http(int fd, Conn& c) {
    size_t e = c.in.find("\r\n\r\n");
    if (e == std::string::npos) {
        if (c.in.size() > 1 << 20) c.closing = true;
        return;
    }
    std::string head = c.in.substr(0, e);
    std::string body = c.in.substr(e + 4);
    c.in.clear();

    size_t sp1 = head.find(' ');
    size_t sp2 = head.find(' ', sp1 + 1);
    if (sp1 == std::string::npos || sp2 == std::string::npos) { c.closing = true; return; }
    std::string method = head.substr(0, sp1);
    std::string path = head.substr(sp1 + 1, sp2 - sp1 - 1);

    size_t cl = 0;
    auto hp = head.find("Content-Length:");
    if (hp != std::string::npos) cl = (size_t)atol(head.c_str() + hp + 15);
    if (body.size() < cl) return;   // 等 body
    body = body.substr(0, cl);

    if (path == "/ws") {
        ws_handshake(fd, head);
        c.is_ws = true;
        return;
    }
    if (path.rfind("/api/", 0) == 0) {
        handle_api(fd, path, body);
        c.closing = true;
        return;
    }
    std::string file = path == "/" ? "/index.html" : path;
    if (file.find("..") != std::string::npos) { http_send(fd, 403, "text/plain", "no"); c.closing = true; return; }
    std::string full = g_static + file;
    std::ifstream f(full, std::ios::binary);
    if (!f) {
        // /res/xxx 走静态目录的上一级
        if (file.rfind("/res/", 0) == 0) {
            full = g_static + "/.." + file;
            f.open(full, std::ios::binary);
        }
    }
    if (!f) { http_send(fd, 404, "text/plain", "not found"); c.closing = true; return; }
    std::string data((std::istreambuf_iterator<char>(f)), std::istreambuf_iterator<char>());
    http_send(fd, 200, mime_of(full), data);
    c.closing = true;
}

static int make_listener(int port) {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    sockaddr_in a{};
    a.sin_family = AF_INET;
    a.sin_addr.s_addr = INADDR_ANY;
    a.sin_port = htons((uint16_t)port);
    if (bind(fd, (sockaddr*)&a, sizeof(a)) < 0) { perror("bind"); exit(1); }
    listen(fd, 64);
    fcntl(fd, F_SETFL, O_NONBLOCK);
    return fd;
}

// ------------------------------------------------------------------ main
int main(int argc, char** argv) {
    // 用 /proc/self/exe 定位自身目录，避免依赖启动时的 CWD
    char exe[PATH_MAX];
    ssize_t elen = readlink("/proc/self/exe", exe, sizeof(exe) - 1);
    std::string bin = (elen > 0) ? std::string(exe, (size_t)elen) : std::string(argv[0]);
    std::string dir = bin.substr(0, bin.find_last_of('/'));
    g_static = dir + "/../static";
    g_bookmark_file = dir + "/../bookmarks.json";
    for (int i = 1; i < argc; i++) {
        std::string a = argv[i];
        if (a == "--port" && i + 1 < argc) g_port = atoi(argv[++i]);
        else if (a == "--static" && i + 1 < argc) g_static = argv[++i];
        else if (a == "--bookmarks" && i + 1 < argc) g_bookmark_file = argv[++i];
    }
    signal(SIGPIPE, SIG_IGN);


    Sdr sdr;
    g_sdr = &sdr;
    std::thread sdr_thread([&] { sdr.run(); });

    int lfd = make_listener(g_port);
    int ep = epoll_create1(0);
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = lfd;
    epoll_ctl(ep, EPOLL_CTL_ADD, lfd, &ev);

    logf_("看板启动: http://0.0.0.0:%d/", g_port);

    epoll_event events[64];
    double last_beat = now_s();
    while (g_run) {
        int n = epoll_wait(ep, events, 64, 200);
        if (now_s() - last_beat > 5.0) {
            last_beat = now_s();
            std::vector<int> dead;
            std::vector<int> ping;
            {
                std::lock_guard<std::mutex> lk(g_conn_mtx);
                for (auto& kv : g_conns) {
                    if (!kv.second->is_ws) continue;
                    if (now_s() - kv.second->last_rx > 60.0) dead.push_back(kv.first);
                    else if (now_s() - kv.second->last_ping > 20.0) ping.push_back(kv.first);
                }
            }
            for (int fd2 : ping) {
                ws_frame(fd2, 0x9, nullptr, 0);
                std::lock_guard<std::mutex> lk(g_conn_mtx);
                auto it = g_conns.find(fd2);
                if (it != g_conns.end()) it->second->last_ping = now_s();
            }
            for (int fd2 : dead) {
                logf_("心跳超时，断开一个客户端");
                {
                    std::lock_guard<std::mutex> lk(g_enc_mtx);
                    auto it = g_encs.find(fd2);
                    if (it != g_encs.end()) { it->second->close(); g_encs.erase(it); g_enc_count--; }
                }
                epoll_ctl(ep, EPOLL_CTL_DEL, fd2, nullptr);
                close(fd2);
                std::lock_guard<std::mutex> lk(g_conn_mtx);
                g_conns.erase(fd2);
            }
            if (!dead.empty()) {
                g_clients -= (int)dead.size();
                logf_("浏览器断开 (%d)", (int)g_clients);
                if (g_clients <= 0) {
                    std::lock_guard<std::mutex> lk(g_mtx);
                    g_p.paused = true;
                }
            }
        }
        for (int i = 0; i < n; i++) {
            int fd = events[i].data.fd;
            if (fd == lfd) {
                while (true) {
                    int cfd = accept(lfd, nullptr, nullptr);
                    if (cfd < 0) break;
                    fcntl(cfd, F_SETFL, O_NONBLOCK);
                    int one = 1;
                    setsockopt(cfd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
                    epoll_event ce{};
                    ce.events = EPOLLIN;
                    ce.data.fd = cfd;
                    epoll_ctl(ep, EPOLL_CTL_ADD, cfd, &ce);
                    auto c = std::make_shared<Conn>();
                    c->fd = cfd;
                    {
                        std::lock_guard<std::mutex> lk(g_conn_mtx);
                        g_conns[cfd] = c;
                    }
                }
            } else {
                std::shared_ptr<Conn> c;
                {
                    std::lock_guard<std::mutex> lk(g_conn_mtx);
                    auto it = g_conns.find(fd);
                    if (it != g_conns.end()) c = it->second;
                }
                if (!c) continue;
                char buf[65536];
                ssize_t r = read(fd, buf, sizeof(buf));
                if (r <= 0) {
                    bool was_ws = c->is_ws;
                    {
                        std::lock_guard<std::mutex> lk(g_enc_mtx);
                        auto it = g_encs.find(fd);
                        if (it != g_encs.end()) { it->second->close(); g_encs.erase(it); }
                    }
                    epoll_ctl(ep, EPOLL_CTL_DEL, fd, nullptr);
                    close(fd);
                    {
                        std::lock_guard<std::mutex> lk(g_conn_mtx);
                        g_conns.erase(fd);
                    }
                    if (was_ws) {
                        g_clients--;
                        logf_("浏览器断开 (%d)", (int)g_clients);
                        if (g_clients <= 0) {
                            std::lock_guard<std::mutex> lk(g_mtx);
                            g_p.paused = true;
                        }
                    }
                    continue;
                }
                if (c->is_ws) {
                    c->last_rx = now_s();
                    c->ws.buf.append(buf, (size_t)r);
                    std::string payload;
                    int op;
                    while (c->ws.next(payload, op)) {
                        if (op == 0x1) on_ws_text(fd, payload);
                        else if (op == 0x8) { c->closing = true; break; }
                        else if (op == 0x9) ws_frame(fd, 0xA, payload.data(), payload.size());
                    }
                    if (c->closing) {
                        epoll_ctl(ep, EPOLL_CTL_DEL, fd, nullptr);
                        close(fd);
                        std::lock_guard<std::mutex> lk(g_conn_mtx);
                        g_conns.erase(fd);
                        g_clients--;
                    }
                } else {
                    c->in.append(buf, (size_t)r);
                    handle_http(fd, *c);
                    if (c->closing) {
                        epoll_ctl(ep, EPOLL_CTL_DEL, fd, nullptr);
                        close(fd);
                        std::lock_guard<std::mutex> lk(g_conn_mtx);
                        g_conns.erase(fd);
                    }
                }
            }
        }
    }
    sdr_thread.join();
    return 0;
}
