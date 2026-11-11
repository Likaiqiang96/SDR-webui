#!/usr/bin/env python3
"""
SDR 网页看板（RSP1 + SoapySDR）

- 频谱 / 瀑布（WebSocket 推 float32 dB 帧）
- 12-bit / 8-bit 收听（8-bit 用软件量化，等效原 rtl_tcp 8-bit 流）
- WFM / NFM / AM 解调，48k 左右 PCM 推给浏览器播放
- 状态：采样率、IQ 码率、音频码率、信噪比、增益、ADC 位数、客户端数
- 自动搜台（扫 87.5–108 MHz，找峰值）
- 懒加载：没有浏览器连接时释放 SDR，手机 SDR++ 仍可继续用 1235 端口
"""
import argparse
import asyncio
import json
import math
import subprocess
import threading
import time

import numpy as np
import aiohttp
from aiohttp import web
from scipy import signal

import SoapySDR
from SoapySDR import SOAPY_SDR_RX, SOAPY_SDR_CS16

import os

BOOKMARK_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'bookmarks.json')
RES_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'res')

READ_N = 16384          # 每次读的复数样本数
FFT_N = 8192            # FFT 点数
SPEC_BINS = 1024        # 发给前端的频谱点数
SPEC_FPS = 15.0
AUDIO_CHUNK = 1024      # 每次推给浏览器的音频样本数（~20ms）



def load_bookmarks():
    try:
        with open(BOOKMARK_FILE, 'r', encoding='utf-8') as f:
            d = json.load(f)
        if 'lists' in d:
            return d
    except Exception:
        pass
    return {'selectedList': 'General', 'bookmarkDisplayMode': 1,
            'lists': {'General': {'showOnWaterfall': True, 'bookmarks': {}}}}


def save_bookmarks(d):
    tmp = BOOKMARK_FILE + '.tmp'
    with open(tmp, 'w', encoding='utf-8') as f:
        json.dump(d, f, indent=4, ensure_ascii=False)
    os.replace(tmp, BOOKMARK_FILE)


def log(*a):
    print(time.strftime('%H:%M:%S'), *a, flush=True)



class OpusEncoder:
    """把 PCM 送进 ffmpeg，输出 WebM/Opus 分片（浏览器 MSE 直接播）"""

    def __init__(self, rate, push, bitrate=40000):
        self.push = push
        self.closed = False
        try:
            self.p = subprocess.Popen(
                ['ffmpeg', '-hide_banner', '-loglevel', 'error',
                 '-f', 's16le', '-ar', str(int(rate)), '-ac', '1', '-i', 'pipe:0',
                 '-c:a', 'libopus', '-b:a', str(bitrate), '-vbr', 'on',
                 '-application', 'audio', '-frame_duration', '10',
                 '-cluster_time_limit', '60', '-cluster_size_limit', '0',
                 '-f', 'webm', 'pipe:1'],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, bufsize=0)
        except Exception as e:
            log('ffmpeg 启动失败:', e)
            self.p = None
            return
        threading.Thread(target=self._reader, daemon=True).start()

    def _reader(self):
        out = self.p.stdout
        while True:
            data = out.read(4096)
            if not data:
                break
            if self.closed:
                break          # 旧编码器的残留数据，丢掉
            try:
                self.push(data)
            except Exception:
                pass

    def write(self, pcm):
        if not self.p or self.closed:
            return
        try:
            self.p.stdin.write(pcm)
        except Exception:
            pass

    def close(self):
        if self.closed:
            return
        self.closed = True
        try:
            self.p.stdin.close()
        except Exception:
            pass
        try:
            self.p.terminate()
        except Exception:
            pass


class Hub:
    """线程安全的广播队列 -> WebSocket 客户端"""

    def __init__(self, loop):
        self.loop = loop
        self.clients = {}          # ws -> {'codec': 'pcm'|'opus'}
        self.opus = {}             # ws -> OpusEncoder（每个客户端独立一路）
        self.opus_bytes = 0
        self.opus_lock = threading.Lock()
        self.q = asyncio.Queue(maxsize=32)

    def push_bin(self, data):
        self._put(('bin', data))

    def push_json(self, obj):
        self._put(('txt', json.dumps(obj, separators=(',', ':'))))

    def _put(self, item):
        def _do():
            if self.q.full():
                try:
                    self.q.get_nowait()
                except Exception:
                    pass
            self.q.put_nowait(item)
        try:
            self.loop.call_soon_threadsafe(_do)
        except RuntimeError:
            pass

    def send_bin_to(self, ws, data):
        try:
            asyncio.run_coroutine_threadsafe(ws.send_bytes(data), self.loop)
        except Exception:
            pass

    def make_opus(self, ws, rate):
        """给某个客户端建一路独立 Opus 编码器"""
        self.opus_rate = float(rate)
        def push(chunk):
            for i in range(0, len(chunk), 4096):
                part = chunk[i:i + 4096]
                self.opus_bytes += len(part)
                self.send_bin_to(ws, b'O' + b'\x00' + len(part).to_bytes(2, 'little') + part)
        enc = OpusEncoder(rate, push)
        with self.opus_lock:
            old = self.opus.get(ws)
            self.opus[ws] = enc
        if old:
            old.close()
        return enc

    def recreate_opus(self, rate):
        for ws, enc in list(self.opus.items()):
            enc.close()
            self.make_opus(ws, rate)

    def drop_opus(self, ws):
        with self.opus_lock:
            enc = self.opus.pop(ws, None)
        if enc:
            enc.close()

    def opus_encoders(self):
        with self.opus_lock:
            return list(self.opus.items())

    def drop_audio(self):
        def _do():
            items = []
            while not self.q.empty():
                try:
                    it = self.q.get_nowait()
                except Exception:
                    break
                if it[0] == 'bin' and it[1][:1] == b'O':
                    continue
                items.append(it)
            for it in items:
                if not self.q.full():
                    self.q.put_nowait(it)
        try:
            self.loop.call_soon_threadsafe(_do)
        except RuntimeError:
            pass

    async def run(self):
        while True:
            kind, payload = await self.q.get()
            dead = []
            for ws, meta in list(self.clients.items()):
                try:
                    if kind == 'bin':
                        if payload[:1] == b'A' and meta.get('codec') == 'opus':
                            continue          # Opus 客户端不要 PCM
                        await ws.send_bytes(payload)
                    else:
                        await ws.send_str(payload)
                except Exception:
                    dead.append(ws)
            for ws in dead:
                self.clients.pop(ws, None)


class SDR(threading.Thread):
    def __init__(self, hub):
        super().__init__(daemon=True)
        self.hub = hub
        self.lock = threading.Lock()
        self.params = {
            'freq': 100_000_000.0,
            'rate': 2_000_000.0,
            'gain': 21.0,          # 0..42 dB，42 = 最大增益
            'bits': 12,            # 12 / 8
            'mode': 'WFM',         # WFM/NFM/AM/USB/LSB/CW/DSB/RAW
            'bw': 180000.0,        # 解调带宽 Hz
            'squelch': -100.0,     # dB，-100 = 关闭
            'offset': 0.0,         # VFO 相对 LO 的偏移 Hz
            'paused': True,
            'codec': 'pcm',         # pcm 默认（兼容旧页面）；新页面收听时切 opus
        }
        self.active = False
        self.dev = None
        self.stream = None
        self.buf = np.empty(2 * READ_N, dtype=np.int16)
        self.err = ''
        self.adc_bits = 0
        self.adc_name = ''
        self._dirty = True
        self.scan_req = False
        self.scan_out = None
        self.scanning = False
        self.running = True

        # 统计
        self.iq_bytes = 0
        self.audio_bytes = 0
        self.snr = 0.0
        self.peak_db = -120.0
        self.audio_rate = 50000
        self.clients = 0
        self.started_at = 0
        self.spectrum = np.full(SPEC_BINS, -120.0, dtype=np.float32)

        # DSP 状态
        self._spec_acc = []
        self._spec_last = 0.0
        self._audio_acc = np.zeros(0, dtype=np.float32)
        self._last_sample = np.complex64(0)
        self._dec2 = 5
        self._fir = None
        self._fir_state = None
        self._deemph_state = None
        self._dc_state = 0.0
        self._status_last = 0.0
        self._rate_mark = time.time()
        self._rate_iq = 0
        self._rate_aud = 0
        self.iq_rate = 0.0
        self.audio_kbps = 0.0
        self.down_kbps = 0.0
        self._down_bytes = 0
        self._rate_down = 0
        self.spec_bytes = 0
        self.pcm_bytes = 0
        self._rate_spec = 0
        self._rate_pcm = 0
        self._rate_opus = 0
        self.opus_bytes = 0

    # ---------- 设备 ----------
    @staticmethod
    def _pick_device():
        """优先 SDRplay，其次 Airspy/RTL 等"""
        try:
            devs = [dict(k) for k in SoapySDR.Device.enumerate()]
        except Exception:
            devs = []
        rank = {'sdrplay': 0, 'airspy': 1, 'rtlsdr': 2, 'hackrf': 3, 'lime': 4, 'bladerf': 5}
        devs.sort(key=lambda d: rank.get(d.get('driver', ''), 9))
        return devs[0] if devs else {'driver': 'sdrplay'}

    def _detect_adc(self, dev):
        """按驱动/型号判断 ADC 位数：MSi2500(SDRplay) = 12bit，RTL2832U = 8bit"""
        try:
            drv = dev.getDriverKey()
            hw = dev.getHardwareKey()
        except Exception:
            drv, hw = '', ''
        if drv in ('rtlsdr', 'hackrf', 'osmosdr'):
            self.adc_bits = 8
        elif drv in ('sdrplay', 'airspy', 'lime', 'bladerf', 'uhd'):
            self.adc_bits = 12
        else:
            self.adc_bits = 12 if drv else 0
        self.adc_name = ('%s/%s' % (drv, hw)) if drv else ''

    def _ensure_dev(self):
        if self.dev is not None:
            return True
        p = self.params
        try:
            dev = SoapySDR.Device(self._pick_device())
            dev.setSampleRate(SOAPY_SDR_RX, 0, p['rate'])
            dev.setFrequency(SOAPY_SDR_RX, 0, p['freq'])
            dev.setGainMode(SOAPY_SDR_RX, 0, False)
            dev.setGain(SOAPY_SDR_RX, 0, self._soapy_gain(p['gain']))
            stream = dev.setupStream(SOAPY_SDR_RX, SOAPY_SDR_CS16)
            dev.activateStream(stream)
            self.dev, self.stream = dev, stream
            self.started_at = time.time()
            self._dirty = False
            self.err = ''
            self._reset_dsp(p['rate'])
            self._detect_adc(dev)
            log('SDR 已打开: %.3f MHz, %.2f MSps (%d-bit ADC)' %
                (p['freq'] / 1e6, p['rate'] / 1e6, self.adc_bits))
            return True
        except Exception as e:
            self.err = str(e)
            self.dev = None
            self.stream = None
            log('SDR 打开失败:', e)
            return False

    def _close_dev(self):
        if self.dev is None:
            return
        try:
            self.dev.deactivateStream(self.stream)
            self.dev.closeStream(self.stream)
        except Exception:
            pass
        try:
            self.dev = None
            self.stream = None
        finally:
            log('SDR 已释放')

    @staticmethod
    def _soapy_gain(ui_gain_db):
        # SoapySDRPlay 的 0..42 是“增益衰减”：0 = 最大增益，42 = 最小
        # 对外用 dB 表示（0..42，42 = 最大增益）
        return float(np.clip(42.0 - ui_gain_db, 0.0, 42.0))

    _COARSE = {D: {k: np.exp(-2j * np.pi * np.arange(D) * k / D).astype(np.complex64)
                   for k in range(D)} for D in (1, 2, 3, 4, 6, 8)}

    def _reset_dsp(self, rate):
        self._spec_acc = []
        self._audio_acc = np.zeros(0, dtype=np.float32)
        self._last_sample = np.complex64(0)
        # 两级抽取：先抽到 ~1 MHz 再解调（WFM 带宽也够），再 FIR 抽到 ~48k
        d1 = max(1, int(round(rate / 1e6)))
        self._dem1 = d1
        self._dec2 = max(1, int(round((rate / d1) / 48000.0)))
        self.audio_rate = rate / d1 / self._dec2
        self._fir = signal.firwin(65, 0.8 / self._dec2).astype(np.float32)
        self._fir_state = np.zeros(len(self._fir) - 1, dtype=np.float32)
        tau = 50e-6
        dt = 1.0 / self.audio_rate
        a = dt / (tau + dt)
        self._deemph_b = np.array([a], dtype=np.float32)
        self._deemph_a = np.array([1.0, -(1.0 - a)], dtype=np.float32)
        self._deemph_state = np.zeros(1, dtype=np.float32)
        self._dc_state = 0.0
        self._mix_phase = 0.0
        self._mix_phase8 = 0.0
        self._coarse_lut = self._COARSE
        self._last8 = np.complex64(0)
        self._audio_fir = None
        self._audio_fir_state = None
        self._audio_bw = 0.0
        self._squelch_hold = 0.0
        self._dc_coef = 1.0 - 2 * math.pi * 20.0 / self.audio_rate

    # ---------- 设置 ----------
    def apply(self, **kw):
        with self.lock:
            changed_rate = False
            for k, v in kw.items():
                if k not in self.params:
                    continue
                if k == 'rate' and abs(float(v) - self.params['rate']) > 1:
                    changed_rate = True
                self.params[k] = v if k in ('mode', 'bits', 'codec') else float(v)
            self._dirty = True
        if changed_rate and self.dev is not None:
            self._close_dev()
        return self.params.copy()

    # ---------- 主循环 ----------
    def run(self):
        while self.running:
            if not self.active or self.params.get('paused'):
                self._close_dev()
                time.sleep(0.15)
                continue
            if not self._ensure_dev():
                time.sleep(0.5)
                continue
            try:
                self._apply_dirty()
                if self.scan_req:
                    self.scan_req = False
                    self._do_scan()
                    continue
                self._read_and_process()
            except Exception as e:
                log('处理异常:', e)
                self.err = str(e)
                self._close_dev()
                time.sleep(0.3)

    def _apply_dirty(self):
        with self.lock:
            if not self._dirty:
                return
            p = self.params.copy()
            self._dirty = False
        try:
            self.dev.setFrequency(SOAPY_SDR_RX, 0, p['freq'])
            self.dev.setGain(SOAPY_SDR_RX, 0, self._soapy_gain(p['gain']))
        except Exception as e:
            log('应用参数失败:', e)

    def _read_and_process(self):
        p = self.params
        r = self.dev.readStream(self.stream, [self.buf], READ_N, timeoutUs=500000)
        if r.ret <= 0:
            return
        n = r.ret
        raw = self.buf[:2 * n]
        self.iq_bytes += n * 4
        x = raw.astype(np.float32) / 32768.0
        z = x[0::2] + 1j * x[1::2]

        z = z.astype(np.complex64)

        now = time.time()
        # ---- 频谱 ----
        self._spec_acc.append(z.copy())
        total = sum(len(a) for a in self._spec_acc)
        while total > FFT_N * 4 and len(self._spec_acc) > 1:
            total -= len(self._spec_acc[0])
            self._spec_acc.pop(0)
        if now - self._spec_last >= 1.0 / SPEC_FPS:
            self._spec_last = now
            if not self.scanning:
                self._push_spectrum()

        # ---- 音频 ----
        self._demod(z, p['mode'])

        # ---- 速率统计 / 状态 ----
        if now - self._rate_mark >= 1.0:
            dt = now - self._rate_mark
            self.iq_rate = (self.iq_bytes - self._rate_iq) / dt / 1e6   # MB/s
            if self.hub.opus_encoders():
                self.audio_kbps = (self.hub.opus_bytes - self._rate_opus) * 8 / dt / 1000.0
            else:
                self.audio_kbps = (self.audio_bytes - self._rate_aud) * 8 / dt / 1000.0
            spec_rate = (self.spec_bytes - self._rate_spec) / dt / 1024.0
            pcm_rate = (self.pcm_bytes - self._rate_pcm) / dt / 1024.0
            opus_rate = (self.hub.opus_bytes - self._rate_opus) / dt / 1024.0
            pcm_clients = sum(1 for m in self.hub.clients.values() if m.get('codec') != 'opus')
            self.down_kbps = spec_rate + opus_rate + (pcm_rate if pcm_clients else 0.0)
            self._rate_spec = self.spec_bytes
            self._rate_pcm = self.pcm_bytes
            self._rate_mark = now
            self._rate_iq = self.iq_bytes
            self._rate_aud = self.audio_bytes
            self._rate_opus = self.hub.opus_bytes
            self._rate_down = self._down_bytes
        if now - self._status_last >= 0.5:
            self._status_last = now
            self._push_status()

    def _push_spectrum(self):
        buf = np.concatenate(self._spec_acc)
        if len(buf) < FFT_N:
            return
        seg = buf[-FFT_N:]
        w = np.hanning(FFT_N).astype(np.float32)
        sp = np.fft.fftshift(np.fft.fft(seg * w))
        p = (sp.real ** 2 + sp.imag ** 2).astype(np.float32)
        db = 10.0 * np.log10(p + 1e-12)
        # 合并到 SPEC_BINS 点
        k = FFT_N // SPEC_BINS
        db = db.reshape(SPEC_BINS, k).max(axis=1).astype(np.float32)
        # dBFS 归一化：FFT 增益
        db -= 20 * math.log10(FFT_N / 4.0)
        self.spectrum = db
        mid = db[SPEC_BINS // 8: SPEC_BINS * 7 // 8]
        if len(mid):
            pk = float(mid.max())
            med = float(np.median(mid))
            snr = max(0.0, pk - med)
            # 平滑，避免读数乱跳
            self.peak_db = 0.8 * self.peak_db + 0.2 * pk if self.peak_db > -119 else pk
            self.snr = 0.8 * self.snr + 0.2 * snr
        q = np.clip(np.round(db + 140.0), 0, 255).astype(np.uint8)
        frame = b'S' + b'\x00' + np.uint16(SPEC_BINS).tobytes() + q.tobytes()
        self.spec_bytes += len(frame)
        self.hub.push_bin(frame)

    def _demod(self, z, mode):
        """先在 fs/8（~1 MHz）上解调，再抽取到音频率：运算量降 8 倍，8 MHz 采样率也扛得住"""
        p = self.params
        bw = float(p.get('bw', 180000.0))
        fs = float(p['rate'])
        if len(z) < 16:
            return

        # 1) VFO 偏移：粗混频(k*fs/8，8 点周期，极便宜) + 抽取后再细混频，避免 16k 点 exp
        off = float(p.get('offset', 0.0))
        D = self._dem1
        fs8 = fs / D
        n8 = len(z) // D * D
        if n8 < D:
            return
        if abs(off) > 1.0:
            k = int(round(off / fs8))
            f_res = off - k * fs8
            kk = k % D
            if kk:
                coarse = self._coarse_lut[D][kk]
                z = (z[:n8] * np.resize(coarse, n8)).astype(np.complex64)
            else:
                z = z[:n8]
        else:
            f_res = 0.0

        # 2) 抽取到 ~1 MHz
        z8 = z[:n8].reshape(-1, D).mean(axis=1).astype(np.complex64)
        if abs(f_res) > 1.0:
            nn = np.arange(len(z8), dtype=np.float64)
            ph = 2.0 * np.pi * f_res * (nn + self._mix_phase8) / fs8
            self._mix_phase8 += len(z8)
            z8 = (z8 * np.exp(-1j * ph)).astype(np.complex64)

        # 3) 解调
        zc = np.concatenate(([self._last8], z8))
        self._last8 = z8[-1]
        if mode in ('WFM', 'NFM'):
            d = zc[1:] * np.conj(zc[:-1])
            a = d.imag / (np.abs(d) + 1e-9)
            a = a.astype(np.float32)
            if mode == 'NFM':
                a = a * 0.7
        elif mode in ('AM', 'DSB'):
            a = np.abs(z8).astype(np.float32)
            a = a - a.mean()
        elif mode in ('USB', 'LSB', 'CW'):
            f = bw / 2.0 if mode == 'USB' else (-bw / 2.0 if mode == 'LSB' else 700.0)
            n = np.arange(len(z8), dtype=np.float64)
            ph = 2.0 * np.pi * f * (n + self._mix_phase8) / fs8
            self._mix_phase8 += len(z8)
            a = (z8 * np.exp(-1j * ph)).real.astype(np.float32)
        elif mode == 'RAW':
            a = z8.real.astype(np.float32)
        else:
            a = z8.real.astype(np.float32)

        # 4) 二级抽取 -> ~48k
        if self._dec2 > 1:
            y, self._fir_state = signal.lfilter(self._fir, [1.0], a, zi=self._fir_state)
            d2 = y[::self._dec2].astype(np.float32)
        else:
            d2 = a

        # 5) 音频低通（按带宽）
        cutoff = min(15000.0 if mode == 'WFM' else bw / 2.0, self.audio_rate * 0.45)
        if cutoff < self.audio_rate * 0.45:
            if (self._audio_fir is None or abs(self._audio_bw - cutoff) > 1.0
                    or len(self._audio_fir_state) != 128):
                self._audio_fir = signal.firwin(129, max(200.0, cutoff) / (self.audio_rate / 2)).astype(np.float32)
                self._audio_fir_state = np.zeros(len(self._audio_fir) - 1, dtype=np.float32)
                self._audio_bw = cutoff
            d2, self._audio_fir_state = signal.lfilter(self._audio_fir, [1.0], d2, zi=self._audio_fir_state)

        # 6) 去加重
        if mode in ('WFM', 'NFM'):
            d2, self._deemph_state = signal.lfilter(self._deemph_b, self._deemph_a, d2,
                                                    zi=self._deemph_state)

        # 7) DC 阻断
        out = np.empty_like(d2)
        st = self._dc_state
        c = self._dc_coef
        for i in range(len(d2)):
            st = c * st + (1 - c) * d2[i]
            out[i] = d2[i] - st
        self._dc_state = st

        # 8) 静噪
        sq = float(self.params.get('squelch', -100.0))
        if sq > -99:
            lvl = 20.0 * np.log10(float(np.sqrt((out ** 2).mean())) + 1e-9)
            if lvl >= sq:
                self._squelch_hold = time.time()
            if time.time() - self._squelch_hold > 0.25:
                out = np.zeros_like(out)

        out = np.clip(out * 6.0, -1.0, 1.0).astype(np.float32)
        self._audio_acc = np.concatenate((self._audio_acc, out))
        while len(self._audio_acc) >= AUDIO_CHUNK:
            chunk = self._audio_acc[:AUDIO_CHUNK]
            self._audio_acc = self._audio_acc[AUDIO_CHUNK:]
            pcm = (chunk * 32767.0).astype(np.int16)
            self.audio_bytes += pcm.nbytes
            frame = b'A' + b'\x00' + np.uint16(len(pcm)).tobytes() + pcm.tobytes()
            self.pcm_bytes += len(frame)
            self.hub.push_bin(frame)
            raw = pcm.tobytes()
            for ws, enc in self.hub.opus_encoders():
                enc.write(raw)

    def _push_status(self):
        p = self.params
        self.hub.push_json({
            'type': 'status',
            'running': self.dev is not None,
            'error': self.err,
            'freq': p['freq'],
            'rate': p['rate'],
            'gain': p['gain'],
            'bits': getattr(self, 'adc_bits', 0),
            'adc_bits': getattr(self, 'adc_bits', 0),
            'adc_name': getattr(self, 'adc_name', ''),
            'mode': p['mode'],
            'audio_rate': round(self.audio_rate, 1),
            'iq_mbps': round(self.iq_rate, 2),
            'audio_kbps': round(self.audio_kbps, 1),
            'down_kbps': round(self.down_kbps, 1),
            'codec': 'opus' if self.hub.opus_encoders() else 'pcm',
            'bw': p.get('bw', 180000.0),
            'squelch': p.get('squelch', -100.0),
            'offset': p.get('offset', 0.0),
            'paused': bool(p.get('paused', False)),
            'snr': round(self.snr, 1),
            'peak_db': round(self.peak_db, 1),
            'clients': self.clients,
            'uptime': round(time.time() - self.started_at, 0) if self.started_at else 0,
        })

    # ---------- 自动搜台 ----------
    def _do_scan(self, f0=87.5e6, f1=108.0e6):
        """按当前采样率扫频：多帧平均 + 局部噪声底 + 要求有谱宽（排除噪声尖峰）"""
        self.scanning = True
        old = self.params['freq']
        rate = float(self.params['rate'])
        span = rate * 0.8
        step = span * 0.9
        nfft = 4096
        bin_hz = rate / nfft
        freqs = (np.arange(nfft) - nfft / 2) * bin_hz
        sel = np.abs(freqs) < span / 2   # 基础掩码，每步再叠加频率范围限制
        w = np.hanning(nfft).astype(np.float32)
        log('开始扫描 %.1f-%.1f MHz (步进 %.2f MHz)' % (f0 / 1e6, f1 / 1e6, step / 1e6))
        res = []
        f = f0
        try:
            while f <= f1:
                self.dev.setFrequency(SOAPY_SDR_RX, 0, f)
                time.sleep(0.12)
                for _ in range(4):        # 丢掉换频前残留的样本
                    self.dev.readStream(self.stream, [self.buf], READ_N, timeoutUs=100000)
                acc = []
                for _ in range(6):
                    r = self.dev.readStream(self.stream, [self.buf], READ_N, timeoutUs=200000)
                    if r.ret > 0:
                        acc.append(self.buf[:2 * r.ret].copy())
                if not acc:
                    f += step
                    continue
                x = np.concatenate(acc).astype(np.float32) / 32768.0
                z = x[0::2] + 1j * x[1::2]
                nfr = min(4, len(z) // nfft)
                if nfr < 1:
                    f += step
                    continue
                p = None
                for k in range(nfr):
                    seg = z[len(z) - nfft * (k + 1): len(z) - nfft * k]
                    sp = np.fft.fftshift(np.fft.fft(seg * w))
                    pw = sp.real ** 2 + sp.imag ** 2
                    p = pw if p is None else p + pw
                p = p / nfr
                db = 10 * np.log10(p + 1e-12) - 20 * math.log10(nfft / 4.0)
                sel2 = sel & ((f + freqs) >= f0) & ((f + freqs) <= f1)
                dbv = db[sel2]
                if len(dbv) < 64:
                    f += step
                    continue
                fv2 = freqs[sel2]
                floor = float(np.percentile(dbv, 20))
                thr = floor + 6.0
                # 平滑，避免多径/纹波把一条台切成很多碎片
                k = max(3, int(round(6000.0 / bin_hz)))
                if k % 2 == 0:
                    k += 1
                sm = np.convolve(dbv, np.ones(k) / k, mode='same')
                above = np.flatnonzero(sm > thr)
                if len(above):
                    breaks = np.flatnonzero(np.diff(above) > 1)
                    groups = list(np.split(above, breaks + 1))
                    # 间距 < 25 kHz 的碎片先合并
                    gap = max(2, int(round(25000.0 / bin_hz)))
                    merged_g = []
                    for g in groups:
                        if merged_g and g[0] - merged_g[-1][-1] <= gap:
                            merged_g[-1] = np.concatenate((merged_g[-1], g))
                        else:
                            merged_g.append(g)
                    for g in merged_g:
                        width_hz = float(len(g) * bin_hz)
                        if width_hz < 55e3:          # 太窄：噪声尖峰/单音
                            continue
                        pi = int(g[int(np.argmax(dbv[g]))])
                        pk = float(dbv[pi])
                        if pk < -85.0 or pk - floor < 14.0:
                            continue
                        mean_lvl = float(dbv[g].mean())
                        if mean_lvl - floor < 5.0:   # 平均也要明显高于底噪
                            continue
                        # 用峰值所在 bin（而不是质心）定位，避免被不对称边带拉偏
                        top = g[np.argsort(dbv[g])[-3:]]
                        wts = 10 ** (np.clip(dbv[top] - pk, -6, 0) / 10.0)
                        center = float((fv2[top] * wts).sum() / wts.sum())
                        res.append((f + center, pk, floor, width_hz))
                f += step
        except Exception as e:
            log('扫描失败:', e)
        try:
            self.dev.setFrequency(SOAPY_SDR_RX, 0, old)
        except Exception:
            pass

        # 合并同一条目被相邻步进重复发现的情况
        res.sort(key=lambda r: r[0])
        merged = []
        for fr, pk, floor, width in res:
            if merged and abs(fr - merged[-1][0]) < 120e3:
                if pk > merged[-1][1]:
                    merged[-1] = (fr, pk, floor, width)
            else:
                merged.append((fr, pk, floor, width))

        # FM 按 0.1 MHz 频道网格对齐（中国 FM 台都在 100 kHz 栅格上）
        grid = 100e3
        on_grid = {}
        for fr, pk, floor, width in merged:
            q = round(fr / grid) * grid
            if q in on_grid:
                if pk > on_grid[q][1]:
                    on_grid[q] = (q, pk, floor, width)
            else:
                on_grid[q] = (q, pk, floor, width)
        merged = sorted(on_grid.values(), key=lambda r: r[0])
        stations = [{'freq': round(fr, 1), 'db': round(pk, 1),
                     'snr': round(pk - floor, 1), 'width': int(width / 1e3)}
                    for fr, pk, floor, width in merged
                    if f0 <= fr <= f1]
        self.scan_out = stations
        self.scanning = False
        log('扫描完成，%d 个台' % len(stations))
        self.hub.push_json({'type': 'scan', 'stations': stations})


class App:
    def __init__(self, static_dir):
        self.static = static_dir
        self.loop = None
        self.hub = None
        self.sdr = None
        self.ws_clients = 0

    async def index(self, request):
        return web.FileResponse(self.static + '/index.html')

    async def ws_handler(self, request):
        ws = web.WebSocketResponse(heartbeat=20, max_msg_size=0)
        await ws.prepare(request)
        self.hub.clients[ws] = {'codec': 'pcm'}
        self.ws_clients += 1
        self.sdr.clients = self.ws_clients
        self.sdr.active = True
        ua = request.headers.get('User-Agent', '?')[:80]
        log('浏览器接入 (%d) UA=%s' % (self.ws_clients, ua))
        # 先发一份状态 + 参数
        await ws.send_str(json.dumps({'type': 'hello', 'params': self.sdr.params,
                                      'audio_rate': self.sdr.audio_rate}))
        try:
            async for msg in ws:
                if msg.type == aiohttp.WSMsgType.TEXT:
                    try:
                        m = json.loads(msg.data)
                    except Exception:
                        continue
                    await self.handle_cmd(m, ws)
                elif msg.type == aiohttp.WSMsgType.ERROR:
                    break
        finally:
            self.hub.clients.pop(ws, None)
            self.hub.drop_opus(ws)
            self.ws_clients = max(0, self.ws_clients - 1)
            self.sdr.clients = self.ws_clients
            if self.ws_clients == 0:
                self.sdr.active = False   # 没人看 -> 释放 SDR
                self.sdr.params['paused'] = True   # 下次打开页面要重新点 ▶
            log('浏览器断开 (%d)' % self.ws_clients)
        return ws

    async def handle_cmd(self, m, ws=None):
        cmd = m.get('cmd')
        if cmd == 'set':
            kw = {}
            for k in ('freq', 'rate', 'gain', 'bits', 'mode', 'bw', 'squelch', 'offset', 'paused'):
                if k in m:
                    kw[k] = m[k]
            p = self.sdr.apply(**kw)
            if 'codec' in m and ws is not None and ws in self.hub.clients:
                c = m['codec']
                self.hub.clients[ws]['codec'] = c
                if c == 'opus':
                    self.hub.make_opus(ws, self.sdr.audio_rate)
                else:
                    self.hub.drop_opus(ws)
                log('客户端切换 codec -> %s' % c)
            await self.hub_push({'type': 'params', 'params': p})
        elif cmd == 'scan':
            if self.sdr.params.get('paused'):
                self.sdr.apply(paused=False)
                log('扫描前自动开始采集')
            self.sdr.scan_req = True
            await self.hub_push({'type': 'scan_started'})
        elif cmd == 'status':
            self.sdr._push_status()


    async def api_bookmarks(self, request):
        if request.method == 'GET':
            return web.json_response(load_bookmarks())
        body = await request.json()
        if 'lists' in body:
            save_bookmarks(body)
        else:
            cur = load_bookmarks()
            lst = body.get('list') or cur.get('selectedList', 'General')
            cur['lists'].setdefault(lst, {'showOnWaterfall': True, 'bookmarks': {}})
            cur['lists'][lst]['bookmarks'].update(body.get('bookmarks', {}))
            save_bookmarks(cur)
        return web.json_response({'ok': True})

    async def api_export(self, request):
        cur = load_bookmarks()
        lst = request.query.get('list') or cur.get('selectedList', 'General')
        bms = cur.get('lists', {}).get(lst, {}).get('bookmarks', {})
        resp = web.json_response({'bookmarks': bms})
        resp.headers['Content-Disposition'] = 'attachment; filename="%s.json"' % lst.replace(' ', '_')
        return resp

    async def api_import(self, request):
        body = await request.json()
        cur = load_bookmarks()
        n = 0
        if 'lists' in body:
            for ln, l in body['lists'].items():
                cur['lists'].setdefault(ln, {'showOnWaterfall': True, 'bookmarks': {}})
                bms = l.get('bookmarks', {})
                cur['lists'][ln]['bookmarks'].update(bms)
                n += len(bms)
        else:
            lst = request.query.get('list') or cur.get('selectedList', 'General')
            cur['lists'].setdefault(lst, {'showOnWaterfall': True, 'bookmarks': {}})
            bms = body.get('bookmarks', {})
            cur['lists'][lst]['bookmarks'].update(bms)
            n += len(bms)
        save_bookmarks(cur)
        return web.json_response({'ok': True, 'imported': n})

    async def api_scan_save(self, request):
        body = await request.json()
        lst = body.get('list', 'Scan')
        cur = load_bookmarks()
        cur['lists'].setdefault(lst, {'showOnWaterfall': True, 'bookmarks': {}})
        mode = body.get('mode', 'WFM')
        bw = body.get('bandwidth', 180000.0)
        bms = {}
        for st in body.get('stations', []):
            f = float(st['freq'])
            name = '%s %.3f' % (mode, f / 1e6)
            bms[name] = {'frequency': f, 'bandwidth': bw, 'mode': mode}
        cur['lists'][lst]['bookmarks'] = bms      # 扫描结果整体替换，避免重复累积
        cur['selectedList'] = lst
        save_bookmarks(cur)
        return web.json_response({'ok': True, 'count': len(bms)})


    async def api_res(self, request):
        out = {'bandplans': [], 'colormaps': []}
        bp = os.path.join(RES_DIR, 'bandplans')
        cm = os.path.join(RES_DIR, 'colormaps')
        try:
            for f in sorted(os.listdir(bp)):
                if not f.endswith('.json'):
                    continue
                try:
                    d = json.load(open(os.path.join(bp, f), encoding='utf-8'))
                    out['bandplans'].append({'file': f, 'name': d.get('name', f),
                                             'country': d.get('country_name', '')})
                except Exception:
                    pass
        except Exception:
            pass
        try:
            for f in sorted(os.listdir(cm)):
                if not f.endswith('.json'):
                    continue
                try:
                    d = json.load(open(os.path.join(cm, f), encoding='utf-8'))
                    out['colormaps'].append({'file': f, 'name': d.get('name', f),
                                             'map': d.get('map', [])})
                except Exception:
                    pass
        except Exception:
            pass
        return web.json_response(out)

    async def idle_status(self):
        """未采集时也要推状态，前端才知道该显示“▶ 未采集”"""
        while True:
            await asyncio.sleep(0.5)
            try:
                if self.sdr is not None and self.sdr.dev is None and self.ws_clients > 0:
                    self.sdr._push_status()
            except Exception:
                pass

    async def watch_audio_rate(self):
        """设备重开/换采样率后，audio_rate 会变；等它稳定再重建 Opus 编码器"""
        while True:
            await asyncio.sleep(0.5)
            try:
                r = float(self.sdr.audio_rate)
            except Exception:
                continue
            if r < 1000:
                continue
            cur = getattr(self.hub, 'opus_rate', None)
            if cur is None:
                self.hub.opus_rate = r
                continue
            if abs(cur - r) > 1.0:
                self.hub.opus_rate = r
                if self.hub.opus_encoders():
                    log('音频采样率变化 %.0f -> %.0f Hz，重建 Opus 编码器' % (cur, r))
                    self.hub.recreate_opus(r)
                    await self.hub_push({'type': 'audio_reset'})

    async def hub_push(self, obj):
        await self.hub.q.put(('txt', json.dumps(obj)))


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--port', type=int, default=8095)
    ap.add_argument('--static', default=__file__.rsplit('/', 1)[0] + '/static')
    args = ap.parse_args()

    app = App(args.static)

    async def on_startup(a):
        app.loop = asyncio.get_running_loop()
        app.hub = Hub(app.loop)
        asyncio.create_task(app.hub.run())
        app.sdr = SDR(app.hub)
        app.sdr.start()
        app.rate_watcher = asyncio.create_task(app.watch_audio_rate())
        app.idle_status_task = asyncio.create_task(app.idle_status())
        log('看板启动: http://0.0.0.0:%d/' % args.port)

    aio = web.Application()
    aio.on_startup.append(on_startup)
    aio.router.add_get('/', app.index)
    aio.router.add_get('/ws', app.ws_handler)
    aio.router.add_get('/api/res', app.api_res)
    aio.router.add_static('/res', RES_DIR)
    aio.router.add_get('/api/bookmarks', app.api_bookmarks)
    aio.router.add_post('/api/bookmarks', app.api_bookmarks)
    aio.router.add_get('/api/bookmarks/export', app.api_export)
    aio.router.add_post('/api/bookmarks/import', app.api_import)
    aio.router.add_post('/api/bookmarks/scan', app.api_scan_save)
    aio.router.add_static('/static', args.static)
    web.run_app(aio, host='0.0.0.0', port=args.port, print=None)


if __name__ == '__main__':
    main()
