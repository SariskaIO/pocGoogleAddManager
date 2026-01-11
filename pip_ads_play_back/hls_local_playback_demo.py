#!/usr/bin/env python3
"""
hls_local_playback_demo.py
- Decoupled Main + Ad pipelines.
- Dynamic Ad Rotation (Video Pool).
- Dynamic Ad Positioning (Corner Rotation).
- Local Playback (Auto Video/Audio Sink).
"""

import sys
import time
import os
import urllib.request
import xml.etree.ElementTree as ET

# macOS: Initialize Cocoa BEFORE importing GStreamer
if sys.platform == 'darwin':
    try:
        import AppKit
        AppKit.NSApplication.sharedApplication()
    except ImportError:
        pass

import gi
gi.require_version('Gst', '1.0')
gi.require_version('Gtk', '3.0')
from gi.repository import Gst, Gtk, GLib

try:
    from gam_api_helper import GAMAPIHelper
    HAS_GAM_API = True
except ImportError:
    HAS_GAM_API = False

Gst.init(None)

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

class LocalAdCompositor:
    def __init__(self, main_url, ad_urls=None, vast_url=None, ad_positions=None, ad_width=320, ad_height=180, interval=20):
        self.main_url = main_url
        self.vast_url = vast_url
        self.ad_urls = ad_urls if isinstance(ad_urls, list) else ([ad_urls] if ad_urls else [])
        self.ad_index = 0
        
        # Position pool for rotation
        self.ad_positions = ad_positions if ad_positions else [(920, 500)]
        self.pos_index = 0
        
        self.ad_url = None
        self.ad_width = ad_width
        self.ad_height = ad_height
        self.interval = interval

        self.main_pipeline = None
        self.ad_pipeline = None
        self.compositor = None
        self.appsrc = None
        self.comp_pad = None
        self.ad_running = False
        self.restart_timer_id = None
        self.retry_count = 0
        self.impression_urls = []
        self.manual_position = False

    def update_ad_position(self, x, y):
        """Update the ad position dynamically if the ad is running."""
        if not self.comp_pad:
            log("[AD] Position update ignored (no ad running)")
            return
        log(f"[AD] Manually moving to ({x}, {y})")
        self.comp_pad.set_property("xpos", x)
        self.comp_pad.set_property("ypos", y)
        self.manual_position = True

    def _on_stdin_input(self, channel, condition):
        line = sys.stdin.readline().strip()
        if not line:
            return True
        
        parts = line.split()
        if not parts:
            return True

        if parts[0] == "pos" and len(parts) >= 2:
            if parts[1] == "auto":
                log("[AD] Resuming automatic rotation")
                self.manual_position = False
            elif len(parts) == 3:
                try:
                    x = int(parts[1])
                    y = int(parts[2])
                    self.update_ad_position(x, y)
                except ValueError:
                    log(f"[INPUT ERROR] Invalid coordinates: {parts[1]}, {parts[2]}")
            else:
                log("[INPUT ERROR] Usage: 'pos <x> <y>' or 'pos auto'")
        return True

    def _on_main_pad_added(self, element, pad):
        caps = pad.get_current_caps()
        if not caps: return
        name = caps.get_structure(0).get_name()
        
        if name.startswith("video"):
            sink = self.main_pipeline.get_by_name("main_video_queue").get_static_pad("sink")
            if not sink.is_linked(): pad.link(sink)
        elif name.startswith("audio"):
            sink = self.main_pipeline.get_by_name("main_audio_queue").get_static_pad("sink")
            if not sink.is_linked(): pad.link(sink)

    def _create_main_pipeline(self):
        pipeline_str = f"""
            compositor name=comp ! videoconvert ! videoscale ! 
            video/x-raw,width=1280,height=720 ! autovideosink
            
            uridecodebin uri="{self.main_url}" name=main_src
            
            main_src. ! queue name=main_video_queue max-size-buffers=10 ! videoconvert ! videoscale ! comp.sink_0
            
            main_src. ! queue name=main_audio_queue max-size-buffers=10 ! audioconvert ! audioresample ! autoaudiosink
            
            appsrc name=ad_appsrc is-live=true do-timestamp=true format=time !
            video/x-raw,format=I420,width={self.ad_width},height={self.ad_height} !
            queue name=ad_queue max-size-buffers=10 leaky=downstream !
            videoconvert ! videoscale ! 
            capsfilter name=ad_link_src caps="video/x-raw,width={self.ad_width},height={self.ad_height}"
        """
        log("Creating Local Playback Pipeline...")
        self.main_pipeline = Gst.parse_launch(pipeline_str)
        self.compositor = self.main_pipeline.get_by_name("comp")
        self.appsrc = self.main_pipeline.get_by_name("ad_appsrc")
        
        caps = Gst.Caps.from_string(f"video/x-raw,format=I420,width={self.ad_width},height={self.ad_height},framerate=30/1")
        self.appsrc.set_property("caps", caps)
        
        main_src = self.main_pipeline.get_by_name("main_src")
        main_src.connect("pad-added", self._on_main_pad_added)

        bus = self.main_pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_main_message)

    def _on_main_message(self, bus, msg):
        if msg.type == Gst.MessageType.EOS:
            log("[MAIN] EOS reached")
            Gtk.main_quit()
        elif msg.type == Gst.MessageType.ERROR:
            err, debug = msg.parse_error()
            if "Quit requested" in str(err): return True
            log(f"[MAIN ERROR] {err}: {debug}")
            Gtk.main_quit()
        elif msg.type == Gst.MessageType.WARNING:
            warn, debug = msg.parse_warning()
            log(f"[MAIN WARNING] {warn}")
        elif msg.type == Gst.MessageType.INFO:
            info, debug = msg.parse_info()
            log(f"[MAIN INFO] {info}")
        elif msg.type == Gst.MessageType.STATE_CHANGED:
            if msg.src == self.main_pipeline:
                old, new, pending = msg.parse_state_changed()
                log(f"[MAIN STATE] {old.value_nick} -> {new.value_nick}")
        return True

    def _fetch_vast_media_url(self):
        log(f"[VAST] Fetching: {self.vast_url[:60]}...")
        self.impression_urls = []
        try:
            with urllib.request.urlopen(self.vast_url) as response:
                xml_data = response.read()
            tree = ET.fromstring(xml_data)
            for imp in tree.findall(".//Impression"):
                if imp.text: self.impression_urls.append(imp.text.strip())
            
            best_url, best_bitrate = None, 0
            for mf in tree.findall(".//MediaFile"):
                if mf.get("type") == "video/mp4":
                    bitrate = int(mf.get("bitrate", 0))
                    if bitrate > best_bitrate and bitrate < 5000:
                        best_bitrate, best_url = bitrate, mf.text.strip()
            return best_url
        except Exception as e:
            log(f"[VAST ERROR] {e}")
        return None

    def _create_ad_pipeline(self):
        if not self.ad_url: return
        appsink = Gst.ElementFactory.make("appsink", "ad_sink")
        appsink.set_property("emit-signals", True)
        appsink.set_property("sync", True)
        appsink.connect("new-sample", self._on_new_ad_sample)

        sink_bin = Gst.Bin.new("ad_sink_bin")
        q = Gst.ElementFactory.make("queue")
        conv = Gst.ElementFactory.make("videoconvert")
        scale = Gst.ElementFactory.make("videoscale")
        caps = Gst.ElementFactory.make("capsfilter")
        caps.set_property("caps", Gst.Caps.from_string(f"video/x-raw,width={self.ad_width},height={self.ad_height},format=I420"))
        
        for e in [q, conv, scale, caps, appsink]: sink_bin.add(e)
        q.link(conv); conv.link(scale); scale.link(caps); caps.link(appsink)
        sink_bin.add_pad(Gst.GhostPad.new("sink", q.get_static_pad("sink")))

        self.ad_pipeline = Gst.ElementFactory.make("playbin", "ad_playbin")
        self.ad_pipeline.set_property("uri", self.ad_url)
        self.ad_pipeline.set_property("video-sink", sink_bin)
        self.ad_pipeline.set_property("volume", 0.0)

        bus = self.ad_pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_ad_message)

    def _on_new_ad_sample(self, appsink):
        if not self.ad_running or not self.appsrc: return Gst.FlowReturn.OK
        sample = appsink.emit("pull-sample")
        if sample:
            buf = sample.get_buffer()
            buf.pts = buf.dts = buf.duration = Gst.CLOCK_TIME_NONE
            self.appsrc.emit("push-buffer", buf)
        return Gst.FlowReturn.OK

    def _on_ad_message(self, bus, msg):
        if msg.type == Gst.MessageType.EOS:
            log(f"[AD] Finished. Waiting {self.interval}s...")
            self._stop_ad_pipeline()
            self._schedule_restart()
        elif msg.type == Gst.MessageType.ERROR:
            err, _ = msg.parse_error()
            log(f"[AD ERROR] {err}")
            self._stop_ad_pipeline()
            self._schedule_restart()
        return True

    def _schedule_restart(self):
        if self.restart_timer_id is None:
            self.restart_timer_id = GLib.timeout_add_seconds(self.interval, self._restart_ad)

    def _stop_ad_pipeline(self):
        self.ad_running = False
        if self.comp_pad:
            ad_src_elem = self.main_pipeline.get_by_name("ad_link_src")
            ad_src_elem.get_static_pad("src").unlink(self.comp_pad)
            self.compositor.release_request_pad(self.comp_pad)
            self.comp_pad = None
        if self.ad_pipeline:
            self.ad_pipeline.set_state(Gst.State.NULL)
            self.ad_pipeline = None

    def _restart_ad(self):
        self.restart_timer_id = None
        if self.vast_url:
            self.ad_url = self._fetch_vast_media_url()
        elif self.ad_urls:
            self.ad_url = self.ad_urls[self.ad_index]
            self.ad_index = (self.ad_index + 1) % len(self.ad_urls)
        
        if not self.ad_url:
            self._schedule_restart()
            return False

        self.comp_pad = self.compositor.request_pad_simple("sink_%u")
        
        if self.manual_position:
            # If manual position was set, use it instead of the pool (reuse current values)
            x = self.comp_pad.get_property("xpos")
            y = self.comp_pad.get_property("ypos")
            log(f"[AD] Playing at manual position ({x}, {y}) - {self.ad_url[:40]}...")
        else:
            x, y = self.ad_positions[self.pos_index]
            log(f"[AD] Playing at ({x}, {y}) - {self.ad_url[:40]}...")
            self.pos_index = (self.pos_index + 1) % len(self.ad_positions)

        self.comp_pad.set_property("xpos", x)
        self.comp_pad.set_property("ypos", y)
        self.comp_pad.set_property("width", self.ad_width)
        self.comp_pad.set_property("height", self.ad_height)
        self.comp_pad.set_property("zorder", 100)
        
        self.main_pipeline.get_by_name("ad_link_src").get_static_pad("src").link(self.comp_pad)
        self._create_ad_pipeline()
        self.ad_running = True
        self.ad_pipeline.set_state(Gst.State.PLAYING)
        return False

    def run(self):
        Gtk.init(None)
        log("Starting Local HLS Playback with Ad Sync...")
        log("Commands: 'pos <x> <y>' to move ad, 'pos auto' to resume rotation")
        
        # Monitor stdin for user input
        GLib.io_add_watch(sys.stdin, GLib.IO_IN, self._on_stdin_input)
        
        self._create_main_pipeline()
        self.main_pipeline.set_state(Gst.State.PLAYING)
        self._restart_ad()
        try:
            Gtk.main()
        except KeyboardInterrupt:
            pass
        finally:
            if self.main_pipeline: self.main_pipeline.set_state(Gst.State.NULL)
            if self.ad_pipeline: self.ad_pipeline.set_state(Gst.State.NULL)

if __name__ == "__main__":
    # India Today Live HLS URL
    MAIN_HLS_URL = "https://indiatodaylive.akamaized.net/hls/live/2014320/indiatoday/indiatodaylive/live_720p/chunks.m3u8"
    # Shared sample ad video
    AD_MP4 = "https://www.w3schools.com/html/mov_bbb.mp4"
    
    positions = [(920, 500), (40, 500), (40, 40), (920, 40)]
    
    player = LocalAdCompositor(main_url=MAIN_HLS_URL, ad_urls=[AD_MP4], ad_positions=positions)
    player.run()
