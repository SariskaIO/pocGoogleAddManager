#!/usr/bin/env python3
"""
Redesigned test_shared_clock.py
Uses decoupled pipelines: Main Pipeline (appsrc) + Ad Pipeline (appsink)
Ensures HLS/SRT live stream never reaches EOS while ads loop at intervals.
"""

import sys
import time

# macOS: Initialize Cocoa BEFORE importing GStreamer
if sys.platform == 'darwin':
    try:
        import AppKit
        AppKit.NSApplication.sharedApplication()
    except ImportError:
        print("Note: PyObjC not installed")

import gi
gi.require_version('Gst', '1.0')
gi.require_version('Gtk', '3.0')
from gi.repository import Gst, Gtk, GLib

Gst.init(None)

def log(msg):
    print(f"[{time.strftime('%H:%M:%S')}] {msg}")

class DecoupledAdCompositor:
    """
    Main HLS player with a decoupled appsrc for ads.
    """
    def __init__(self, main_url, ad_url, ad_x=920, ad_y=500, ad_width=320, ad_height=180, interval=5):
        self.main_url = main_url
        self.ad_url = ad_url
        self.ad_x = ad_x
        self.ad_y = ad_y
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

    def _on_main_pad_added(self, element, pad):
        caps = pad.get_current_caps()
        if not caps: return
        name = caps.get_structure(0).get_name()
        
        if name.startswith("video"):
            sink = self.main_pipeline.get_by_name("main_video_queue").get_static_pad("sink")
            if not sink.is_linked():
                pad.link(sink)
                log("[MAIN] Video linked")
        elif name.startswith("audio"):
            sink = self.main_pipeline.get_by_name("audio_queue").get_static_pad("sink")
            if not sink.is_linked():
                pad.link(sink)
                log("[MAIN] Audio linked")

    def _create_main_pipeline(self):
        pipeline_str = f"""
            compositor name=comp ! videoconvert ! videoscale ! video/x-raw,width=1280,height=720 ! autovideosink
            
            uridecodebin uri="{self.main_url}" name=main_src
            
            main_src. ! queue name=main_video_queue max-size-buffers=10 ! videoconvert ! videoscale ! comp.sink_0
            main_src. ! queue name=audio_queue max-size-buffers=10 ! audioconvert ! audioresample ! autoaudiosink
            
            appsrc name=ad_appsrc is-live=true do-timestamp=true format=time !
            video/x-raw,format=I420,width={self.ad_width},height={self.ad_height} !
            queue name=ad_queue max-size-buffers=10 leaky=downstream !
            videoconvert ! videoscale ! 
            capsfilter name=ad_link_src caps="video/x-raw,width={self.ad_width},height={self.ad_height}"
        """
        self.main_pipeline = Gst.parse_launch(pipeline_str)
        self.compositor = self.main_pipeline.get_by_name("comp")
        self.appsrc = self.main_pipeline.get_by_name("ad_appsrc")
        
        # Explicitly set caps on appsrc to avoid negotiation failure
        caps = Gst.Caps.from_string(f"video/x-raw,format=I420,width={self.ad_width},height={self.ad_height},framerate=30/1")
        self.appsrc.set_property("caps", caps)
        
        # Connect pad-added for uridecodebin
        main_src = self.main_pipeline.get_by_name("main_src")
        main_src.connect("pad-added", self._on_main_pad_added)

        bus = self.main_pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_main_message)

    def _on_main_message(self, bus, msg):
        if msg.type == Gst.MessageType.EOS:
            log("[MAIN] EOS reached - quitting")
            Gtk.main_quit()
        elif msg.type == Gst.MessageType.ERROR:
            err, debug = msg.parse_error()
            # On macOS, glimagesink sometimes throws "Quit requested" (3) as a resource error
            # during dynamic pipeline changes. We treat it as a warning to avoid crashing.
            if "Quit requested" in str(err):
                log(f"[MAIN WARNING] Transient sink error ignored: {err}")
                return True
            log(f"[MAIN ERROR] {err}: {debug}")
            Gtk.main_quit()
        return True

    def _create_ad_pipeline(self):
        """Create separate pipeline for ad playback using playbin for robustness"""
        # Create appsink first
        appsink = Gst.ElementFactory.make("appsink", "ad_sink")
        appsink.set_property("emit-signals", True)
        appsink.set_property("sync", True)
        appsink.set_property("max-buffers", 5)
        appsink.set_property("drop", True)
        appsink.connect("new-sample", self._on_new_ad_sample)

        # Build sink bin for playbin
        sink_bin = Gst.Bin.new("ad_sink_bin")
        queue = Gst.ElementFactory.make("queue", "ad_queue")
        convert = Gst.ElementFactory.make("videoconvert", "ad_convert")
        scale = Gst.ElementFactory.make("videoscale", "ad_scale")
        caps = Gst.ElementFactory.make("capsfilter", "ad_caps")
        caps.set_property("caps", Gst.Caps.from_string(
            f"video/x-raw,width={self.ad_width},height={self.ad_height},format=I420"
        ))

        for elem in [queue, convert, scale, caps, appsink]:
            sink_bin.add(elem)
        
        queue.link(convert)
        convert.link(scale)
        scale.link(caps)
        caps.link(appsink)

        ghost = Gst.GhostPad.new("sink", queue.get_static_pad("sink"))
        sink_bin.add_pad(ghost)

        # Create playbin
        self.ad_pipeline = Gst.ElementFactory.make("playbin", "ad_playbin")
        self.ad_pipeline.set_property("uri", self.ad_url)
        self.ad_pipeline.set_property("video-sink", sink_bin)
        self.ad_pipeline.set_property("volume", 0.0) # Silent ad

        bus = self.ad_pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect("message", self._on_ad_message)

    def _on_new_ad_sample(self, appsink):
        if not self.ad_running or not self.appsrc:
            return Gst.FlowReturn.OK
        
        sample = appsink.emit("pull-sample")
        if sample:
            buf = sample.get_buffer()
            # Clear timestamps to let appsrc (do-timestamp=true) handle it
            buf.pts = Gst.CLOCK_TIME_NONE
            buf.dts = Gst.CLOCK_TIME_NONE
            buf.duration = Gst.CLOCK_TIME_NONE
            
            # Push buffer to main pipeline appsrc
            ret = self.appsrc.emit("push-buffer", buf)
            if ret != Gst.FlowReturn.OK:
                log(f"[AD] Push rejected: {ret}")
        return Gst.FlowReturn.OK

    def _on_ad_message(self, bus, msg):
        if msg.type == Gst.MessageType.EOS:
            log(f"[AD] Finished. Waiting {self.interval}s...")
            self._stop_ad_pipeline()
            self._schedule_restart()
        elif msg.type == Gst.MessageType.ERROR:
            err, debug = msg.parse_error()
            self.retry_count += 1
            log(f"[AD ERROR] (Try #{self.retry_count}) {err}")
            self._stop_ad_pipeline()
            self._schedule_restart()
        return True

    def _schedule_restart(self):
        """Schedule a restart if one isn't already pending"""
        if self.restart_timer_id is None:
            self.restart_timer_id = GLib.timeout_add_seconds(self.interval, self._restart_ad)

    def _stop_ad_pipeline(self):
        self.ad_running = False
        
        # 1. Unlink and release compositor pad (Removes window)
        if self.comp_pad:
            ad_src_elem = self.main_pipeline.get_by_name("ad_link_src")
            ad_src_pad = ad_src_elem.get_static_pad("src")
            log(f"[COMPOSITOR] Releasing ad pad (Unlinking window)")
            ad_src_pad.unlink(self.comp_pad)
            self.compositor.release_request_pad(self.comp_pad)
            self.comp_pad = None

        # 2. Stop ad pipeline
        if self.ad_pipeline:
            self.ad_pipeline.set_state(Gst.State.NULL)
            self.ad_pipeline = None

    def _restart_ad(self):
        self.restart_timer_id = None
        log("[AD] Restarting fresh (Adding window)...")
        
        # 1. Request new pad from compositor
        self.comp_pad = self.compositor.request_pad_simple("sink_%u")
        self.comp_pad.set_property("xpos", self.ad_x)
        self.comp_pad.set_property("ypos", self.ad_y)
        self.comp_pad.set_property("width", self.ad_width)
        self.comp_pad.set_property("height", self.ad_height)
        self.comp_pad.set_property("zorder", 100)
        
        # 2. Link appsrc chain to compositor
        ad_src_elem = self.main_pipeline.get_by_name("ad_link_src")
        ad_src_pad = ad_src_elem.get_static_pad("src")
        ad_src_pad.link(self.comp_pad)
        
        # 3. Create and start ad source pipeline
        self._create_ad_pipeline()
        self.ad_running = True
        self.ad_pipeline.set_state(Gst.State.PLAYING)
        return False

    def run(self):
        Gtk.init(None)
        log("Starting Decoupled Compositor...")
        self._create_main_pipeline()

        # Build but don't start ad pipeline yet (let run() do it)
        # self._create_ad_pipeline() 

        self.main_pipeline.set_state(Gst.State.PLAYING)
        
        # Trigger first ad play immediately manually
        self._restart_ad()

        try:
            Gtk.main()
        except KeyboardInterrupt:
            pass
        finally:
            self.ad_running = False
            self.main_pipeline.set_state(Gst.State.NULL)
            self.ad_pipeline.set_state(Gst.State.NULL)

if __name__ == "__main__":
    # Hardcoded for testing as requested
    main_hls = "https://indiatodaylive.akamaized.net/hls/live/2014320/indiatoday/indiatodaylive/live_720p/chunks.m3u8"
    ad_mp4 = "https://www.w3schools.com/html/mov_bbb.mp4"
    
    player = DecoupledAdCompositor(main_url=main_hls, ad_url=ad_mp4)
    player.run()
